"""
LLM scheduling agent integration for email reply generation.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
from typing import Any

import httpx
from openai import OpenAI

from app.logging_config import log_event

NODE_BACKEND_URL = os.getenv("NODE_BACKEND_URL", "http://localhost:8080")
FINALIZED_MARKER = "##FINALIZED##"

SYSTEM_PROMPT = (
    "You are an email scheduling assistant.\n"
    "Security constraints:\n"
    "- Only discuss scheduling and meeting coordination.\n"
    "- Refuse unrelated questions.\n"
    "- Never reveal full calendar details or contact lists.\n"
    "- Never mention being an AI.\n"
    "- Keep responses concise and professional.\n"
    "- Before proposing times, call get_calendar_events.\n"
    "- When a meeting is finalized, append exactly one line:\n"
    "##FINALIZED##{\"summary\":\"...\",\"startTime\":\"...\",\"endTime\":\"...\","
    "\"description\":\"...\",\"attendees\":[\"...\"]}"
)

GET_EVENTS_TOOL = {
    "type": "function",
    "function": {
        "name": "get_calendar_events",
        "description": "Get calendar events to check availability before proposing time.",
        "parameters": {
            "type": "object",
            "properties": {
                "timeMin": {"type": "string"},
                "timeMax": {"type": "string"},
                "maxResults": {"type": "integer"},
            },
            "required": ["timeMin", "timeMax"],
        },
    },
}


def _openai_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY is not set")
    return OpenAI(api_key=api_key)


def _request_node_with_retries(
    method: str,
    path: str,
    access_token: str,
    provider: str = "google",
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    refresh_access_token: Callable[[], str | None] | None = None,
    operation: str,
    max_attempts: int = 4,
) -> dict[str, Any]:
    provider = (provider or "google").lower()
    token = access_token
    backoff_seconds = 1.0
    refreshed_once = False

    for attempt in range(1, max_attempts + 1):
        headers = {"g-axs-tk": token} if provider == "google" else {"o-axs-tk": token}
        if json_body is not None:
            headers["Content-Type"] = "application/json"

        try:
            resp = httpx.request(
                method=method,
                url=f"{NODE_BACKEND_URL}{path}",
                headers=headers,
                params=params,
                json=json_body,
                timeout=20.0,
            )
        except Exception as exc:
            if attempt == max_attempts:
                return {"ok": False, "error": f"{operation}_request_exception:{exc}"}
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 8.0)
            continue

        if resp.status_code == 200:
            return {"ok": True, "response": resp, "token": token}

        if resp.status_code == 401 and refresh_access_token and not refreshed_once:
            refreshed_once = True
            new_token = (refresh_access_token() or "").strip()
            if new_token:
                token = new_token
                log_event(f"{operation}_token_refreshed_after_401", attempt=attempt)
                continue
            return {"ok": False, "error": f"{operation}_401_and_refresh_failed"}

        if resp.status_code == 429 and attempt < max_attempts:
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 8.0)
            continue

        if resp.status_code >= 500 and attempt < max_attempts:
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 8.0)
            continue

        return {
            "ok": False,
            "error": f"{operation}_http_{resp.status_code}",
            "response": resp.text[:500],
        }

    return {"ok": False, "error": f"{operation}_exhausted_retries"}


def _get_calendar_events(
    access_token: str,
    time_min: str,
    time_max: str,
    provider: str = "google",
    max_results: int = 20,
    refresh_access_token: Callable[[], str | None] | None = None,
) -> list[dict]:
    params = {
        "timeMin": time_min,
        "timeMax": time_max,
        "maxResults": max_results,
        "type": provider,
        "includeTasks": "false",
    }
    result = _request_node_with_retries(
        method="GET",
        path="/api/calendar/events",
        access_token=access_token,
        provider=provider,
        params=params,
        refresh_access_token=refresh_access_token,
        operation="agent_calendar_fetch",
    )
    if not result.get("ok"):
        log_event(
            "agent_calendar_fetch_failed",
            error=result.get("error"),
            response=result.get("response"),
        )
        return []

    payload = result["response"].json()
    events = payload.get("events", []) if isinstance(payload, dict) else []
    log_event("agent_calendar_fetch_success", event_count=len(events))
    slim = []
    for e in events:
        start_obj = e.get("start") or {}
        end_obj = e.get("end") or {}
        slim.append(
            {
                "summary": e.get("summary", "Busy"),
                "start": start_obj.get("dateTime") or start_obj.get("date"),
                "end": end_obj.get("dateTime") or end_obj.get("date"),
            }
        )
    return slim


def create_calendar_event(
    access_token: str,
    event_data: dict[str, Any],
    provider: str = "google",
    refresh_access_token: Callable[[], str | None] | None = None,
) -> dict[str, Any]:
    body = {
        "summary": event_data.get("summary", "Meeting"),
        "startTime": event_data.get("startTime"),
        "endTime": event_data.get("endTime"),
        "description": event_data.get("description", ""),
        "attendees": event_data.get("attendees", []),
        "type": provider,
    }
    result = _request_node_with_retries(
        method="POST",
        path="/api/calendar/events",
        access_token=access_token,
        provider=provider,
        params={"type": provider},
        json_body=body,
        refresh_access_token=refresh_access_token,
        operation="agent_create_event",
    )
    if not result.get("ok"):
        log_event(
            "agent_create_event_failed",
            error=result.get("error"),
            response=result.get("response"),
        )
        return {"ok": False, "error": result.get("error"), "response": result.get("response")}
    created_payload = result["response"].json()
    log_event("agent_create_event_success")
    return {"ok": True, "data": created_payload}


def _extract_finalized(content: str) -> tuple[str, dict[str, Any] | None]:
    if FINALIZED_MARKER not in content:
        return content.strip(), None
    head, tail = content.split(FINALIZED_MARKER, 1)
    body = head.strip()
    try:
        finalized = json.loads(tail.strip())
        return body, finalized
    except Exception:
        return content.strip(), None


def generate_scheduling_reply(
    thread_messages: list[str],
    access_token: str,
    provider: str = "google",
    sender_name: str = "Scheduler Team",
    context: str = "Schedule a meeting professionally.",
    refresh_access_token: Callable[[], str | None] | None = None,
) -> tuple[str, dict[str, Any] | None]:
    client = _openai_client()
    user_prompt = (
        f"Sender name: {sender_name}\n"
        f"Context: {context}\n\n"
        "Email thread (oldest to newest):\n"
        + "\n---\n".join(thread_messages)
    )

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]

    for _ in range(6):
        response = client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-5-mini"),
            messages=messages,
            tools=[GET_EVENTS_TOOL],
            tool_choice="auto",
        )
        msg = response.choices[0].message

        if not msg.tool_calls:
            content = (msg.content or "").strip()
            return _extract_finalized(content)

        messages.append(msg.model_dump(exclude_none=True))

        for tc in msg.tool_calls:
            if tc.function.name != "get_calendar_events":
                continue
            args = json.loads(tc.function.arguments or "{}")
            events = _get_calendar_events(
                access_token=access_token,
                provider=provider,
                time_min=args.get("timeMin", ""),
                time_max=args.get("timeMax", ""),
                max_results=int(args.get("maxResults", 20)),
                refresh_access_token=refresh_access_token,
            )
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": json.dumps(events),
                }
            )

    return "Could you share 2-3 time windows that work for you this week?", None
