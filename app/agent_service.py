"""
LLM scheduling agent integration for email reply generation.
"""

from __future__ import annotations

import json
import os
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


def _get_calendar_events(google_access_token: str, time_min: str, time_max: str, max_results: int = 20) -> list[dict]:
    headers = {"g-axs-tk": google_access_token}
    params = {
        "timeMin": time_min,
        "timeMax": time_max,
        "maxResults": max_results,
        "type": "google",
    }
    url = f"{NODE_BACKEND_URL}/api/calendar/events"
    try:
        resp = httpx.get(url, headers=headers, params=params, timeout=20.0)
        if resp.status_code != 200:
            log_event(
                "agent_calendar_fetch_failed",
                status_code=resp.status_code,
                response=resp.text[:500],
            )
            return []
        payload = resp.json()
        events = payload.get("events", []) if isinstance(payload, dict) else []
        slim = []
        for e in events:
            slim.append(
                {
                    "summary": e.get("summary", "Busy"),
                    "start": (e.get("start") or {}).get("dateTime"),
                    "end": (e.get("end") or {}).get("dateTime"),
                }
            )
        return slim
    except Exception as exc:
        log_event("agent_calendar_fetch_exception", error=str(exc))
        return []


def create_calendar_event(google_access_token: str, event_data: dict[str, Any]) -> dict[str, Any]:
    headers = {"g-axs-tk": google_access_token, "Content-Type": "application/json"}
    url = f"{NODE_BACKEND_URL}/api/calendar/events"
    body = {
        "summary": event_data.get("summary", "Meeting"),
        "startTime": event_data.get("startTime"),
        "endTime": event_data.get("endTime"),
        "description": event_data.get("description", ""),
        "attendees": event_data.get("attendees", []),
        "type": "google",
    }
    try:
        resp = httpx.post(url, headers=headers, json=body, timeout=20.0)
        if resp.status_code != 200:
            return {"ok": False, "error": f"create_event_http_{resp.status_code}", "response": resp.text[:500]}
        return {"ok": True, "data": resp.json()}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


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
    google_access_token: str,
    sender_name: str = "Scheduler Team",
    context: str = "Schedule a meeting professionally.",
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
                google_access_token=google_access_token,
                time_min=args.get("timeMin", ""),
                time_max=args.get("timeMax", ""),
                max_results=int(args.get("maxResults", 20)),
            )
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": json.dumps(events),
                }
            )

    return "Could you share 2-3 time windows that work for you this week?", None
