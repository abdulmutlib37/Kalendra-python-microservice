"""
LLM scheduling agent integration for email reply generation.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from openai import OpenAI

from app.logging_config import log_event

NODE_BACKEND_URL = os.getenv("NODE_BACKEND_URL", "http://localhost:8080")
FINALIZED_MARKER = "##FINALIZED##"

SYSTEM_PROMPT = (
    "You are Kalendra, an AI scheduling assistant acting on behalf of {sender_name}.\n"
    "Your goal is to coordinate and finalize a meeting time with the recipient via email.\n\n"
    "Security constraints:\n"
    "- Only discuss scheduling and meeting coordination.\n"
    "- Refuse unrelated questions and redirect back to scheduling.\n"
    "- Never reveal full calendar details or contact lists.\n"
    "- Never mention being an AI.\n"
    "- Keep responses concise and professional.\n"
    "- Before proposing times, call get_calendar_events.\n\n"
    "Guidelines:\n"
    "- Be warm, professional, and natural — write like a thoughtful human assistant\n"
    "- Keep emails concise but not abrupt; friendly but not overly casual\n"
    "- ALWAYS call get_calendar_events before proposing or confirming any time slot\n"
    "- Use calendar data to propose times with no conflicts; avoid back-to-back meetings where possible\n"
    "- Propose 2-3 specific available slots when suggesting times\n"
    "- If the other party proposes a time, verify it against the calendar before confirming\n"
    "- Never mention you are an AI or that you are checking a calendar\n"
    "- For ongoing replies, do not always start with 'Hi <name>'; only greet when naturally needed\n"
    "- Do not include subject lines, headers, or sign-offs — just the email body\n\n"
    "- If the recipient confirms/accepts a proposed slot (e.g. 'okay fine', 'sounds good', 'book it'), "
    "and a concrete slot is already present in the thread context, finalize immediately instead of asking extra back-and-forth questions\n"
    "- If asked for sensitive details (specific attendees, reasons, full calendar history), share only high-level availability and redirect to scheduling\n\n"
    "When a meeting time has been FULLY agreed upon by both parties, end your reply with "
    'exactly this JSON block on its own line (nothing after it):\n'
    '##FINALIZED##{{"summary": "...", "startTime": "...", "endTime": "...", '
    '"description": "...", "attendees": ["..."]}}'
)

GET_EVENTS_TOOL = {
    "type": "function",
    "function": {
        "name": "get_calendar_events",
        "description": "Fetch the user's calendar events for a given time range to check availability and avoid conflicts.",
        "parameters": {
            "type": "object",
            "properties": {
                "timeMin": {
                    "type": "string",
                    "description": "ISO 8601 datetime lower bound e.g. 2026-02-17T00:00:00Z",
                },
                "timeMax": {
                    "type": "string",
                    "description": "ISO 8601 datetime upper bound e.g. 2026-02-24T00:00:00Z",
                },
                "maxResults": {
                    "type": "integer",
                    "description": "Max number of events to return. Default 20.",
                },
            },
            "required": ["timeMin", "timeMax"],
        },
    },
}

INITIAL_EMAIL_SYSTEM_PROMPT = (
    "You are Kalendra, a warm and professional scheduling assistant acting on behalf of {sender_name}.\n"
    "Your goal is to write the first outbound email to initiate scheduling a meeting with the recipient.\n\n"
    "Guidelines:\n"
    "- Match this flow used in email-poc: greeting, warm intro, meeting ask, short friendly close\n"
    "- Be warm, professional, and natural - write like a thoughtful human assistant\n"
    "- Keep the email concise but not abrupt; friendly but not overly casual\n"
    "- Never mention you are an AI\n"
    "- Do not sign as Kalendra; sign off as {sender_name}\n"
    "- Do not include subject lines or headers in your response body\n"
    "- NEVER paste the context text verbatim as a standalone sentence\n"
    "- Do not include any title/subject line inside the body\n"
    "Respond with exactly two parts separated by a blank line:\n"
    "1. First line: SUBJECT: <your subject line>\n"
    "2. Remaining lines: The email body (greeting, message, sign-off with sender name)."
)


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
    user_timezone: str | None = None,
    user_timezone_offset_minutes: int | None = None,
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
    def _parse_iso(value: str | None) -> datetime | None:
        if not value:
            return None
        raw = value.strip()
        if not raw:
            return None
        try:
            if raw.endswith("Z"):
                raw = raw[:-1] + "+00:00"
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None

    def _localize(dt_value: str | None) -> str | None:
        dt = _parse_iso(dt_value)
        if not dt:
            return None
        if user_timezone:
            try:
                return dt.astimezone(ZoneInfo(user_timezone)).strftime("%a, %b %d, %I:%M %p")
            except Exception:
                pass
        if user_timezone_offset_minutes is not None:
            try:
                local_dt = dt + timedelta(minutes=int(user_timezone_offset_minutes))
                return local_dt.strftime("%a, %b %d, %I:%M %p")
            except Exception:
                pass
        return dt.astimezone(timezone.utc).strftime("%a, %b %d, %I:%M %p UTC")

    slim = []
    for e in events:
        start_obj = e.get("start") or {}
        end_obj = e.get("end") or {}
        start_value = start_obj.get("dateTime") or start_obj.get("date")
        end_value = end_obj.get("dateTime") or end_obj.get("date")
        slim.append(
            {
                "summary": e.get("summary", "Busy"),
                "start": start_value,
                "end": end_value,
                "startLocal": _localize(start_value),
                "endLocal": _localize(end_value),
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


def generate_initial_email(
    sender_name: str,
    recipient_name: str,
    context: str,
) -> tuple[str, str]:
    """
    Generate the first outbound email (subject + body) via LLM.
    Returns (subject, body). No hardcoded templates.
    """
    client = _openai_client()
    system = INITIAL_EMAIL_SYSTEM_PROMPT.format(sender_name=sender_name)
    user_msg = (
        f"Context: {context}\n\n"
        f"Recipient name: {recipient_name or 'there'}\n\n"
        "Write the first scheduling email in email-poc style. "
        "Personalize naturally from context, but do not copy instruction-like wording. "
        "Respond with SUBJECT: on first line, blank line, then body only."
    )
    response = client.chat.completions.create(
        model=os.getenv("OPENAI_MODEL", "gpt-5-mini"),
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg},
        ],
    )
    content = (response.choices[0].message.content or "").strip()
    if not content:
        raise ValueError("LLM returned empty initial email")

    lines = content.split("\n")
    subject = "Meeting Request"
    body_start = 0
    for i, line in enumerate(lines):
        if line.strip().upper().startswith("SUBJECT:"):
            subject = line.split(":", 1)[-1].strip()
            if not subject:
                subject = "Meeting Request"
            body_start = i + 1
            break
    body_lines = lines[body_start:]
    body = "\n".join(body_lines).strip()
    # Guardrails: body must not repeat Subject/title line.
    body_clean_lines = [ln for ln in body.split("\n")]
    while body_clean_lines and not body_clean_lines[0].strip():
        body_clean_lines.pop(0)
    if body_clean_lines and body_clean_lines[0].strip().lower().startswith("subject:"):
        body_clean_lines = body_clean_lines[1:]
    if body_clean_lines and subject and body_clean_lines[0].strip().lower() == subject.strip().lower():
        body_clean_lines = body_clean_lines[1:]
    body = "\n".join(body_clean_lines).strip()
    if not body:
        raise ValueError("LLM returned no email body")
    return subject, body


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


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _local_tzinfo(user_timezone: str | None, user_timezone_offset_minutes: int | None):
    if user_timezone:
        try:
            return ZoneInfo(user_timezone)
        except Exception:
            pass
    if user_timezone_offset_minutes is not None:
        try:
            return timezone(timedelta(minutes=int(user_timezone_offset_minutes)))
        except Exception:
            return None
    return None


def normalize_finalized_event_times(
    event_data: dict[str, Any],
    user_timezone: str | None = None,
    user_timezone_offset_minutes: int | None = None,
) -> dict[str, Any]:
    """
    Ensure finalized start/end are interpreted in sender-local timezone, not UTC.
    If LLM emits UTC/no-tz timestamps, reinterpret them as local wall-clock times.
    """
    if not isinstance(event_data, dict):
        return event_data
    tz = _local_tzinfo(user_timezone, user_timezone_offset_minutes)
    if not tz:
        return event_data

    out = dict(event_data)
    for key in ("startTime", "endTime"):
        raw = out.get(key)
        if not raw or not isinstance(raw, str):
            continue
        dt = _parse_iso_datetime(raw)
        if not dt:
            continue
        is_utc_or_naive = (dt.tzinfo is None) or (dt.utcoffset() == timedelta(0))
        if not is_utc_or_naive:
            continue
        # Keep wall-clock intent (e.g. "3 PM") but pin to user-local timezone.
        naive = dt.replace(tzinfo=None)
        localized = naive.replace(tzinfo=tz)
        out[key] = localized.isoformat()
    return out


def _intervals_overlap(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return a_start < b_end and a_end > b_start


def find_conflicting_event(
    access_token: str,
    event_data: dict[str, Any],
    provider: str = "google",
    refresh_access_token: Callable[[], str | None] | None = None,
) -> dict[str, Any] | None:
    """
    Return the first conflicting event if overlap exists, else None.
    """
    start_dt = _parse_iso_datetime(str(event_data.get("startTime", "")))
    end_dt = _parse_iso_datetime(str(event_data.get("endTime", "")))
    if not start_dt or not end_dt or end_dt <= start_dt:
        return None

    # Query slightly wider to catch events that start earlier but overlap.
    query_min = (start_dt - timedelta(hours=12)).isoformat()
    query_max = (end_dt + timedelta(hours=12)).isoformat()
    events = _get_calendar_events(
        access_token=access_token,
        provider=provider,
        time_min=query_min,
        time_max=query_max,
        max_results=50,
        refresh_access_token=refresh_access_token,
    )
    for ev in events:
        ev_start = _parse_iso_datetime(ev.get("start"))
        ev_end = _parse_iso_datetime(ev.get("end"))
        if not ev_start or not ev_end:
            continue
        if _intervals_overlap(start_dt, end_dt, ev_start, ev_end):
            return ev
    return None


def generate_scheduling_reply(
    thread_messages: list[str],
    access_token: str,
    provider: str = "google",
    sender_name: str = "Scheduler Team",
    context: str = "Schedule a meeting professionally.",
    refresh_access_token: Callable[[], str | None] | None = None,
    user_timezone: str | None = None,
    user_timezone_offset_minutes: int | None = None,
) -> tuple[str, dict[str, Any] | None]:
    """
    Tool-calling LLM loop (matches email-poc generate_reply):
    1. LLM decides to call get_calendar_events with a time range
    2. We fetch events and return them
    3. LLM reasons over availability and writes the email reply
    Returns (reply_body, finalized_event_dict_or_None).
    """
    client = _openai_client()
    system = SYSTEM_PROMPT.format(sender_name=sender_name)
    thread_display = "\n---\n".join(thread_messages) if thread_messages else "(no prior messages)"
    timezone_line = "unknown timezone"
    if user_timezone:
        timezone_line = f"timezone: {user_timezone}"
    elif user_timezone_offset_minutes is not None:
        timezone_line = f"timezone offset: {user_timezone_offset_minutes} minutes from UTC"

    user_prompt = (
        f"Context: {context}\n\n"
        f"Recipient time preference: {timezone_line}\n\n"
        "Full email thread (oldest to newest):\n"
        f"{thread_display}\n\n"
        "Check the calendar for availability then write the next reply to move towards finalizing a meeting time. "
        "When proposing times, use recipient-local phrasing and avoid UTC notation."
    )

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": system},
        {"role": "user", "content": user_prompt},
    ]

    while True:
        response = client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-5-mini"),
            messages=messages,
            tools=[GET_EVENTS_TOOL],
            tool_choice="auto",
        )
        msg = response.choices[0].message

        if not msg.tool_calls:
            content = (msg.content or "").strip()
            body, finalized = _extract_finalized(content)
            if not body:
                body = (
                    "Thanks for the update. I can help finalize scheduling - "
                    "please confirm your preferred time slot."
                )
            # Avoid repetitive greetings in ongoing turns.
            lines = body.split("\n")
            while lines and not lines[0].strip():
                lines = lines[1:]
            if lines and lines[0].strip().lower().startswith(("hi ", "hello ", "dear ")):
                lines = lines[1:]
                while lines and not lines[0].strip():
                    lines = lines[1:]
                body = "\n".join(lines).strip()
            return body, finalized

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
                user_timezone=user_timezone,
                user_timezone_offset_minutes=user_timezone_offset_minutes,
            )
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": json.dumps(events),
                }
            )
