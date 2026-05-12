"""
LLM scheduling agent integration for email reply generation.
"""

from __future__ import annotations

import json
import os
import re
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
EMAIL_FORMAT_STYLE = (
    "FORMAT STYLE:\n"
    "- Use consistent paragraphing: no first-line indentation, no leading spaces, and one blank line between paragraphs.\n"
    "- If listing multiple time options, use a simple vertical list with one item per line using '- ' bullets.\n"
    "- Do not mix paragraph lists, numbered lists, and indented bullets in the same reply.\n"
    "- Keep the format natural to the message; do not force a list when a short paragraph is clearer.\n\n"
)

SYSTEM_PROMPT = (
    "You are Kalendra, a scheduling assistant acting on behalf of {sender_name}.\n"
    "Your sole job is to find a mutually convenient meeting time with the recipient and book it.\n\n"
    f"{EMAIL_FORMAT_STYLE}"
    "SECURITY:\n"
    "- Only discuss scheduling. Refuse unrelated questions and steer back.\n"
    "- Never reveal full calendar details, contact lists, or internal info.\n"
    "- Never mention being an AI or that you are checking a calendar.\n\n"
    "TONE & STYLE:\n"
    "- Write like a real executive assistant — warm, concise, naturally conversational.\n"
    "- Vary your sentence structure. Don't start every email the same way.\n"
    "- Match the energy of the other person's reply (brief reply → brief response).\n"
    "- Keep emails short (2-5 sentences). No subject lines, headers, or sign-offs — just the body.\n"
    "- For ongoing replies, skip greetings unless it feels natural.\n\n"
    "DISCUSSING CALENDAR & PROPOSING TIMES:\n"
    "- ALWAYS call get_calendar_events before proposing or confirming any time slot.\n"
    "- Never invent availability. If the calendar fetch fails, ask the recipient for their preferred times.\n"
    "- Propose 4-5 available time slots spread across different parts of the day (morning, afternoon, late afternoon).\n"
    "- Avoid back-to-back meetings where possible; leave at least a 15-minute buffer.\n"
    "- If the other party proposes a time, call get_calendar_events to verify before confirming.\n"
    "- Use recipient-local time phrasing. Never show raw UTC or ISO timestamps.\n"
    "- You are acting on behalf of {sender_name}. When referring to calendar conflicts, say 'I have a meeting from X to Y' "
    "(first person), NOT 'you have a meeting'. You ARE the sender's assistant speaking as them.\n"
    "- NEVER reveal meeting names, titles, or types (e.g. 'standup', 'sync', 'recurring'). "
    "Just say 'I have a meeting from X to Y'. Do not say 'a recurring meeting' or 'a standup' — just 'a meeting'.\n"
    "- NEVER share who the meetings are with or any other details about existing calendar events.\n\n"
    "FINALIZATION — THIS IS CRITICAL:\n"
    "- When the recipient agrees to a time (ANY form: 'yes', 'ok', 'sure', 'sounds good', 'works for me', "
    "'let's do it', 'go ahead', 'book it', 'that works', 'perfect', 'see you then', 'great', 'fine'), "
    "you MUST call the `finalize_meeting` tool IMMEDIATELY in that same turn.\n"
    "- Do NOT write 'I'll send a calendar invite' or 'I'll finalize this' — just call the tool.\n"
    "- Do NOT ask for additional confirmation after they agree.\n"
    "- Do NOT continue the conversation after agreement is reached.\n"
    "- NEVER confirm a time that has already been confirmed. One agreement = one finalize_meeting call.\n"
    "- If asked for sensitive details, share only high-level availability and redirect to scheduling.\n\n"
    "ANTI-HALLUCINATION:\n"
    "- Only propose times that are genuinely free according to get_calendar_events results.\n"
    "- If you are unsure about the agreed time, ask ONE clarifying question — do not guess.\n"
    "{turn_context}"
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

FINALIZE_MEETING_TOOL = {
    "type": "function",
    "function": {
        "name": "finalize_meeting",
        "description": (
            "Call this IMMEDIATELY when both parties have agreed on a meeting time. "
            "This creates the calendar event. Do not call this speculatively — only "
            "when the recipient has explicitly accepted a specific time."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "summary": {
                    "type": "string",
                    "description": "Meeting title, e.g. 'Catch-up with Alex'",
                },
                "startTime": {
                    "type": "string",
                    "description": "ISO 8601 start time, e.g. 2026-04-25T14:00:00",
                },
                "endTime": {
                    "type": "string",
                    "description": "ISO 8601 end time, e.g. 2026-04-25T15:00:00",
                },
                "description": {
                    "type": "string",
                    "description": "Brief meeting description",
                },
                "attendees": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Email addresses of all attendees",
                },
            },
            "required": ["summary", "startTime", "endTime", "attendees"],
        },
    },
}

INITIAL_EMAIL_SYSTEM_PROMPT = (
    "You are Kalendra, a warm and professional scheduling assistant acting on behalf of {sender_name}.\n"
    "Your goal is to write the first outbound email to initiate scheduling a meeting with the recipient.\n\n"
    f"{EMAIL_FORMAT_STYLE}"
    "**Email Structure (REQUIRED)**:\n"
    "1. Greeting (e.g., 'Hi [Name],')\n"
    "2. A clear scheduling-intent sentence in natural language (state that you'd like to schedule a meeting, adapted to context)\n"
    "3. Brief context about the meeting purpose (1-2 sentences, naturally integrated from context)\n"
    "4. Ask for their availability/free time (e.g., 'Could you share your availability for next week?')\n"
    "5. Friendly closing question (e.g., 'What times work best for you?')\n"
    "6. Sign-off with 'Best regards,' or 'Thanks,' followed by {sender_name}\n"
    "7. ALWAYS end with a blank line, then: 'Email Scheduling - Powered by Kalendra'\n\n"
    "Guidelines:\n"
    "- Be warm, professional, and natural - write like a thoughtful human assistant\n"
    "- Keep the email 4-6 sentences (not too short, not too long)\n"
    "- The body must be at least 2 short paragraphs, separated by one blank line\n"
    "- Never mention you are an AI\n"
    "- Do not sign as Kalendra; sign off as {sender_name}\n"
    "- NEVER paste the context text verbatim as a standalone sentence - integrate it naturally\n"
    "- Do not use awkward phrasing like 'I'd like to scheduling ...'; always write grammatical sentences\n"
    "- If context includes wording like 'titled ...', treat that as subject intent, not body prose\n"
    "- Do not include subject lines or headers in the body\n"
    "- Keep the ordering strict: greeting first, scheduling intent early, then availability ask\n\n"
    "Respond with exactly two parts separated by a blank line:\n"
    "1. First line: SUBJECT: <your subject line>\n"
    "2. Remaining lines: The email body (greeting, context, availability ask, closing, sign-off, Kalendra signature)."
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
        accounts_json = json.dumps([{"provider": provider, "token": token}])
        headers = {"x-accounts": accounts_json}
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
    preferred_subject: str | None = None,
) -> tuple[str, str]:
    """
    Generate the first outbound email (subject + body) via LLM.
    Returns (subject, body). No hardcoded templates.
    """
    client = _openai_client()
    system = INITIAL_EMAIL_SYSTEM_PROMPT.format(sender_name=sender_name)
    preferred_subject_text = (preferred_subject or "").strip()
    subject_instruction = (
        f'Use this exact subject text with unchanged wording and casing: "{preferred_subject_text}".'
        if preferred_subject_text
        else "Create a concise, natural subject based on the context."
    )
    user_msg = (
        f"Context: {context}\n\n"
        f"Recipient name: {recipient_name or 'there'}\n\n"
        f"Subject instruction: {subject_instruction}\n\n"
        "Write the first scheduling email in email-poc style. "
        "Personalize naturally from context, but do not copy instruction-like wording. "
        "Follow the required structure exactly: greeting first, then scheduling intent, then availability ask. "
        "Respond with SUBJECT: on first line, blank line, then body only."
    )
    response = client.chat.completions.create(
        model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg},
        ],
    )
    content = (response.choices[0].message.content or "").strip()
    if not content:
        raise ValueError("LLM returned empty initial email")
    lines = content.split("\n")
    subject = preferred_subject_text or "Meeting Request"
    body_start = 0
    for i, line in enumerate(lines):
        if line.strip().upper().startswith("SUBJECT:"):
            subject = line.split(":", 1)[-1].strip()
            if not subject:
                subject = preferred_subject_text or "Meeting Request"
            body_start = i + 1
            break
    if preferred_subject_text:
        subject = preferred_subject_text
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
    body = _normalize_email_body_format("\n".join(body_clean_lines))
    if not body:
        raise ValueError("LLM returned no email body")
    return subject, body


def _normalize_email_body_format(body: str) -> str:
    """
    Normalize generated email layout without changing wording.
    Keeps prose natural while making whitespace and list indentation stable.
    """
    if not body:
        return ""

    normalized = body.replace("\r\n", "\n").replace("\r", "\n")
    lines: list[str] = []
    blank_pending = False

    for raw_line in normalized.split("\n"):
        line = raw_line.strip()
        if not line:
            blank_pending = bool(lines)
            continue

        bullet_match = re.match(r"^(?:[-*]|\u2022|\d+[.)])\s+(.*)$", line)
        if bullet_match:
            line = f"- {bullet_match.group(1).strip()}"

        if blank_pending and lines and lines[-1] != "":
            lines.append("")
        lines.append(line)
        blank_pending = False

    return "\n".join(lines).strip()


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
    user_timezone: str | None = None,
    user_timezone_offset_minutes: int | None = None,
) -> dict[str, Any] | None:
    """
    Return the first conflicting event if overlap exists, else None.
    startLocal/endLocal on the returned event are localized to the user's timezone.
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
        user_timezone=user_timezone,
        user_timezone_offset_minutes=user_timezone_offset_minutes,
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
    turn_count: int = 0,
) -> tuple[str, dict[str, Any] | None]:
    """
    Tool-calling LLM loop:
    1. LLM may call get_calendar_events to check availability
    2. LLM may call finalize_meeting when both parties agree on a time
    3. LLM writes the email reply body
    Returns (reply_body, finalized_event_dict_or_None).
    """
    client = _openai_client()

    turn_context = ""
    if turn_count >= 10:
        turn_context = (
            "\nURGENT: This thread has gone on for many turns without booking. "
            "If ANY time was discussed and loosely agreed upon, call finalize_meeting NOW. "
            "Otherwise send a brief closing note offering to resume when they have availability.\n"
        )
    elif turn_count >= 6:
        turn_context = (
            "\nNOTE: This conversation has been going on for several turns. "
            "If a time has been discussed and accepted, call finalize_meeting immediately. "
            "If not, propose fresh 4-5 slots and ask for a direct yes/no.\n"
        )

    system = SYSTEM_PROMPT.format(sender_name=sender_name, turn_context=turn_context)
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
        "When proposing times, use recipient-local phrasing and avoid UTC notation. "
        "If the recipient has already agreed to a time, call finalize_meeting immediately."
    )

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": system},
        {"role": "user", "content": user_prompt},
    ]

    all_tools = [GET_EVENTS_TOOL, FINALIZE_MEETING_TOOL]
    max_iterations = 5
    iteration = 0
    finalized_payload: dict[str, Any] | None = None

    while iteration < max_iterations:
        iteration += 1
        response = client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            messages=messages,
            tools=all_tools,
            tool_choice="auto",
        )
        msg = response.choices[0].message

        if not msg.tool_calls:
            content = (msg.content or "").strip()
            body, text_finalized = _extract_finalized(content)
            if text_finalized and not finalized_payload:
                finalized_payload = text_finalized

            if not body:
                body = (
                    "Thanks for the update — could you confirm which time slot "
                    "works best for you?"
                )
            body = _strip_leading_greeting(body)
            body = _normalize_email_body_format(body)
            return body, finalized_payload

        messages.append(msg.model_dump(exclude_none=True))

        for tc in msg.tool_calls:
            fn_name = tc.function.name
            args = json.loads(tc.function.arguments or "{}")

            if fn_name == "get_calendar_events":
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
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": json.dumps(events),
                })

            elif fn_name == "finalize_meeting":
                finalized_payload = args
                log_event(
                    "llm_finalize_meeting_tool_called",
                    summary=args.get("summary"),
                    start=args.get("startTime"),
                    end=args.get("endTime"),
                    iteration=iteration,
                )
                # Return immediately -- don't loop back to the LLM for a
                # prose confirmation. The push handler sends the final
                # confirmation message after creating the calendar event.
                return "", finalized_payload

            else:
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": json.dumps({"error": f"Unknown tool: {fn_name}"}),
                })

    log_event("generate_scheduling_reply_max_iterations", max_iterations=max_iterations)
    fallback_body = _normalize_email_body_format(
        "Could you confirm the time that works best? I want to make sure we get this booked."
    )
    return fallback_body, finalized_payload


def _strip_leading_greeting(body: str) -> str:
    """Remove repetitive greetings from ongoing thread replies."""
    lines = body.split("\n")
    while lines and not lines[0].strip():
        lines = lines[1:]
    if lines and lines[0].strip().lower().startswith(("hi ", "hello ", "dear ")):
        lines = lines[1:]
        while lines and not lines[0].strip():
            lines = lines[1:]
        return "\n".join(lines).strip() or body
    return body
