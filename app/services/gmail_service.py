from __future__ import annotations

import base64
import json
import os
import re
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import parseaddr
from typing import Any, Callable

from google.auth.transport import requests as g_requests
from google.oauth2 import id_token as g_id_token

from app.logging_config import log_event
from app.utils.email_footer import build_html_email
from app.repository import (
    create_thread,
    find_recent_active_thread_by_recipient,
    find_single_active_thread_for_user,
    find_thread_by_gmail_thread,
    mark_thread_message_processed,
    save_thread,
    update_watch_state,
    get_watch_state,
)
from app.services.agent_service import (
    classify_recipient_intent,
    should_fire_stalled,
    create_calendar_event,
    find_conflicting_event,
    generate_initial_email,
    generate_scheduling_reply,
)
from app.services.fcm_service import send_flow_completed, send_intent_notification
from app.token_manager import TokenManager
from app.utils.http_client import request_with_refresh
from app.utils.text_utils import strip_reply_prefix, strip_quoted_reply
from app.utils.time_utils import format_event_time, normalize_finalized_event_times

GMAIL_API_BASE = "https://gmail.googleapis.com/gmail/v1/users/me"
MAX_AGENT_TURNS = int(os.getenv("MAX_AGENT_TURNS", "15"))
_SNIPPET_MAX_CHARS = 120


def compute_thread_status(thread_data: dict, intent: str = "") -> str:
    """
    Derive the UI-facing thread_status from stored thread fields + optional classified intent.

    Status priority (highest to lowest):
      cancelled        — LLM classified intent is 'cancelled'
      meeting_executed — status==completed and finalized_start_time has passed
      reschedule_requested — status==completed and intent==reschedule_booked
      scheduled        — status==completed and meeting time is still in the future
      negotiating_time — status==active and turn_count > 0
      initiated        — status==active and turn_count == 0
    """
    if intent == "cancelled":
        return "cancelled"

    status = str(thread_data.get("status", "active")).lower()
    turn_count = int(thread_data.get("turn_count", 0))

    if status == "completed":
        if intent == "reschedule_booked":
            return "reschedule_requested"
        start_str = str(thread_data.get("finalized_start_time", "") or "")
        if start_str:
            try:
                start_dt = datetime.fromisoformat(start_str)
                if start_dt.tzinfo is None:
                    start_dt = start_dt.replace(tzinfo=timezone.utc)
                if datetime.now(timezone.utc) >= start_dt:
                    return "meeting_executed"
            except Exception:
                pass
        return "scheduled"

    # active thread
    if turn_count == 0:
        return "initiated"
    return "negotiating_time"


def _make_snippet(text: str) -> str:
    text = " ".join(text.split())
    return text[:_SNIPPET_MAX_CHARS].rstrip() + ("..." if len(text) > _SNIPPET_MAX_CHARS else "")


def _gmail(token: str, method: str, path: str, refresh_fn: Callable[[], str | None] | None = None, **kwargs):
    return request_with_refresh(method, f"{GMAIL_API_BASE}{path}", token, refresh_fn, **kwargs)


def verify_pubsub_auth_header(auth_header: str | None, expected_audience: str) -> dict[str, Any]:
    if not auth_header or not auth_header.lower().startswith("bearer "):
        return {"ok": False, "error": "missing_bearer_token"}
    raw_token = auth_header.split(" ", 1)[1].strip()
    if not raw_token:
        return {"ok": False, "error": "empty_bearer_token"}
    try:
        req = g_requests.Request()
        claims = g_id_token.verify_oauth2_token(raw_token, req, audience=expected_audience)
        return {"ok": True, "data": {"email": claims.get("email"), "claims": claims}}
    except Exception as exc:
        return {"ok": False, "error": f"invalid_pubsub_auth:{exc}"}


def renew_gmail_watch(user_email: str, token_manager: TokenManager) -> dict[str, Any]:
    topic = os.getenv("GMAIL_PUBSUB_TOPIC")
    if not topic:
        log_event("gmail_watch_renew_failed", user_email=user_email, error="missing_topic")
        return {"ok": False, "error": "GMAIL_PUBSUB_TOPIC is not configured"}

    token = token_manager.get_fresh_token(user_email=user_email, provider="google")
    if not token:
        log_event("gmail_watch_renew_failed", user_email=user_email, error="token_unavailable")
        return {"ok": False, "error": "No valid Google token"}

    body: dict[str, Any] = {"topicName": topic}
    label_ids = os.getenv("GMAIL_WATCH_LABEL_IDS")
    if label_ids:
        body["labelIds"] = [x.strip() for x in label_ids.split(",") if x.strip()]
        body["labelFilterAction"] = "include"

    resp = _gmail(token, "POST", "/watch", json=body)
    if resp.status_code != 200:
        log_event("gmail_watch_renew_failed", user_email=user_email, status_code=resp.status_code, response=resp.text[:500])
        return {"ok": False, "error": f"watch renew failed: {resp.status_code}"}

    payload = resp.json()
    expiration_ms = int(payload.get("expiration", "0"))
    watch_expiration = datetime.fromtimestamp(expiration_ms / 1000, tz=timezone.utc)
    renew_at = watch_expiration - timedelta(days=1)
    history_id = str(payload.get("historyId", ""))

    save_result = update_watch_state(
        provider="google",
        user_email=user_email,
        data={"history_id": history_id, "watch_expiration": watch_expiration, "expires_at": renew_at},
    )
    if not save_result.get("ok"):
        return {"ok": False, "error": save_result.get("error", "watch_state_update_failed")}

    log_event("gmail_watch_renewed", user_email=user_email, history_id=history_id, watch_expiration=watch_expiration.isoformat())
    return {"ok": True, "data": {"user_email": user_email, "history_id": history_id}}


def _ensure_watch(token: str, user_email: str) -> dict[str, Any]:
    topic = os.getenv("GMAIL_PUBSUB_TOPIC")
    if not topic:
        return {"ok": False, "error": "GMAIL_PUBSUB_TOPIC is not configured"}

    body: dict[str, Any] = {"topicName": topic}
    label_ids = os.getenv("GMAIL_WATCH_LABEL_IDS")
    if label_ids:
        body["labelIds"] = [x.strip() for x in label_ids.split(",") if x.strip()]
        body["labelFilterAction"] = "include"

    resp = _gmail(token, "POST", "/watch", json=body)
    if resp.status_code != 200:
        log_event("gmail_watch_http_error", status_code=resp.status_code, response=resp.text[:500], topic=topic)
        return {"ok": False, "error": f"watch_failed:{resp.status_code}", "response": resp.text[:500]}

    payload = resp.json()
    history_id = str(payload.get("historyId", ""))
    expiration_ms = int(payload.get("expiration", "0"))
    watch_expiration = datetime.fromtimestamp(expiration_ms / 1000, tz=timezone.utc) if expiration_ms else None
    renew_at = (watch_expiration - timedelta(days=1)) if watch_expiration else None

    save_data: dict[str, Any] = {"history_id": history_id}
    if watch_expiration:
        save_data["watch_expiration"] = watch_expiration
    if renew_at:
        save_data["expires_at"] = renew_at

    save_res = update_watch_state(provider="google", user_email=user_email, data=save_data)
    if not save_res.get("ok"):
        return {"ok": False, "error": save_res.get("error", "watch_state_update_failed")}

    return {"ok": True, "data": {"history_id": history_id, "watch_expiration": watch_expiration.isoformat() if watch_expiration else None}}


def _get_message_metadata(token: str, message_id: str, refresh_fn=None) -> dict[str, str] | None:
    resp = _gmail(token, "GET", f"/messages/{message_id}", refresh_fn,
                  params={"format": "metadata", "metadataHeaders": ["From", "Subject", "Message-ID"]})
    if resp.status_code != 200:
        log_event("gmail_message_fetch_failed", message_id=message_id, status_code=resp.status_code)
        return None
    payload = resp.json().get("payload", {})
    headers = {h.get("name", ""): h.get("value", "") for h in payload.get("headers", [])}
    return {"from": headers.get("From", ""), "subject": headers.get("Subject", ""), "message_id_header": headers.get("Message-ID", "")}


def _extract_text_body(payload: dict) -> str:
    mime_type = payload.get("mimeType", "")
    if mime_type == "text/plain":
        data = (payload.get("body") or {}).get("data", "")
        if data:
            try:
                return base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
            except Exception:
                return ""
        return ""
    for part in payload.get("parts") or []:
        text = _extract_text_body(part)
        if text:
            return text
    return ""


def _get_thread_messages(token: str, thread_id: str, refresh_fn=None) -> list[str]:
    resp = _gmail(token, "GET", f"/threads/{thread_id}", refresh_fn, params={"format": "full"})
    if resp.status_code != 200:
        log_event("gmail_thread_fetch_failed", thread_id=thread_id, status_code=resp.status_code)
        return []
    messages = []
    for m in resp.json().get("messages", []):
        payload = m.get("payload", {})
        headers = {h.get("name", ""): h.get("value", "") for h in payload.get("headers", [])}
        sender = headers.get("From", "Unknown")
        body = _extract_text_body(payload).strip() or (m.get("snippet") or "").strip()
        if body:
            messages.append(f"From: {sender}\n{body[:800]}")
    return messages


def _strip_quoted_reply(text: str) -> str:
    """Remove quoted reply blocks from email body, keeping only the new message content."""
    # Remove Gmail-style HTML quote block if present (shouldn't be in plain text but guard anyway)
    # Split into lines and walk until we hit a quote boundary
    lines = text.splitlines()
    clean: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Hard stop: any line starting with >
        if stripped.startswith(">"):
            break

        # Hard stop: --- or ___ separator lines
        if re.match(r"^[-_]{3,}$", stripped):
            break

        # "On <date>, <name> wrote:" — may span 1–3 lines ending with "wrote:"
        # Detect the start: line begins with "On " and either ends with "wrote:" on same
        # line or within the next 3 lines
        if re.match(r"^on\s+", stripped, re.IGNORECASE):
            # Gather up to 4 lines to see if this block ends with "wrote:"
            lookahead = " ".join(lines[i:i+4]).strip()
            if re.search(r"wrote:\s*$", lookahead, re.IGNORECASE):
                break

        # "From: " header at the start of a forwarded block (only stop if we already have content)
        if re.match(r"^from\s*:", stripped, re.IGNORECASE) and clean:
            break

        clean.append(line)
        i += 1

    return "\n".join(clean).strip()


def get_thread_messages_structured(token: str, thread_id: str, refresh_fn=None) -> list[dict]:
    """Return thread messages as structured dicts: {sender, body, timestamp (ISO)}."""
    resp = _gmail(token, "GET", f"/threads/{thread_id}", refresh_fn, params={"format": "full"})
    if resp.status_code != 200:
        log_event("gmail_thread_fetch_failed", thread_id=thread_id, status_code=resp.status_code)
        return []
    result = []
    for m in resp.json().get("messages", []):
        payload = m.get("payload", {})
        headers = {h.get("name", ""): h.get("value", "") for h in payload.get("headers", [])}
        sender = headers.get("From", "Unknown")
        raw_body = _extract_text_body(payload).strip() or (m.get("snippet") or "").strip()
        body = _strip_quoted_reply(raw_body) or raw_body
        if not body:
            continue
        internal_ms = m.get("internalDate")
        if internal_ms:
            try:
                ts = datetime.fromtimestamp(int(internal_ms) / 1000, tz=timezone.utc).isoformat()
            except Exception:
                ts = headers.get("Date", "")
        else:
            ts = headers.get("Date", "")
        result.append({"sender": sender, "body": body, "timestamp": ts})
    return result


def _send_reply(token: str, user_email: str, to_email: str, subject: str, body: str,
                thread_id: str, parent_message_id: str, refresh_fn=None,
                sender_name: str = "") -> bool:
    subj = subject.strip() or "Re:"
    if not subj.lower().startswith("re:"):
        subj = f"Re: {subj}"

    msg = MIMEMultipart("alternative")
    msg["To"] = to_email
    msg["Subject"] = subj
    msg["In-Reply-To"] = parent_message_id
    msg["References"] = parent_message_id
    msg.attach(MIMEText(body, "plain", "utf-8"))
    msg.attach(MIMEText(build_html_email(body, sender_name), "html", "utf-8"))

    raw_b64 = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    send_resp = _gmail(token, "POST", "/messages/send", refresh_fn, json={"raw": raw_b64, "threadId": thread_id})
    if send_resp.status_code not in (200, 202):
        log_event("agent_reply_send_failed", user_email=user_email, status_code=send_resp.status_code, response=send_resp.text[:500])
        return False
    log_event("email_sent", action="agent_reply", thread_id=thread_id, to=to_email)
    return True


def _send_initial_email(token: str, to_email: str, sender_name: str, recipient_name: str | None,
                        context: str, custom_subject: str | None = None, custom_body: str | None = None,
                        tone: str | None = None) -> tuple[str | None, str | None]:
    if custom_subject and custom_body:
        subject = custom_subject.strip()
        body = custom_body.strip()
    elif custom_body:
        body = custom_body.strip()
        try:
            generated_subject, _ = generate_initial_email(
                sender_name=sender_name,
                recipient_name=(recipient_name or "").strip() or "there",
                context=(context or "").strip() or "schedule a meeting",
                tone=tone,
            )
            subject = custom_subject.strip() if custom_subject else generated_subject
        except Exception as exc:
            log_event("initial_email_llm_failed", error=str(exc), to=to_email)
            subject = custom_subject.strip() if custom_subject else "Meeting Request"
    else:
        try:
            generated_subject, generated_body = generate_initial_email(
                sender_name=sender_name,
                recipient_name=(recipient_name or "").strip() or "there",
                context=(context or "").strip() or "schedule a meeting",
                tone=tone,
            )
            subject = custom_subject.strip() if custom_subject else generated_subject
            body = generated_body
        except Exception as exc:
            log_event("initial_email_llm_failed", error=str(exc), to=to_email)
            return None, None

    msg = MIMEMultipart("alternative")
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))
    msg.attach(MIMEText(build_html_email(body, sender_name), "html", "utf-8"))
    raw_b64 = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    send_resp = _gmail(token, "POST", "/messages/send", json={"raw": raw_b64})
    if send_resp.status_code not in (200, 202):
        log_event("gmail_initial_email_send_failed", to=to_email, status_code=send_resp.status_code, response=send_resp.text[:500])
        return None, None

    data = send_resp.json() if send_resp.content else {}
    thread_id = data.get("threadId")
    if not thread_id:
        log_event("gmail_initial_email_thread_missing", to=to_email)
        return None, None

    log_event("email_sent", action="initiate_flow", thread_id=thread_id, to=to_email)
    return thread_id, data.get("id")


def _has_explicit_rejection(thread_messages: list[str]) -> bool:
    if not thread_messages:
        return False
    latest = (thread_messages[-1] or "").lower()
    return bool(re.search(
        r"\b(cancel|never mind|don'?t book|wrong time|actually no|not that time|"
        r"hold off|stop|don'?t schedule|changed my mind|reschedule|pick another)\b", latest))


def _latest_message_allows_conflict(thread_messages: list[str]) -> bool:
    if not thread_messages:
        return False
    latest = (thread_messages[-1] or "").lower()
    # Only suppress conflict notification when the user explicitly acknowledged the conflict
    return bool(re.search(
        r"\b(book it anyway|schedule anyway|still book|still schedule|conflict is fine|"
        r"i am okay with (the )?conflict|go ahead anyway|conflict (is )?ok|"
        r"ignore the conflict|don'?t worry about (the )?conflict|proceed (despite|with) (the )?conflict)\b", latest))


def _guardrail_reply(thread_messages: list[str]) -> str | None:
    latest = (thread_messages[-1] or "").lower() if thread_messages else ""
    if re.search(r"\b(trump|epstein|politics|crypto price|stock tips|weather|sports score|movie|music|news)\b", latest):
        return "I can help with meeting scheduling in this thread. Please share a time window that works for you and I will coordinate it."
    if re.search(r"\b(who is in your meeting|last meeting with|all bookings|share your calendar)\b", latest):
        return "I can only share high-level availability for scheduling and cannot share private meeting details. If helpful, I can propose open time slots for our meeting."
    return None


def _handle_finalization(
    token: str, user_email: str, from_email: str, message_id: str,
    thread_id: str, thread_doc_id: str, thread_data: dict,
    finalized_payload: dict, meta: dict, convo: list[str],
    refresh_fn: Callable,
) -> tuple[bool, str | None]:
    """
    Attempt to create calendar event and send confirmation.
    Returns (finalized: bool, fallback_reply_text: str | None).
    """
    finalized_payload = normalize_finalized_event_times(
        finalized_payload,
        user_timezone=(thread_data.get("user_timezone") or "").strip() or None,
        user_timezone_offset_minutes=thread_data.get("user_timezone_offset_minutes"),
    )
    conflict = find_conflicting_event(
        access_token=token, event_data=finalized_payload, provider="google",
        refresh_access_token=refresh_fn,
        user_timezone=(thread_data.get("user_timezone") or "").strip() or None,
        user_timezone_offset_minutes=thread_data.get("user_timezone_offset_minutes"),
    )
    if conflict and not _latest_message_allows_conflict(convo):
        raw_start = conflict.get("startLocal") or conflict.get("start") or ""
        conflict_start = format_event_time(raw_start) if raw_start else "that time"
        # Parse day/time from conflict start for the notification
        parts = conflict_start.split(" at ", 1)
        conflict_day = parts[0].strip() if parts else conflict_start
        conflict_time = parts[1].strip() if len(parts) > 1 else ""
        send_intent_notification(
            user_email=user_email,
            recipient_name=thread_data.get("recipient_name", ""),
            recipient_email=from_email,
            meeting_title=thread_data.get("summary", ""),
            intent="calendar_conflict",
            conflict_day=conflict_day,
            conflict_time=conflict_time,
        )
        return False, f"I have a conflict at {conflict_start}. Should I book this anyway, or would you prefer a different slot?"

    existing_attendees = finalized_payload.get("attendees") or []
    normalized = {str(a).strip().lower() for a in existing_attendees if isinstance(a, str) and str(a).strip()}
    normalized.update({user_email.lower(), from_email.lower()})
    finalized_payload["attendees"] = sorted(normalized)

    create_result = create_calendar_event(token, finalized_payload, provider="google", refresh_access_token=refresh_fn)
    if create_result.get("ok"):
        friendly_time = format_event_time(finalized_payload.get("startTime", ""))
        confirm_text = f"All set! We're confirmed for {friendly_time}. A calendar invite is on its way to you."
        _send_reply(token, user_email, from_email, meta.get("subject", "Re:"), confirm_text,
                    thread_id, meta.get("message_id_header", ""), refresh_fn,
                    sender_name=thread_data.get("sender_name", ""))
        send_flow_completed(
            user_email=user_email,
            recipient_name=thread_data.get("recipient_name", ""),
            recipient_email=from_email,
            meeting_title=finalized_payload.get("summary", ""),
            start_time=finalized_payload.get("startTime", ""),
            end_time=finalized_payload.get("endTime", ""),
        )
        completed_data = {
            "status": "completed",
            "state": "completed",
            "finalized_at": datetime.now(timezone.utc),
            "finalized_start_time": finalized_payload.get("startTime", ""),
            "finalized_end_time": finalized_payload.get("endTime", ""),
            "finalized_summary": finalized_payload.get("summary", ""),
        }
        completed_data["thread_status"] = compute_thread_status(completed_data)
        save_thread(thread_doc_id, completed_data)
        log_event("thread_finalized", thread_doc_id=thread_doc_id)
        return True, None
    else:
        log_event("thread_finalize_create_event_failed", thread_doc_id=thread_doc_id, error=create_result.get("error"))
        return False, "I tried to book the meeting but ran into a technical issue. Let me try again shortly — no action needed on your end."


def _extract_notification_data(push_payload: dict[str, Any]) -> dict[str, str] | None:
    message = push_payload.get("message") or {}
    data_b64 = message.get("data")
    if not data_b64:
        return None
    try:
        decoded = base64.b64decode(data_b64).decode("utf-8")
        data = json.loads(decoded)
        return {"emailAddress": data.get("emailAddress", ""), "historyId": str(data.get("historyId", ""))}
    except Exception as exc:
        log_event("gmail_push_decode_failed", error=str(exc))
        return None


def initiate_google_email_flow(
    google_access_token: str, sender_name: str, recipient_email: str,
    recipient_name: str | None, context: str,
    email_subject: str | None = None, email_body: str | None = None,
    tone: str | None = None,
    user_timezone: str | None = None, user_timezone_offset_minutes: int | None = None,
    token_manager: TokenManager | None = None,
) -> dict[str, Any]:
    token = (google_access_token or "").strip()
    if not token:
        return {"ok": False, "error": "google_access_token is required", "status_code": 400}
    if not recipient_email or not sender_name:
        return {"ok": False, "error": "sender_name and recipient_email are required", "status_code": 400}

    profile_resp = _gmail(token, "GET", "/profile")
    if profile_resp.status_code != 200:
        return {"ok": False, "error": "failed_to_fetch_gmail_profile", "status_code": 401}
    user_email = (profile_resp.json().get("emailAddress") or "").lower().strip()
    if not user_email:
        return {"ok": False, "error": "gmail_profile_missing_email", "status_code": 401}

    if token_manager is not None:
        try:
            existing_refresh = token_manager.get_existing_refresh_token(user_email, "google")
            refresh_to_store = (
                existing_refresh if existing_refresh and not existing_refresh.strip().startswith("ya29.") else token
            )
            stored = token_manager.store_tokens(user_email=user_email, provider="google",
                                                access_token=token, refresh_token=refresh_to_store, expires_in_seconds=3300)
            if not stored:
                log_event("initiate_token_store_failed", user_email=user_email)
        except Exception as exc:
            log_event("initiate_token_store_exception", user_email=user_email, error=str(exc))

    thread_id, _ = _send_initial_email(token=token, to_email=recipient_email, sender_name=sender_name,
                                        recipient_name=recipient_name, context=context or "",
                                        custom_subject=email_subject, custom_body=email_body, tone=tone)
    if not thread_id:
        return {"ok": False, "error": "failed_to_send_initial_email", "status_code": 502}

    sent_body = email_body or ""
    created = create_thread(provider="google", user_email=user_email, data={
        "gmail_thread_id": thread_id, "status": "active", "state": "open", "turn_count": 0,
        "context": context or "", "sender_name": sender_name,
        "recipient_email": recipient_email.lower(), "recipient_name": (recipient_name or "").strip(),
        "summary": (email_subject or "").strip(),
        "last_message_snippet": _make_snippet(sent_body) if sent_body else "",
        "user_timezone": (user_timezone or "").strip(), "user_timezone_offset_minutes": user_timezone_offset_minutes,
        "attendees": [recipient_email.lower()], "source": "node_execute_initiate_email_flow",
        "initiated_at": datetime.now(timezone.utc),
        "thread_status": "initiated",
    })
    if not created.get("ok"):
        return {"ok": False, "error": created.get("error", "thread_create_failed"), "status_code": 500}

    enable_watch = os.getenv("ENABLE_INITIATE_WATCH_SETUP", "true").strip().lower() not in {"0", "false", "no", "off"}
    require_watch = os.getenv("REQUIRE_WATCH_SETUP_ON_INITIATE", "true").strip().lower() not in {"0", "false", "no", "off"}
    watch_result: dict[str, Any] = {"ok": True}

    if enable_watch:
        watch_result = _ensure_watch(token=token, user_email=user_email)
        if not watch_result.get("ok"):
            log_event("gmail_watch_setup_failed" if require_watch else "gmail_watch_setup_warning",
                      user_email=user_email, thread_id=thread_id, error=watch_result.get("error"))
            if require_watch:
                return {"ok": False, "error": watch_result.get("error", "gmail_watch_setup_failed"), "status_code": 502}
    elif require_watch:
        return {"ok": False, "error": "gmail_watch_setup_required_but_disabled", "status_code": 500}

    log_event("email_flow_initiated", thread_id=thread_id, user_email=user_email, recipient_email=recipient_email.lower())
    return {"ok": True, "data": {"status": "ok", "thread_id": thread_id, "watch_ok": bool(watch_result.get("ok")),
                                  "watch_error": None if watch_result.get("ok") else watch_result.get("error")}}


def process_gmail_push(push_payload: dict[str, Any], token_manager: TokenManager) -> dict[str, Any]:
    log_event("webhook_received", provider="google")
    parsed = _extract_notification_data(push_payload)
    if not parsed:
        return {"ok": False, "error": "invalid_push_payload"}

    email_address = parsed["emailAddress"]
    new_history_id = parsed["historyId"]

    watch_result = get_watch_state(provider="google", user_email=email_address)
    if not watch_result.get("ok"):
        return {"ok": True, "message": "watch user not found, ignored"}

    watch_data = watch_result["data"]
    old_history_id = str(watch_data.get("history_id", "")).strip()
    if not old_history_id:
        update_watch_state(provider="google", user_email=email_address, data={"history_id": new_history_id})
        return {"ok": True, "message": "history baseline set"}

    token = token_manager.get_fresh_token(email_address, "google")
    if not token:
        return {"ok": False, "error": "google_token_unavailable"}

    def refresh_fn():
        return token_manager.get_fresh_token(email_address, "google")

    history_resp = _gmail(token, "GET", "/history", refresh_fn,
                          params={"startHistoryId": old_history_id, "historyTypes": "messageAdded"})
    if history_resp.status_code != 200:
        if history_resp.status_code == 404:
            update_watch_state(provider="google", user_email=email_address, data={"history_id": new_history_id})
            return {"ok": True, "message": "history reset required; baseline updated"}
        return {"ok": False, "error": f"history_list_failed:{history_resp.status_code}"}

    history_items = history_resp.json().get("history", [])
    max_items = max(1, int(os.getenv("MAX_PUSH_MESSAGES_PER_WEBHOOK", "25")))
    processed_items = 0
    capped = False
    replied = 0
    finalized = 0

    for item in history_items:
        for added in item.get("messagesAdded", []):
            try:
                if processed_items >= max_items:
                    log_event("gmail_push_processing_capped", user_email=email_address, max_items=max_items)
                    capped = True
                    break

                msg = added.get("message", {})
                msg_id = msg.get("id")
                thread_id = msg.get("threadId")
                if not msg_id or not thread_id:
                    continue

                meta = _get_message_metadata(token, msg_id, refresh_fn)
                if not meta:
                    continue
                from_email = parseaddr(meta["from"])[1].lower()
                if not from_email or from_email == email_address.lower():
                    continue

                thread_lookup = find_thread_by_gmail_thread("google", email_address, thread_id)
                if thread_lookup.get("ok"):
                    thread_doc_id = thread_lookup["data"]["thread_doc_id"]
                    thread_data = thread_lookup["data"]
                else:
                    log_event("gmail_push_untracked_thread_ignored", user_email=email_address, gmail_thread_id=thread_id, from_email=from_email, subject=meta.get("subject", ""))
                    continue

                mark_res = mark_thread_message_processed(thread_doc_id, msg_id)
                if not mark_res.get("ok") or mark_res["data"].get("already_processed"):
                    continue

                # Completed threads: allow agent to keep replying until meeting time passes
                if str(thread_data.get("status", "")).lower() == "completed":
                    start_str = str(thread_data.get("finalized_start_time", "") or "")
                    meeting_passed = False
                    if start_str:
                        try:
                            start_dt = datetime.fromisoformat(start_str)
                            if start_dt.tzinfo is None:
                                start_dt = start_dt.replace(tzinfo=timezone.utc)
                            meeting_passed = datetime.now(timezone.utc) >= start_dt
                        except Exception:
                            pass
                    if meeting_passed:
                        # Meeting time has passed — silently ignore new messages, mark executed
                        save_thread(thread_doc_id, {
                            "thread_status": "meeting_executed",
                            "last_message_id": msg_id,
                        })
                        processed_items += 1
                        continue
                    # Meeting is still in the future — fall through to let agent reply normally

                turn_count = int(thread_data.get("turn_count", 0))
                if turn_count >= MAX_AGENT_TURNS:
                    log_event("agent_max_turns_reached", thread_doc_id=thread_doc_id, turn_count=turn_count)
                    continue

                convo = _get_thread_messages(token, thread_id, refresh_fn)
                if not convo:
                    continue

                # The last message in convo is the recipient's incoming message (agent hasn't replied yet)
                incoming_body = convo[-1] if convo else ""
                incoming_snippet = _make_snippet(incoming_body)

                # Classify recipient intent BEFORE agent replies, so we classify the right message
                intent_result = classify_recipient_intent(
                    incoming_message=incoming_body,
                    thread_status=str(thread_data.get("status", "active")),
                )

                # Stalled check: fire at turn 3 and turn 6 only
                if should_fire_stalled(turn_count, convo):
                    send_intent_notification(
                        user_email=email_address,
                        recipient_name=thread_data.get("recipient_name", ""),
                        recipient_email=from_email,
                        meeting_title=strip_reply_prefix(meta.get("subject", "")),
                        intent="stalled",
                    )

                guardrail_text = _guardrail_reply(convo)
                if guardrail_text:
                    sent = _send_reply(token, email_address, from_email, meta.get("subject", "Re:"),
                                       guardrail_text, thread_id, meta.get("message_id_header", ""), refresh_fn,
                                       sender_name=thread_data.get("sender_name", ""))
                    if sent:
                        replied += 1
                        updated = {"gmail_thread_id": thread_id, "status": "active", "state": "open",
                                   "turn_count": turn_count + 1, "last_message_id": msg_id,
                                   "last_message_snippet": _make_snippet(guardrail_text),
                                   "attendees": list(set((thread_data.get("attendees") or []) + [from_email]))}
                        updated["thread_status"] = compute_thread_status({**thread_data, **updated})
                        save_thread(thread_doc_id, updated)
                    else:
                        save_thread(thread_doc_id, {"last_message_snippet": incoming_snippet})
                    processed_items += 1
                    continue

                if turn_count >= 10:
                    closing_text = "It seems we're having trouble finding a time. Feel free to reply whenever you have a slot that works, and I'll get it booked right away."
                    sent = _send_reply(token, email_address, from_email, meta.get("subject", "Re:"),
                                       closing_text, thread_id, meta.get("message_id_header", ""), refresh_fn,
                                       sender_name=thread_data.get("sender_name", ""))
                    if sent:
                        replied += 1
                        save_thread(thread_doc_id, {"turn_count": turn_count + 1, "last_message_id": msg_id,
                                                     "last_message_snippet": _make_snippet(closing_text)})
                    else:
                        save_thread(thread_doc_id, {"last_message_snippet": incoming_snippet})
                    processed_items += 1
                    continue

                reply_text, finalized_payload = generate_scheduling_reply(
                    thread_messages=convo, access_token=token, provider="google",
                    sender_name=thread_data.get("sender_name", "Scheduler Team"),
                    context=thread_data.get("context", "Schedule a meeting via email."),
                    refresh_access_token=refresh_fn,
                    user_timezone=(thread_data.get("user_timezone") or "").strip() or None,
                    user_timezone_offset_minutes=thread_data.get("user_timezone_offset_minutes"),
                    turn_count=turn_count,
                    recipient_email=thread_data.get("recipient_email", ""),
                    recipient_name=thread_data.get("recipient_name", ""),
                )
                if not reply_text and not finalized_payload:
                    continue

                if finalized_payload and not _has_explicit_rejection(convo):
                    did_finalize, fallback_text = _handle_finalization(
                        token, email_address, from_email, msg_id, thread_id, thread_doc_id, thread_data,
                        finalized_payload, meta, convo, refresh_fn,
                    )
                    if did_finalize:
                        finalized += 1
                        replied += 1
                        processed_items += 1
                        continue
                    elif fallback_text:
                        reply_text = fallback_text

                sent = _send_reply(token, email_address, from_email, meta.get("subject", "Re:"),
                                   reply_text, thread_id, meta.get("message_id_header", ""), refresh_fn,
                                   sender_name=thread_data.get("sender_name", ""))
                if not sent:
                    save_thread(thread_doc_id, {"last_message_snippet": incoming_snippet})
                    continue

                # new_time_suggestion / reschedule_booked / cancelled — calendar_conflict fires in _handle_finalization, stalled fires above
                classified_intent = intent_result.get("intent")
                if classified_intent not in ("none", None):
                    send_intent_notification(
                        user_email=email_address,
                        recipient_name=thread_data.get("recipient_name", ""),
                        recipient_email=from_email,
                        meeting_title=strip_reply_prefix(meta.get("subject", "")),
                        intent=classified_intent,
                        proposed_day=intent_result.get("proposed_day", ""),
                        proposed_time=intent_result.get("proposed_time", ""),
                    )
                # Fire conflict notification if recipient mentioned a busy/conflict time
                if intent_result.get("conflict_day") or intent_result.get("conflict_time"):
                    send_intent_notification(
                        user_email=email_address,
                        recipient_name=thread_data.get("recipient_name", ""),
                        recipient_email=from_email,
                        meeting_title=strip_reply_prefix(meta.get("subject", "")),
                        intent="calendar_conflict",
                        conflict_day=intent_result.get("conflict_day", ""),
                        conflict_time=intent_result.get("conflict_time", ""),
                    )
                replied += 1
                updated = {"gmail_thread_id": thread_id, "status": "active", "state": "open",
                           "turn_count": turn_count + 1, "last_message_id": msg_id,
                           "last_message_snippet": _make_snippet(reply_text),
                           "attendees": list(set((thread_data.get("attendees") or []) + [from_email]))}
                if classified_intent == "cancelled":
                    updated["status"] = "cancelled"
                    updated["state"] = "cancelled"
                elif classified_intent == "reschedule_booked" and str(thread_data.get("status", "")).lower() == "completed":
                    # Keep status=completed so compute_thread_status can return reschedule_requested
                    updated["status"] = "completed"
                    updated["state"] = "completed"
                updated["thread_status"] = compute_thread_status({**thread_data, **updated}, intent=classified_intent or "")
                save_thread(thread_doc_id, updated)
                processed_items += 1
            except Exception as exc:
                log_event("gmail_push_message_process_failed", error=str(exc))
                continue
        if processed_items >= max_items:
            capped = True
            break

    if not capped:
        update_watch_state(provider="google", user_email=email_address, data={"history_id": new_history_id})
    else:
        log_event("gmail_push_history_not_advanced_due_to_cap", user_email=email_address, max_items=max_items)

    return {"ok": True, "data": {"replied_count": replied, "finalized_count": finalized}}
