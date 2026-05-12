"""
Google/Outlook watch renewal + push processing.

Uses the restructured Firestore layout under a2h-emailing/config:
  - watch_state subcollection (doc ID = provider::userEmail)
  - threads subcollection (doc ID = provider::userEmail::threadID)
"""

from __future__ import annotations

import base64
import json
import os
import re
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from email.utils import parseaddr
from typing import Any, Callable

import httpx
from google.auth.transport import requests as g_requests
from google.oauth2 import id_token as g_id_token

from app.agent_service import (
    create_calendar_event,
    find_conflicting_event,
    generate_initial_email,
    generate_scheduling_reply,
    normalize_finalized_event_times,
)
from app.logging_config import log_event
from app.repository import (
    create_thread,
    delete_thread,
    find_recent_active_thread_by_recipient,
    find_single_active_thread_for_user,
    find_thread_by_gmail_thread,
    mark_thread_message_processed,
    save_thread,
    update_watch_state,
)
from app.token_manager import TokenManager

def _format_event_time(iso_str: str) -> str:
    """Format an ISO 8601 datetime into a human-friendly string like 'Thursday, April 24 at 9:00 PM'."""
    if not iso_str:
        return "the agreed time"
    try:
        raw = iso_str.strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        return dt.strftime("%A, %B %d at %I:%M %p").replace(" 0", " ")
    except Exception:
        return iso_str


GMAIL_API_BASE = "https://gmail.googleapis.com/gmail/v1/users/me"
OUTLOOK_API_BASE = "https://graph.microsoft.com/v1.0"
MAX_AGENT_TURNS = int(os.getenv("MAX_AGENT_TURNS", "15"))


def _extract_subject_from_context(context: str) -> str:
    text = (context or "").strip()
    if not text:
        return "Meeting Scheduling"

    # Prefer explicit title signals from upstream prompts.
    quoted = re.search(r'titled\s+"([^"]+)"', text, flags=re.IGNORECASE)
    if quoted and quoted.group(1).strip():
        return quoted.group(1).strip()

    title_word = re.search(r"\btitle(?:d)?\s+([^\n\r,.!?]+)", text, flags=re.IGNORECASE)
    if title_word and title_word.group(1).strip():
        return title_word.group(1).strip().strip(" .,:;-")

    subject_like = re.search(r"subject\s*[:=-]\s*([^\n\r]+)", text, flags=re.IGNORECASE)
    if subject_like and subject_like.group(1).strip():
        return subject_like.group(1).strip().strip(" .,:;-")

    cleaned = re.sub(r"\s+", " ", text).strip(" .,:;-")
    if not cleaned:
        return "Meeting Scheduling"
    return " ".join(cleaned.split()[:8]).strip()


def _extract_human_detail_from_context(context: str) -> str:
    text = (context or "").strip()
    if not text:
        return ""

    # Remove robotic instruction style wrappers while retaining useful scheduling details.
    text = re.sub(r"^user requested to\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^please\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^initiate (an )?email flow with\s+[^ ]+\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip(" .")

    if len(text) < 10:
        return ""
    # Keep detail concise and human.
    return text[:220].strip()


def _request_gmail(
    token: str,
    method: str,
    path: str,
    refresh_access_token: Callable[[], str | None] | None = None,
    **kwargs,
) -> httpx.Response:
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"
    headers["Content-Type"] = "application/json"
    resp = httpx.request(
        method=method,
        url=f"{GMAIL_API_BASE}{path}",
        headers=headers,
        timeout=20.0,
        **kwargs,
    )
    if resp.status_code == 401 and refresh_access_token:
        try:
            refreshed = (refresh_access_token() or "").strip()
        except Exception:
            refreshed = ""
        if refreshed:
            headers["Authorization"] = f"Bearer {refreshed}"
            resp = httpx.request(
                method=method,
                url=f"{GMAIL_API_BASE}{path}",
                headers=headers,
                timeout=20.0,
                **kwargs,
            )
    return resp


def _request_outlook(
    token: str,
    method: str,
    path: str,
    refresh_access_token: Callable[[], str | None] | None = None,
    **kwargs,
) -> httpx.Response:
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"
    headers["Content-Type"] = "application/json"
    resp = httpx.request(
        method=method,
        url=f"{OUTLOOK_API_BASE}{path}",
        headers=headers,
        timeout=20.0,
        **kwargs,
    )
    if resp.status_code == 401 and refresh_access_token:
        try:
            refreshed = (refresh_access_token() or "").strip()
        except Exception:
            refreshed = ""
        if refreshed:
            headers["Authorization"] = f"Bearer {refreshed}"
            resp = httpx.request(
                method=method,
                url=f"{OUTLOOK_API_BASE}{path}",
                headers=headers,
                timeout=20.0,
                **kwargs,
            )
    return resp


def verify_pubsub_auth_header(auth_header: str | None, expected_audience: str) -> dict[str, Any]:
    """
    Verify Pub/Sub push OIDC bearer token.
    """
    if not auth_header or not auth_header.lower().startswith("bearer "):
        return {"ok": False, "error": "missing_bearer_token"}
    raw_token = auth_header.split(" ", 1)[1].strip()
    if not raw_token:
        return {"ok": False, "error": "empty_bearer_token"}

    try:
        req = g_requests.Request()
        claims = g_id_token.verify_oauth2_token(raw_token, req, audience=expected_audience)
        email = claims.get("email")
        return {"ok": True, "data": {"email": email, "claims": claims}}
    except Exception as exc:
        return {"ok": False, "error": f"invalid_pubsub_auth:{exc}"}


def renew_gmail_watch(user_email: str, token_manager: TokenManager) -> dict[str, Any]:
    """
    Renew Gmail watch for one user.

    Requires env var:
      GMAIL_PUBSUB_TOPIC=projects/<project-id>/topics/<topic-name>
    """
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

    resp = _request_gmail(token, "POST", "/watch", json=body)
    if resp.status_code != 200:
        log_event(
            "gmail_watch_renew_failed",
            user_email=user_email,
            status_code=resp.status_code,
            response=resp.text[:500],
        )
        return {"ok": False, "error": f"watch renew failed: {resp.status_code}"}

    payload = resp.json()
    expiration_ms = int(payload.get("expiration", "0"))
    watch_expiration = datetime.fromtimestamp(expiration_ms / 1000, tz=timezone.utc)

    renew_at = watch_expiration - timedelta(days=1)
    history_id = str(payload.get("historyId", ""))

    save_result = update_watch_state(
        provider="google",
        user_email=user_email,
        data={
            "history_id": history_id,
            "watch_expiration": watch_expiration,
            "expires_at": renew_at,
        },
    )
    if not save_result.get("ok"):
        return {"ok": False, "error": save_result.get("error", "watch_state_update_failed")}

    log_event(
        "gmail_watch_renewed",
        user_email=user_email,
        history_id=history_id,
        watch_expiration=watch_expiration.isoformat(),
        renew_at=renew_at.isoformat(),
    )
    return {"ok": True, "data": {"user_email": user_email, "history_id": history_id}}


def renew_outlook_watch(user_email: str, token_manager: TokenManager) -> dict[str, Any]:
    from app.repository import get_watch_state

    token = token_manager.get_fresh_token(user_email=user_email, provider="outlook")
    if not token:
        return {"ok": False, "error": "No valid Outlook token"}

    watch_result = get_watch_state(provider="outlook", user_email=user_email)
    state = watch_result.get("data") if watch_result.get("ok") else {}
    subscription_id = str((state or {}).get("subscription_id", "")).strip()
    if not subscription_id:
        return _ensure_outlook_subscription_with_access_token(token=token, user_email=user_email)

    expires = (datetime.now(timezone.utc) + timedelta(days=2)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    patch_resp = _request_outlook(
        token,
        "PATCH",
        f"/subscriptions/{subscription_id}",
        json={"expirationDateTime": expires},
    )
    if patch_resp.status_code == 404:
        return _ensure_outlook_subscription_with_access_token(token=token, user_email=user_email)
    if patch_resp.status_code not in (200, 202):
        return {"ok": False, "error": f"outlook_subscription_renew_failed:{patch_resp.status_code}"}

    update_watch_state(
        provider="outlook",
        user_email=user_email,
        data={
            "watch_expiration": datetime.fromisoformat(expires.replace("Z", "+00:00")),
            "expires_at": datetime.fromisoformat(expires.replace("Z", "+00:00")) - timedelta(hours=6),
        },
    )
    return {"ok": True, "data": {"user_email": user_email, "subscription_id": subscription_id}}


def _extract_notification_data(push_payload: dict[str, Any]) -> dict[str, str] | None:
    message = push_payload.get("message") or {}
    data_b64 = message.get("data")
    if not data_b64:
        return None
    try:
        decoded = base64.b64decode(data_b64).decode("utf-8")
        data = json.loads(decoded)
        return {
            "emailAddress": data.get("emailAddress", ""),
            "historyId": str(data.get("historyId", "")),
        }
    except Exception as exc:
        log_event("gmail_push_decode_failed", error=str(exc))
        return None


def _send_noted_reply(token: str, user_email: str, thread_id: str, message_id: str) -> bool:
    meta_resp = _request_gmail(
        token,
        "GET",
        f"/messages/{message_id}",
        params={"format": "metadata", "metadataHeaders": ["From", "Subject", "Message-ID"]},
    )
    if meta_resp.status_code != 200:
        log_event("gmail_message_fetch_failed", status_code=meta_resp.status_code)
        return False

    payload = meta_resp.json().get("payload", {})
    headers = {h.get("name", ""): h.get("value", "") for h in payload.get("headers", [])}

    from_header = headers.get("From", "")
    from_email = parseaddr(from_header)[1].lower()
    if from_email == user_email.lower():
        return False

    subject = headers.get("Subject", "").strip() or "Re:"
    if not subject.lower().startswith("re:"):
        subject = f"Re: {subject}"
    parent_message_id = headers.get("Message-ID", "")

    raw_message = (
        f"To: {from_email}\r\n"
        f"Subject: {subject}\r\n"
        f'Content-Type: text/plain; charset="UTF-8"\r\n'
        f"In-Reply-To: {parent_message_id}\r\n"
        f"References: {parent_message_id}\r\n"
        "\r\n"
        "Noted"
    )
    raw_b64 = base64.urlsafe_b64encode(raw_message.encode("utf-8")).decode("utf-8")

    send_resp = _request_gmail(
        token,
        "POST",
        "/messages/send",
        json={"raw": raw_b64, "threadId": thread_id},
    )
    if send_resp.status_code not in (200, 202):
        log_event(
            "gmail_noted_reply_failed",
            status_code=send_resp.status_code,
            response=send_resp.text[:500],
        )
        return False

    log_event("email_sent", action="noted_reply", thread_id=thread_id, to=from_email)
    return True


def _get_message_metadata(
    token: str,
    message_id: str,
    refresh_access_token: Callable[[], str | None] | None = None,
) -> dict[str, str] | None:
    resp = _request_gmail(
        token,
        "GET",
        f"/messages/{message_id}",
        refresh_access_token=refresh_access_token,
        params={"format": "metadata", "metadataHeaders": ["From", "Subject", "Message-ID"]},
    )
    if resp.status_code != 200:
        log_event("gmail_message_fetch_failed", message_id=message_id, status_code=resp.status_code)
        return None
    payload = resp.json().get("payload", {})
    headers = {h.get("name", ""): h.get("value", "") for h in payload.get("headers", [])}
    return {
        "from": headers.get("From", ""),
        "subject": headers.get("Subject", ""),
        "message_id_header": headers.get("Message-ID", ""),
    }


def _extract_text_body(payload: dict) -> str:
    """Walk a Gmail MIME payload tree and return the text/plain body."""
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


def _get_thread_messages_for_agent(
    token: str,
    thread_id: str,
    refresh_access_token: Callable[[], str | None] | None = None,
) -> list[str]:
    resp = _request_gmail(
        token,
        "GET",
        f"/threads/{thread_id}",
        refresh_access_token=refresh_access_token,
        params={"format": "full"},
    )
    if resp.status_code != 200:
        log_event("gmail_thread_fetch_failed", thread_id=thread_id, status_code=resp.status_code)
        return []
    thread = resp.json()
    messages = []
    for m in thread.get("messages", []):
        payload = m.get("payload", {})
        headers = {h.get("name", ""): h.get("value", "") for h in payload.get("headers", [])}
        sender = headers.get("From", "Unknown")

        body = _extract_text_body(payload).strip()
        if not body:
            body = (m.get("snippet") or "").strip()
        if body:
            messages.append(f"From: {sender}\n{body[:800]}")
    return messages


def _has_explicit_rejection(thread_messages: list[str]) -> bool:
    """
    Lightweight safety veto: block finalization only when the latest inbound
    message contains a clear cancellation or rejection signal.
    """
    if not thread_messages:
        return False
    latest = (thread_messages[-1] or "").lower()
    return bool(
        re.search(
            r"\b(cancel|never mind|don'?t book|wrong time|actually no|not that time|"
            r"hold off|stop|don'?t schedule|changed my mind|reschedule|pick another)\b",
            latest,
        )
    )


def _latest_message_allows_conflict(thread_messages: list[str]) -> bool:
    if not thread_messages:
        return False
    latest = (thread_messages[-1] or "").lower()
    return bool(
        re.search(
            r"\b(book it anyway|schedule anyway|still book|still schedule|conflict is fine|"
            r"i am okay with conflict|go ahead anyway|go ahead|do it|book it|yes|sure|"
            r"that('?s| is) fine|okay|ok|no problem|that works|works for me|"
            r"you can do it|please do|proceed|confirm it|lock it in)\b",
            latest,
        )
    )


def _latest_inbound_text(thread_messages: list[str]) -> str:
    if not thread_messages:
        return ""
    latest = (thread_messages[-1] or "")
    parts = latest.split("\n", 1)
    if len(parts) == 2:
        return parts[1].strip()
    return latest.strip()


def _is_offtopic_email_request(text: str) -> bool:
    t = (text or "").lower()
    if not t:
        return False
    return bool(
        re.search(
            r"\b(trump|epstein|politics|crypto price|stock tips|weather|sports score|movie|music|news)\b",
            t,
        )
    )


def _is_sensitive_calendar_data_request(text: str) -> bool:
    t = (text or "").lower()
    if not t:
        return False
    return bool(
        re.search(
            r"\b(what is your calendar like|who is in your meeting|who all|last time you met|last meeting with|all bookings|share your calendar|meeting with)\b",
            t,
        )
    )


def _guardrail_override_reply(thread_messages: list[str]) -> str | None:
    latest_text = _latest_inbound_text(thread_messages)
    if _is_offtopic_email_request(latest_text):
        return (
            "I can help with meeting scheduling in this thread. "
            "Please share a time window that works for you and I will coordinate it."
        )
    if _is_sensitive_calendar_data_request(latest_text):
        return (
            "I can only share high-level availability for scheduling and cannot share private meeting details. "
            "If helpful, I can propose open time slots for our meeting."
        )
    return None


def _send_agent_reply(
    token: str,
    user_email: str,
    to_email: str,
    subject: str,
    body: str,
    thread_id: str,
    parent_message_id: str,
    refresh_access_token: Callable[[], str | None] | None = None,
) -> bool:
    subj = subject.strip() or "Re:"
    if not subj.lower().startswith("re:"):
        subj = f"Re: {subj}"
    raw_message = (
        f"To: {to_email}\r\n"
        f"Subject: {subj}\r\n"
        f'Content-Type: text/plain; charset="UTF-8"\r\n'
        f"In-Reply-To: {parent_message_id}\r\n"
        f"References: {parent_message_id}\r\n"
        "\r\n"
        f"{body}"
    )
    raw_b64 = base64.urlsafe_b64encode(raw_message.encode("utf-8")).decode("utf-8")
    send_resp = _request_gmail(
        token,
        "POST",
        "/messages/send",
        refresh_access_token=refresh_access_token,
        json={"raw": raw_b64, "threadId": thread_id},
    )
    if send_resp.status_code not in (200, 202):
        log_event(
            "agent_reply_send_failed",
            user_email=user_email,
            status_code=send_resp.status_code,
            response=send_resp.text[:500],
        )
        return False
    log_event("email_sent", action="agent_reply", thread_id=thread_id, to=to_email)
    return True


def _send_initial_thread_email(
    token: str,
    to_email: str,
    sender_name: str,
    recipient_name: str | None,
    context: str,
    custom_subject: str | None = None,
    custom_body: str | None = None,
) -> tuple[str | None, str | None]:
    """
    Send the first email in a scheduling flow and return (thread_id, message_id).
    Uses custom subject/body if provided, otherwise generates via LLM.
    """
    context_text = (context or "").strip()
    
    # Use custom subject/body if provided (from frontend edits), otherwise generate
    if custom_subject and custom_body:
        subject = custom_subject.strip()
        body = custom_body.strip()
    else:
        try:
            subject, body = generate_initial_email(
                sender_name=sender_name,
                recipient_name=(recipient_name or "").strip() or "there",
                context=context_text or "schedule a meeting",
            )
        except Exception as exc:
            log_event("initial_email_llm_failed", error=str(exc), to=to_email)
            return None, None

    msg = MIMEText(body)
    msg["To"] = to_email
    msg["Subject"] = subject
    raw_b64 = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")

    send_resp = _request_gmail(token, "POST", "/messages/send", json={"raw": raw_b64})
    if send_resp.status_code not in (200, 202):
        log_event(
            "gmail_initial_email_send_failed",
            to=to_email,
            status_code=send_resp.status_code,
            response=send_resp.text[:500],
        )
        return None, None

    data = send_resp.json() if send_resp.content else {}
    thread_id = data.get("threadId")
    message_id = data.get("id")
    if not thread_id:
        log_event("gmail_initial_email_thread_missing", to=to_email)
        return None, None

    log_event("email_sent", action="initiate_flow", thread_id=thread_id, to=to_email)
    return thread_id, message_id


def _ensure_watch_with_access_token(token: str, user_email: str) -> dict[str, Any]:
    """
    Ensure Gmail watch is active using the provided access token.
    """
    topic = os.getenv("GMAIL_PUBSUB_TOPIC")
    if not topic:
        return {"ok": False, "error": "GMAIL_PUBSUB_TOPIC is not configured"}

    body: dict[str, Any] = {"topicName": topic}
    label_ids = os.getenv("GMAIL_WATCH_LABEL_IDS")
    if label_ids:
        body["labelIds"] = [x.strip() for x in label_ids.split(",") if x.strip()]
        body["labelFilterAction"] = "include"

    resp = _request_gmail(token, "POST", "/watch", json=body)
    if resp.status_code != 200:
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

    return {
        "ok": True,
        "data": {
            "history_id": history_id,
            "watch_expiration": watch_expiration.isoformat() if watch_expiration else None,
            "renew_at": renew_at.isoformat() if renew_at else None,
        },
    }


def initiate_google_email_flow(
    google_access_token: str,
    sender_name: str,
    recipient_email: str,
    recipient_name: str | None,
    context: str,
    email_subject: str | None = None,
    email_body: str | None = None,
    user_timezone: str | None = None,
    user_timezone_offset_minutes: int | None = None,
    token_manager: TokenManager | None = None,
) -> dict[str, Any]:
    """
    Start a Gmail scheduling thread, persist mapping in Firestore, and activate watch.
    """
    token = (google_access_token or "").strip()
    if not token:
        return {"ok": False, "error": "google_access_token is required", "status_code": 400}
    if not recipient_email or not sender_name:
        return {"ok": False, "error": "sender_name and recipient_email are required", "status_code": 400}

    profile_resp = _request_gmail(token, "GET", "/profile")
    if profile_resp.status_code != 200:
        return {"ok": False, "error": "failed_to_fetch_gmail_profile", "status_code": 401}
    user_email = (profile_resp.json().get("emailAddress") or "").lower().strip()
    if not user_email:
        return {"ok": False, "error": "gmail_profile_missing_email", "status_code": 401}

    # Store token for immediate follow-up. Prefer existing real refresh token (from
    # onboarding) so tokens don't expire daily. Only mirror access_token as fallback.
    if token_manager is not None:
        try:
            existing_refresh = token_manager.get_existing_refresh_token(user_email, "google")
            refresh_to_store = (
                existing_refresh
                if existing_refresh and not existing_refresh.strip().startswith("ya29.")
                else token
            )
            stored = token_manager.store_tokens(
                user_email=user_email,
                provider="google",
                access_token=token,
                refresh_token=refresh_to_store,
                expires_in_seconds=3300,
            )
            if not stored:
                log_event("initiate_token_store_failed", user_email=user_email)
        except Exception as exc:
            log_event("initiate_token_store_exception", user_email=user_email, error=str(exc))

    thread_id, _message_id = _send_initial_thread_email(
        token=token,
        to_email=recipient_email,
        sender_name=sender_name,
        recipient_name=recipient_name,
        context=context or "",
        custom_subject=email_subject,
        custom_body=email_body,
    )
    if not thread_id:
        return {"ok": False, "error": "failed_to_send_initial_email", "status_code": 502}

    created = create_thread(
        provider="google",
        user_email=user_email,
        data={
            "gmail_thread_id": thread_id,
            "status": "active",
            "state": "open",
            "turn_count": 0,
            "context": context or "",
            "sender_name": sender_name,
            "recipient_email": recipient_email.lower(),
            "recipient_name": (recipient_name or "").strip(),
            "user_timezone": (user_timezone or "").strip(),
            "user_timezone_offset_minutes": user_timezone_offset_minutes,
            "attendees": [recipient_email.lower()],
            "source": "node_execute_initiate_email_flow",
            "initiated_at": datetime.now(timezone.utc),
        },
    )
    if not created.get("ok"):
        return {"ok": False, "error": created.get("error", "thread_create_failed"), "status_code": 500}

    enable_watch_setup = os.getenv("ENABLE_INITIATE_WATCH_SETUP", "true").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    require_watch_setup = os.getenv("REQUIRE_WATCH_SETUP_ON_INITIATE", "true").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    watch_result: dict[str, Any] = {"ok": True, "skipped": not enable_watch_setup}
    if enable_watch_setup:
        watch_result = _ensure_watch_with_access_token(token=token, user_email=user_email)
        if not watch_result.get("ok"):
            event_name = "gmail_watch_setup_failed" if require_watch_setup else "gmail_watch_setup_warning"
            log_event(
                event_name,
                user_email=user_email,
                thread_id=thread_id,
                error=watch_result.get("error"),
            )
            if require_watch_setup:
                return {
                    "ok": False,
                    "error": watch_result.get("error", "gmail_watch_setup_failed"),
                    "status_code": 502,
                }
    else:
        log_event(
            "gmail_watch_setup_skipped_on_initiate",
            user_email=user_email,
            thread_id=thread_id,
        )
        if require_watch_setup:
            return {
                "ok": False,
                "error": "gmail_watch_setup_required_but_disabled",
                "status_code": 500,
            }

    log_event(
        "email_flow_initiated",
        thread_id=thread_id,
        user_email=user_email,
        recipient_email=recipient_email.lower(),
    )
    return {
        "ok": True,
        "data": {
            "status": "ok",
            "thread_id": thread_id,
            "watch_ok": bool(watch_result.get("ok")),
            "watch_error": None if watch_result.get("ok") else watch_result.get("error"),
        },
    }


def _send_initial_outlook_thread_email(
    token: str,
    to_email: str,
    sender_name: str,
    recipient_name: str | None,
    context: str,
    custom_subject: str | None = None,
    custom_body: str | None = None,
) -> dict[str, Any]:
    """
    Send the first email in an Outlook scheduling flow.
    Uses custom subject/body if provided, otherwise generates via LLM.
    """
    context_text = (context or "").strip()
    
    # Use custom subject/body if provided (from frontend edits), otherwise generate
    if custom_subject and custom_body:
        subject = custom_subject.strip()
        body = custom_body.strip()
    else:
        try:
            subject, body = generate_initial_email(
                sender_name=sender_name,
                recipient_name=(recipient_name or "").strip() or "there",
                context=context_text or "schedule a meeting",
            )
        except Exception as exc:
            log_event("initial_email_llm_failed", error=str(exc), to=to_email)
            return {"ok": False, "error": f"initial_email_llm_failed:{exc}", "status_code": 502}

    draft_resp = _request_outlook(
        token,
        "POST",
        "/me/messages",
        json={
            "subject": subject,
            "body": {"contentType": "Text", "content": body},
            "toRecipients": [{"emailAddress": {"address": to_email}}],
        },
    )
    if draft_resp.status_code not in (200, 201):
        err_text = draft_resp.text[:1000]
        try:
            payload = draft_resp.json()
            graph_msg = (
                payload.get("error", {}).get("message")
                if isinstance(payload, dict)
                else None
            )
            if graph_msg:
                err_text = str(graph_msg)
        except Exception:
            pass
        log_event(
            "outlook_initial_email_draft_failed",
            to=to_email,
            status_code=draft_resp.status_code,
            response=err_text[:500],
        )
        return {
            "ok": False,
            "status_code": draft_resp.status_code,
            "error": f"outlook_message_create_failed:{err_text}",
        }

    draft = draft_resp.json()
    message_id = draft.get("id")
    conversation_id = draft.get("conversationId")
    if not message_id:
        return {"ok": False, "status_code": 502, "error": "outlook_message_create_missing_id"}

    send_resp = _request_outlook(token, "POST", f"/me/messages/{message_id}/send")
    if send_resp.status_code not in (200, 202):
        err_text = send_resp.text[:1000]
        try:
            payload = send_resp.json()
            graph_msg = (
                payload.get("error", {}).get("message")
                if isinstance(payload, dict)
                else None
            )
            if graph_msg:
                err_text = str(graph_msg)
        except Exception:
            pass
        log_event(
            "outlook_initial_email_send_failed",
            to=to_email,
            status_code=send_resp.status_code,
            response=err_text[:500],
        )
        return {
            "ok": False,
            "status_code": send_resp.status_code,
            "error": f"outlook_message_send_failed:{err_text}",
        }

    thread_id = conversation_id or message_id
    log_event("email_sent", action="initiate_flow_outlook", thread_id=thread_id, to=to_email)
    return {"ok": True, "thread_id": thread_id, "message_id": message_id}


def _ensure_outlook_subscription_with_access_token(token: str, user_email: str) -> dict[str, Any]:
    webhook_url = (os.getenv("OUTLOOK_WEBHOOK_URL") or "").strip()
    if not webhook_url:
        return {"ok": False, "error": "OUTLOOK_WEBHOOK_URL is not configured"}

    expires = (datetime.now(timezone.utc) + timedelta(days=2)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    body = {
        "changeType": "created",
        "notificationUrl": webhook_url,
        "resource": "/me/messages",
        "expirationDateTime": expires,
        "clientState": f"kalendra::{user_email.lower()}",
    }
    resp = _request_outlook(token, "POST", "/subscriptions", json=body)
    if resp.status_code not in (200, 201):
        return {"ok": False, "error": f"subscription_failed:{resp.status_code}", "response": resp.text[:500]}

    payload = resp.json()
    subscription_id = payload.get("id")
    expiration = payload.get("expirationDateTime")
    expires_at = None
    if expiration:
        try:
            expires_at = datetime.fromisoformat(expiration.replace("Z", "+00:00"))
        except Exception:
            expires_at = None

    save_data: dict[str, Any] = {"subscription_id": subscription_id}
    if expires_at:
        save_data["watch_expiration"] = expires_at
        save_data["expires_at"] = expires_at - timedelta(hours=6)
    update_watch_state(provider="outlook", user_email=user_email, data=save_data)
    return {"ok": True, "data": {"subscription_id": subscription_id, "watch_expiration": expiration}}


def initiate_outlook_email_flow(
    outlook_access_token: str,
    sender_name: str,
    recipient_email: str,
    recipient_name: str | None,
    context: str,
    email_subject: str | None = None,
    email_body: str | None = None,
    user_timezone: str | None = None,
    user_timezone_offset_minutes: int | None = None,
    token_manager: TokenManager | None = None,
) -> dict[str, Any]:
    token = (outlook_access_token or "").strip()
    if not token:
        return {"ok": False, "error": "outlook_access_token is required", "status_code": 400}
    if not recipient_email or not sender_name:
        return {"ok": False, "error": "sender_name and recipient_email are required", "status_code": 400}

    profile_resp = _request_outlook(token, "GET", "/me?$select=mail,userPrincipalName")
    if profile_resp.status_code != 200:
        return {"ok": False, "error": "failed_to_fetch_outlook_profile", "status_code": 401}
    p = profile_resp.json()
    user_email = (p.get("mail") or p.get("userPrincipalName") or "").lower().strip()
    if not user_email:
        return {"ok": False, "error": "outlook_profile_missing_email", "status_code": 401}

    if token_manager is not None:
        # Preserve existing real refresh token if available.
        # Do not overwrite it with short-lived access token on each initiate call.
        existing_refresh = token_manager.get_existing_refresh_token(user_email, "outlook")
        refresh_to_store = existing_refresh or token
        token_manager.store_tokens(
            user_email=user_email,
            provider="outlook",
            access_token=token,
            refresh_token=refresh_to_store,
            expires_in_seconds=3300,
        )

    send_result = _send_initial_outlook_thread_email(
        token=token,
        to_email=recipient_email,
        sender_name=sender_name,
        recipient_name=recipient_name,
        context=context or "",
        custom_subject=email_subject,
        custom_body=email_body,
    )
    if not send_result.get("ok"):
        return {
            "ok": False,
            "error": send_result.get("error", "failed_to_send_initial_outlook_email"),
            "status_code": int(send_result.get("status_code") or 502),
        }
    thread_id = str(send_result.get("thread_id") or "").strip()
    if not thread_id:
        return {"ok": False, "error": "failed_to_resolve_outlook_thread_id", "status_code": 502}

    created = create_thread(
        provider="outlook",
        user_email=user_email,
        data={
            "gmail_thread_id": thread_id,
            "status": "active",
            "state": "open",
            "turn_count": 0,
            "context": context or "",
            "sender_name": sender_name,
            "recipient_email": recipient_email.lower(),
            "recipient_name": (recipient_name or "").strip(),
            "user_timezone": (user_timezone or "").strip(),
            "user_timezone_offset_minutes": user_timezone_offset_minutes,
            "attendees": [recipient_email.lower()],
            "source": "node_execute_initiate_email_flow",
            "initiated_at": datetime.now(timezone.utc),
        },
    )
    if not created.get("ok"):
        return {"ok": False, "error": created.get("error", "thread_create_failed"), "status_code": 500}

    watch_result = _ensure_outlook_subscription_with_access_token(token=token, user_email=user_email)
    if not watch_result.get("ok"):
        return {
            "ok": False,
            "error": watch_result.get("error", "outlook_watch_setup_failed"),
            "status_code": 502,
        }
    return {
        "ok": True,
        "data": {
            "status": "ok",
            "thread_id": thread_id,
            "watch_ok": bool(watch_result.get("ok")),
            "watch_error": None if watch_result.get("ok") else watch_result.get("error"),
        },
    }


def process_outlook_push(push_payload: dict[str, Any], token_manager: TokenManager) -> dict[str, Any]:
    notifications = push_payload.get("value") or []
    replied = 0
    finalized = 0
    for n in notifications:
        client_state = str(n.get("clientState", ""))
        if not client_state.startswith("kalendra::"):
            continue
        user_email = client_state.split("::", 1)[1].strip().lower()
        if not user_email:
            continue

        resource_data = n.get("resourceData") or {}
        message_id = resource_data.get("id")
        if not message_id:
            resource = str(n.get("resource", ""))
            if "/messages/" in resource:
                message_id = resource.rsplit("/messages/", 1)[-1].split("?")[0]
        if not message_id:
            continue

        token = token_manager.get_fresh_token(user_email, "outlook")
        if not token:
            continue

        msg_resp = _request_outlook(
            token,
            "GET",
            f"/me/messages/{message_id}?$select=id,conversationId,subject,from,bodyPreview,internetMessageId",
            refresh_access_token=lambda: token_manager.get_fresh_token(user_email, "outlook"),
        )
        if msg_resp.status_code != 200:
            continue
        msg = msg_resp.json()
        thread_id = msg.get("conversationId") or msg.get("id")
        from_email = (msg.get("from", {}).get("emailAddress", {}).get("address") or "").lower()
        if not from_email or from_email == user_email:
            continue

        lookup = find_thread_by_gmail_thread("outlook", user_email, thread_id)
        if not lookup.get("ok"):
            continue
        thread_doc_id = lookup["data"]["thread_doc_id"]
        thread_data = lookup["data"]

        mark_res = mark_thread_message_processed(thread_doc_id, message_id)
        if not mark_res.get("ok") or mark_res["data"].get("already_processed"):
            continue

        turn_count = int(thread_data.get("turn_count", 0))
        if turn_count >= MAX_AGENT_TURNS:
            continue

        # Avoid brittle Graph conversation filters by keeping a lightweight
        # per-thread transcript in Firestore for Outlook threads.
        snippet = (msg.get("bodyPreview") or "").strip()
        convo = list(thread_data.get("conversation_messages") or [])
        if snippet:
            convo.append(f"From: {from_email}\n{snippet}")
        # Keep context bounded for token/cost safety.
        convo = convo[-30:]
        if not convo:
            continue

        guardrail_text = _guardrail_override_reply(convo)
        if guardrail_text:
            reply_resp = _request_outlook(
                token, "POST", f"/me/messages/{message_id}/reply",
                refresh_access_token=lambda: token_manager.get_fresh_token(user_email, "outlook"),
                json={"comment": guardrail_text},
            )
            if reply_resp.status_code in (200, 202):
                replied += 1
                save_thread(thread_doc_id, {
                    "gmail_thread_id": thread_id, "status": "active", "state": "open",
                    "turn_count": turn_count + 1, "last_message_id": message_id,
                    "attendees": list(set((thread_data.get("attendees") or []) + [from_email])),
                    "conversation_messages": (convo + [f"From: {user_email}\n{guardrail_text}"])[-30:],
                })
            continue

        if turn_count >= 10:
            closing_text = (
                "It seems we're having trouble finding a time. Feel free to reply "
                "whenever you have a slot that works, and I'll get it booked right away."
            )
            reply_resp = _request_outlook(
                token, "POST", f"/me/messages/{message_id}/reply",
                refresh_access_token=lambda: token_manager.get_fresh_token(user_email, "outlook"),
                json={"comment": closing_text},
            )
            if reply_resp.status_code in (200, 202):
                replied += 1
                save_thread(thread_doc_id, {
                    "turn_count": turn_count + 1, "last_message_id": message_id,
                    "conversation_messages": (convo + [f"From: {user_email}\n{closing_text}"])[-30:],
                })
            continue

        reply_text, finalized_payload = generate_scheduling_reply(
            thread_messages=convo,
            access_token=token,
            provider="outlook",
            sender_name=thread_data.get("sender_name", "Scheduler Team"),
            context=thread_data.get("context", "Schedule a meeting via email."),
            refresh_access_token=lambda: token_manager.get_fresh_token(user_email, "outlook"),
            user_timezone=(thread_data.get("user_timezone") or "").strip() or None,
            user_timezone_offset_minutes=thread_data.get("user_timezone_offset_minutes"),
            turn_count=turn_count,
        )
        # Empty reply_text is expected when finalize_meeting tool was called --
        # the finalization block below handles event creation and sends confirmation.
        if not reply_text and not finalized_payload:
            continue

        if finalized_payload and not _has_explicit_rejection(convo):
            finalized_payload = normalize_finalized_event_times(
                finalized_payload,
                user_timezone=(thread_data.get("user_timezone") or "").strip() or None,
                user_timezone_offset_minutes=thread_data.get("user_timezone_offset_minutes"),
            )
            conflict = find_conflicting_event(
                access_token=token,
                event_data=finalized_payload,
                provider="outlook",
                refresh_access_token=lambda: token_manager.get_fresh_token(user_email, "outlook"),
                user_timezone=(thread_data.get("user_timezone") or "").strip() or None,
                user_timezone_offset_minutes=thread_data.get("user_timezone_offset_minutes"),
            )
            if conflict and not _latest_message_allows_conflict(convo):
                raw_start = conflict.get("startLocal") or conflict.get("start") or ""
                conflict_start = _format_event_time(raw_start) if raw_start else "that time"
                reply_text = (
                    f"I have a conflict at {conflict_start}. "
                    "Should I book this anyway, or would you prefer a different slot?"
                )
                finalized_payload = None
            else:
                existing_attendees = finalized_payload.get("attendees") or []
                normalized = {
                    str(a).strip().lower()
                    for a in existing_attendees
                    if isinstance(a, str) and str(a).strip()
                }
                normalized.update({user_email.lower(), from_email.lower()})
                finalized_payload["attendees"] = sorted(normalized)
                create_result = create_calendar_event(
                    token, finalized_payload, provider="outlook",
                    refresh_access_token=lambda: token_manager.get_fresh_token(user_email, "outlook"),
                )
                if create_result.get("ok"):
                    friendly_time = _format_event_time(finalized_payload.get("startTime", ""))
                    confirm_text = (
                        f"All set! We're confirmed for {friendly_time}. "
                        "A calendar invite is on its way to you."
                    )
                    _request_outlook(
                        token, "POST", f"/me/messages/{message_id}/reply",
                        refresh_access_token=lambda: token_manager.get_fresh_token(user_email, "outlook"),
                        json={"comment": confirm_text},
                    )
                    delete_thread(thread_doc_id)
                    finalized += 1
                    replied += 1
                    log_event("thread_finalized", thread_doc_id=thread_doc_id, provider="outlook")
                    continue
                else:
                    log_event("thread_finalize_create_event_failed", thread_doc_id=thread_doc_id, provider="outlook", error=create_result.get("error"))
                    reply_text = (
                        "I tried to book the meeting but ran into a technical issue. "
                        "Let me try again shortly — no action needed on your end."
                    )
                    finalized_payload = None

        reply_resp = _request_outlook(
            token, "POST", f"/me/messages/{message_id}/reply",
            refresh_access_token=lambda: token_manager.get_fresh_token(user_email, "outlook"),
            json={"comment": reply_text},
        )
        if reply_resp.status_code not in (200, 202):
            continue
        replied += 1

        save_thread(thread_doc_id, {
            "gmail_thread_id": thread_id, "status": "active", "state": "open",
            "turn_count": turn_count + 1, "last_message_id": message_id,
            "attendees": list(set((thread_data.get("attendees") or []) + [from_email])),
            "conversation_messages": (convo + [f"From: {user_email}\n{reply_text}"])[-30:],
        })

    return {"ok": True, "data": {"replied_count": replied, "finalized_count": finalized}}


def process_gmail_push(push_payload: dict[str, Any], token_manager: TokenManager) -> dict[str, Any]:
    """
    Process Gmail Pub/Sub push and run agentic scheduling replies.
    """
    from app.repository import get_watch_state

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
        update_watch_state(
            provider="google",
            user_email=email_address,
            data={"history_id": new_history_id},
        )
        return {"ok": True, "message": "history baseline set"}

    token = token_manager.get_fresh_token(email_address, "google")
    if not token:
        return {"ok": False, "error": "google_token_unavailable"}

    history_resp = _request_gmail(
        token,
        "GET",
        "/history",
        refresh_access_token=lambda: token_manager.get_fresh_token(email_address, "google"),
        params={"startHistoryId": old_history_id, "historyTypes": "messageAdded"},
    )
    if history_resp.status_code != 200:
        if history_resp.status_code == 404:
            update_watch_state(
                provider="google",
                user_email=email_address,
                data={"history_id": new_history_id},
            )
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
                    log_event(
                        "gmail_push_processing_capped",
                        user_email=email_address,
                        max_items=max_items,
                    )
                    capped = True
                    break

                msg = added.get("message", {})
                msg_id = msg.get("id")
                thread_id = msg.get("threadId")
                if not msg_id or not thread_id:
                    continue

                meta = _get_message_metadata(
                    token,
                    msg_id,
                    refresh_access_token=lambda: token_manager.get_fresh_token(email_address, "google"),
                )
                if not meta:
                    continue
                from_email = parseaddr(meta["from"])[1].lower()
                if not from_email or from_email == email_address.lower():
                    continue

                # Fast reject: only process threads explicitly tracked in Firestore.
                thread_lookup = find_thread_by_gmail_thread("google", email_address, thread_id)
                if thread_lookup.get("ok"):
                    thread_doc_id = thread_lookup["data"]["thread_doc_id"]
                    thread_data = thread_lookup["data"]
                else:
                    # Recovery path: if exactly one active thread exists for this
                    # recipient, rebind it to the new Gmail thread id and continue.
                    recovered = find_recent_active_thread_by_recipient(
                        provider="google",
                        user_email=email_address,
                        recipient_email=from_email,
                    )
                    if recovered.get("ok"):
                        thread_doc_id = recovered["data"]["thread_doc_id"]
                        thread_data = recovered["data"]
                        save_thread(
                            thread_doc_id,
                            {
                                "provider": "google",
                                "user_email": email_address.lower(),
                                "gmail_thread_id": thread_id,
                            },
                        )
                        log_event(
                            "gmail_push_thread_recovered_by_recipient",
                            user_email=email_address,
                            gmail_thread_id=thread_id,
                            thread_doc_id=thread_doc_id,
                            from_email=from_email,
                        )
                    else:
                        recovered_single = find_single_active_thread_for_user(
                            provider="google",
                            user_email=email_address,
                        )
                        if recovered_single.get("ok"):
                            thread_doc_id = recovered_single["data"]["thread_doc_id"]
                            thread_data = recovered_single["data"]
                            save_thread(
                                thread_doc_id,
                                {
                                    "provider": "google",
                                    "user_email": email_address.lower(),
                                    "gmail_thread_id": thread_id,
                                },
                            )
                            log_event(
                                "gmail_push_thread_recovered_single_active",
                                user_email=email_address,
                                gmail_thread_id=thread_id,
                                thread_doc_id=thread_doc_id,
                                from_email=from_email,
                            )
                        else:
                            log_event(
                                "gmail_push_untracked_thread_ignored",
                                user_email=email_address,
                                gmail_thread_id=thread_id,
                                from_email=from_email,
                                subject=meta.get("subject", ""),
                            )
                            continue

                # Atomic dedupe guard: one message ID should be processed once.
                mark_res = mark_thread_message_processed(thread_doc_id, msg_id)
                if not mark_res.get("ok"):
                    continue
                if mark_res["data"].get("already_processed"):
                    continue

                turn_count = int(thread_data.get("turn_count", 0))
                if turn_count >= MAX_AGENT_TURNS:
                    log_event("agent_max_turns_reached", thread_doc_id=thread_doc_id, turn_count=turn_count)
                    continue

                convo = _get_thread_messages_for_agent(
                    token,
                    thread_id,
                    refresh_access_token=lambda: token_manager.get_fresh_token(email_address, "google"),
                )
                if not convo:
                    continue

                guardrail_text = _guardrail_override_reply(convo)
                if guardrail_text:
                    sent = _send_agent_reply(
                        token=token, user_email=email_address, to_email=from_email,
                        subject=meta.get("subject", "Re:"), body=guardrail_text,
                        thread_id=thread_id, parent_message_id=meta.get("message_id_header", ""),
                        refresh_access_token=lambda: token_manager.get_fresh_token(email_address, "google"),
                    )
                    if sent:
                        replied += 1
                        save_thread(thread_doc_id, {
                            "gmail_thread_id": thread_id, "status": "active", "state": "open",
                            "turn_count": turn_count + 1, "last_message_id": msg_id,
                            "attendees": list(set((thread_data.get("attendees") or []) + [from_email])),
                        })
                    processed_items += 1
                    continue

                if turn_count >= 10:
                    closing_text = (
                        "It seems we're having trouble finding a time. Feel free to reply "
                        "whenever you have a slot that works, and I'll get it booked right away."
                    )
                    sent = _send_agent_reply(
                        token=token, user_email=email_address, to_email=from_email,
                        subject=meta.get("subject", "Re:"), body=closing_text,
                        thread_id=thread_id, parent_message_id=meta.get("message_id_header", ""),
                        refresh_access_token=lambda: token_manager.get_fresh_token(email_address, "google"),
                    )
                    if sent:
                        replied += 1
                        save_thread(thread_doc_id, {
                            "turn_count": turn_count + 1, "last_message_id": msg_id,
                        })
                    processed_items += 1
                    continue

                reply_text, finalized_payload = generate_scheduling_reply(
                    thread_messages=convo,
                    access_token=token,
                    provider="google",
                    sender_name=thread_data.get("sender_name", "Scheduler Team"),
                    context=thread_data.get("context", "Schedule a meeting via email."),
                    refresh_access_token=lambda: token_manager.get_fresh_token(email_address, "google"),
                    user_timezone=(thread_data.get("user_timezone") or "").strip() or None,
                    user_timezone_offset_minutes=thread_data.get("user_timezone_offset_minutes"),
                    turn_count=turn_count,
                )
                # Empty reply_text is expected when finalize_meeting tool was called --
                # the finalization block below handles event creation and sends confirmation.
                if not reply_text and not finalized_payload:
                    continue

                if finalized_payload and not _has_explicit_rejection(convo):
                    finalized_payload = normalize_finalized_event_times(
                        finalized_payload,
                        user_timezone=(thread_data.get("user_timezone") or "").strip() or None,
                        user_timezone_offset_minutes=thread_data.get("user_timezone_offset_minutes"),
                    )
                    conflict = find_conflicting_event(
                        access_token=token,
                        event_data=finalized_payload,
                        provider="google",
                        refresh_access_token=lambda: token_manager.get_fresh_token(email_address, "google"),
                        user_timezone=(thread_data.get("user_timezone") or "").strip() or None,
                        user_timezone_offset_minutes=thread_data.get("user_timezone_offset_minutes"),
                    )
                    if conflict and not _latest_message_allows_conflict(convo):
                        raw_start = conflict.get("startLocal") or conflict.get("start") or ""
                        conflict_start = _format_event_time(raw_start) if raw_start else "that time"
                        reply_text = (
                            f"I have a conflict at {conflict_start}. "
                            "Should I book this anyway, or would you prefer a different slot?"
                        )
                        finalized_payload = None
                    else:
                        existing_attendees = finalized_payload.get("attendees") or []
                        normalized = {
                            str(a).strip().lower()
                            for a in existing_attendees
                            if isinstance(a, str) and str(a).strip()
                        }
                        normalized.update({email_address.lower(), from_email.lower()})
                        finalized_payload["attendees"] = sorted(normalized)
                        create_result = create_calendar_event(
                            token, finalized_payload, provider="google",
                            refresh_access_token=lambda: token_manager.get_fresh_token(email_address, "google"),
                        )
                        if create_result.get("ok"):
                            friendly_time = _format_event_time(finalized_payload.get("startTime", ""))
                            confirm_text = (
                                f"All set! We're confirmed for {friendly_time}. "
                                "A calendar invite is on its way to you."
                            )
                            _send_agent_reply(
                                token=token, user_email=email_address, to_email=from_email,
                                subject=meta.get("subject", "Re:"), body=confirm_text,
                                thread_id=thread_id, parent_message_id=meta.get("message_id_header", ""),
                                refresh_access_token=lambda: token_manager.get_fresh_token(email_address, "google"),
                            )
                            delete_thread(thread_doc_id)
                            finalized += 1
                            replied += 1
                            log_event("thread_finalized", thread_doc_id=thread_doc_id)
                            processed_items += 1
                            continue
                        else:
                            log_event("thread_finalize_create_event_failed", thread_doc_id=thread_doc_id, error=create_result.get("error"))
                            reply_text = (
                                "I tried to book the meeting but ran into a technical issue. "
                                "Let me try again shortly — no action needed on your end."
                            )
                            finalized_payload = None

                sent = _send_agent_reply(
                    token=token, user_email=email_address, to_email=from_email,
                    subject=meta.get("subject", "Re:"), body=reply_text,
                    thread_id=thread_id, parent_message_id=meta.get("message_id_header", ""),
                    refresh_access_token=lambda: token_manager.get_fresh_token(email_address, "google"),
                )
                if not sent:
                    continue

                replied += 1
                save_thread(thread_doc_id, {
                    "gmail_thread_id": thread_id, "status": "active", "state": "open",
                    "turn_count": turn_count + 1, "last_message_id": msg_id,
                    "attendees": list(set((thread_data.get("attendees") or []) + [from_email])),
                })
                processed_items += 1
            except Exception as exc:
                # Never fail whole webhook batch due to a single bad message.
                log_event("gmail_push_message_process_failed", error=str(exc))
                continue
        if processed_items >= max_items:
            capped = True
            break

    if not capped:
        update_watch_state(
            provider="google",
            user_email=email_address,
            data={"history_id": new_history_id},
        )
    else:
        # Do not advance history baseline when capped; this avoids dropping
        # unprocessed messages from the same Gmail history window.
        log_event(
            "gmail_push_history_not_advanced_due_to_cap",
            user_email=email_address,
            max_items=max_items,
        )
    return {"ok": True, "data": {"replied_count": replied, "finalized_count": finalized, "capped": capped}}
