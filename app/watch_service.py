"""
Google watch renewal + push processing (Google-only).

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
from typing import Any

import httpx
from google.auth.transport import requests as g_requests
from google.oauth2 import id_token as g_id_token

from app.agent_service import create_calendar_event, generate_scheduling_reply
from app.logging_config import log_event
from app.repository import (
    create_thread,
    delete_thread,
    find_thread_by_gmail_thread,
    mark_thread_message_processed,
    save_thread,
    update_watch_state,
)
from app.token_manager import TokenManager

GMAIL_API_BASE = "https://gmail.googleapis.com/gmail/v1/users/me"
MAX_AGENT_TURNS = int(os.getenv("MAX_AGENT_TURNS", "15"))


def _extract_subject_from_context(context: str) -> str:
    text = (context or "").strip()
    if not text:
        return "Meeting Scheduling"

    # Prefer explicit title signals from upstream prompts.
    quoted = re.search(r'titled\s+"([^"]+)"', text, flags=re.IGNORECASE)
    if quoted and quoted.group(1).strip():
        return quoted.group(1).strip()

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

    # Remove robotic instruction style prefixes.
    text = re.sub(r"^user requested to\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^please\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip(" .")

    lower = text.lower()
    if lower.startswith("schedule a meeting titled"):
        # Keep only practical scheduling detail if available (e.g. "next week").
        if "next week" in lower:
            return "I am looking to schedule this for next week."
        return ""

    if len(text) < 12:
        return ""
    return text


def _request_gmail(token: str, method: str, path: str, **kwargs) -> httpx.Response:
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"
    headers["Content-Type"] = "application/json"
    return httpx.request(
        method=method,
        url=f"{GMAIL_API_BASE}{path}",
        headers=headers,
        timeout=20.0,
        **kwargs,
    )


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


def _get_message_metadata(token: str, message_id: str) -> dict[str, str] | None:
    resp = _request_gmail(
        token,
        "GET",
        f"/messages/{message_id}",
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


def _get_thread_messages_for_agent(token: str, thread_id: str) -> list[str]:
    resp = _request_gmail(token, "GET", f"/threads/{thread_id}", params={"format": "metadata", "metadataHeaders": ["From", "Subject"]})
    if resp.status_code != 200:
        log_event("gmail_thread_fetch_failed", thread_id=thread_id, status_code=resp.status_code)
        return []
    thread = resp.json()
    messages = []
    for m in thread.get("messages", []):
        payload = m.get("payload", {})
        headers = {h.get("name", ""): h.get("value", "") for h in payload.get("headers", [])}
        sender = headers.get("From", "Unknown")
        snippet = (m.get("snippet") or "").strip()
        if snippet:
            messages.append(f"From: {sender}\n{snippet}")
    return messages


def _send_agent_reply(
    token: str,
    user_email: str,
    to_email: str,
    subject: str,
    body: str,
    thread_id: str,
    parent_message_id: str,
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
    send_resp = _request_gmail(token, "POST", "/messages/send", json={"raw": raw_b64, "threadId": thread_id})
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
) -> tuple[str | None, str | None]:
    """
    Send the first email in a scheduling flow and return (thread_id, message_id).
    """
    context_text = (context or "").strip()
    subject = _extract_subject_from_context(context_text)
    detail_line = _extract_human_detail_from_context(context_text)

    greeting_name = (recipient_name or "").strip()
    salutation = f"Hi {greeting_name}," if greeting_name else "Hi,"

    body_lines = [
        salutation,
        "",
        "I would like to schedule a meeting with you.",
    ]
    if detail_line:
        body_lines.extend(["", detail_line])
    body_lines.extend(
        [
            "",
            "Please share a suitable time.",
            "",
            "Regards,",
            sender_name,
            "",
            "Email Scheduling - Powered by Kalendra",
        ]
    )
    body = "\n".join(body_lines)

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

    # Store token for immediate follow-up processing. In this flow we only receive
    # an access token, so we mirror it into refresh_token as a short-term fallback.
    # This keeps back-and-forth working like email-poc for active sessions.
    if token_manager is not None:
        try:
            stored = token_manager.store_tokens(
                user_email=user_email,
                provider="google",
                access_token=token,
                refresh_token=token,
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
            "attendees": [recipient_email.lower()],
            "source": "node_execute_initiate_email_flow",
            "initiated_at": datetime.now(timezone.utc),
        },
    )
    if not created.get("ok"):
        return {"ok": False, "error": created.get("error", "thread_create_failed"), "status_code": 500}

    enable_watch_setup = os.getenv("ENABLE_INITIATE_WATCH_SETUP", "false").lower() == "true"
    watch_result: dict[str, Any] = {"ok": True, "skipped": not enable_watch_setup}
    if enable_watch_setup:
        watch_result = _ensure_watch_with_access_token(token=token, user_email=user_email)
        if not watch_result.get("ok"):
            log_event(
                "gmail_watch_setup_warning",
                user_email=user_email,
                thread_id=thread_id,
                error=watch_result.get("error"),
            )
    else:
        log_event(
            "gmail_watch_setup_skipped_on_initiate",
            user_email=user_email,
            thread_id=thread_id,
        )

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

                # Fast reject: only process threads explicitly tracked in Firestore.
                thread_lookup = find_thread_by_gmail_thread("google", email_address, thread_id)
                if thread_lookup.get("ok"):
                    thread_doc_id = thread_lookup["data"]["thread_doc_id"]
                    thread_data = thread_lookup["data"]
                else:
                    log_event(
                        "gmail_push_untracked_thread_ignored",
                        user_email=email_address,
                        gmail_thread_id=thread_id,
                    )
                    continue

                meta = _get_message_metadata(token, msg_id)
                if not meta:
                    continue
                from_email = parseaddr(meta["from"])[1].lower()
                if not from_email or from_email == email_address.lower():
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

                convo = _get_thread_messages_for_agent(token, thread_id)
                if not convo:
                    continue
                reply_text, finalized_payload = generate_scheduling_reply(
                    thread_messages=convo,
                    google_access_token=token,
                    sender_name=thread_data.get("sender_name", "Scheduler Team"),
                    context=thread_data.get("context", "Schedule a meeting via email."),
                    refresh_access_token=lambda: token_manager.get_fresh_token(email_address, "google"),
                )
                if not reply_text:
                    continue

                sent = _send_agent_reply(
                    token=token,
                    user_email=email_address,
                    to_email=from_email,
                    subject=meta.get("subject", "Re:"),
                    body=reply_text,
                    thread_id=thread_id,
                    parent_message_id=meta.get("message_id_header", ""),
                )
                if not sent:
                    continue

                replied += 1
                save_thread(
                    thread_doc_id,
                    {
                        "gmail_thread_id": thread_id,
                        "status": "active",
                        "state": "open",
                        "turn_count": turn_count + 1,
                        "last_message_id": msg_id,
                        "attendees": list(set((thread_data.get("attendees") or []) + [from_email])),
                    },
                )

                if finalized_payload:
                    create_result = create_calendar_event(
                        token,
                        finalized_payload,
                        refresh_access_token=lambda: token_manager.get_fresh_token(email_address, "google"),
                    )
                    if create_result.get("ok"):
                        delete_thread(thread_doc_id)
                        finalized += 1
                        log_event("thread_finalized", thread_doc_id=thread_doc_id)
                    else:
                        log_event("thread_finalize_create_event_failed", thread_doc_id=thread_doc_id, error=create_result.get("error"))
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
