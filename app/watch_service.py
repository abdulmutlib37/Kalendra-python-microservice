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
from datetime import datetime, timedelta, timezone
from email.utils import parseaddr
from typing import Any

import httpx

from app.logging_config import log_event
from app.repository import update_watch_state
from app.token_manager import TokenManager

GMAIL_API_BASE = "https://gmail.googleapis.com/gmail/v1/users/me"


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


def process_gmail_push(push_payload: dict[str, Any], token_manager: TokenManager) -> dict[str, Any]:
    """
    Process Gmail Pub/Sub push message and reply "Noted" on new messages.
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
    replied = 0
    for item in history_items:
        for added in item.get("messagesAdded", []):
            msg = added.get("message", {})
            msg_id = msg.get("id")
            thread_id = msg.get("threadId")
            if not msg_id or not thread_id:
                continue

            if _send_noted_reply(token, email_address, thread_id, msg_id):
                replied += 1

    update_watch_state(
        provider="google",
        user_email=email_address,
        data={"history_id": new_history_id},
    )
    return {"ok": True, "data": {"replied_count": replied}}
