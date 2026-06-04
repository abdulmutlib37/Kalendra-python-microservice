from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from app.logging_config import log_event
from app.repository import (
    create_thread,
    delete_thread,
    find_thread_by_gmail_thread,
    mark_thread_message_processed,
    save_thread,
    update_watch_state,
    get_watch_state,
)
from app.services.agent_service import (
    create_calendar_event,
    find_conflicting_event,
    generate_initial_email,
    generate_scheduling_reply,
)
from app.services.fcm_service import send_flow_completed, send_flow_update
from app.token_manager import TokenManager
from app.utils.http_client import request_with_refresh
from app.utils.text_utils import strip_reply_prefix, strip_quoted_reply
from app.utils.time_utils import format_event_time, normalize_finalized_event_times

OUTLOOK_API_BASE = "https://graph.microsoft.com/v1.0"
MAX_AGENT_TURNS = int(os.getenv("MAX_AGENT_TURNS", "15"))


def _outlook(token: str, method: str, path: str, refresh_fn: Callable[[], str | None] | None = None, **kwargs):
    return request_with_refresh(method, f"{OUTLOOK_API_BASE}{path}", token, refresh_fn, **kwargs)


def get_thread_messages_structured(thread_data: dict) -> list[dict]:
    """Parse Outlook conversation_messages into structured dicts: {sender, body, timestamp (ISO)}."""
    raw_messages = list(thread_data.get("conversation_messages") or [])
    if not raw_messages:
        return []

    initiated_at = thread_data.get("initiated_at")
    try:
        base_dt = datetime.fromisoformat(str(initiated_at)) if initiated_at else datetime.now(timezone.utc)
        if base_dt.tzinfo is None:
            base_dt = base_dt.replace(tzinfo=timezone.utc)
    except Exception:
        base_dt = datetime.now(timezone.utc)

    result = []
    for i, raw in enumerate(raw_messages):
        lines = str(raw).split("\n", 1)
        sender = ""
        body = raw
        if len(lines) == 2 and lines[0].startswith("From:"):
            sender = lines[0][len("From:"):].strip()
            body = lines[1].strip()
        # Spread messages evenly from initiated_at; last message is closest to now
        msg_time = (base_dt + (datetime.now(timezone.utc) - base_dt) * (i / max(len(raw_messages) - 1, 1)))
        result.append({
            "sender": sender,
            "body": body,
            "timestamp": msg_time.isoformat(),
        })
    return result


def renew_outlook_watch(user_email: str, token_manager: TokenManager) -> dict[str, Any]:
    token = token_manager.get_fresh_token(user_email=user_email, provider="outlook")
    if not token:
        return {"ok": False, "error": "No valid Outlook token"}

    watch_result = get_watch_state(provider="outlook", user_email=user_email)
    state = watch_result.get("data") if watch_result.get("ok") else {}
    subscription_id = str((state or {}).get("subscription_id", "")).strip()
    if not subscription_id:
        return _ensure_subscription(token=token, user_email=user_email)

    expires = (datetime.now(timezone.utc) + timedelta(days=2)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    patch_resp = _outlook(token, "PATCH", f"/subscriptions/{subscription_id}", json={"expirationDateTime": expires})
    if patch_resp.status_code == 404:
        return _ensure_subscription(token=token, user_email=user_email)
    if patch_resp.status_code not in (200, 202):
        return {"ok": False, "error": f"outlook_subscription_renew_failed:{patch_resp.status_code}"}

    expiry_dt = datetime.fromisoformat(expires.replace("Z", "+00:00"))
    update_watch_state(provider="outlook", user_email=user_email, data={
        "watch_expiration": expiry_dt,
        "expires_at": expiry_dt - timedelta(hours=6),
    })
    return {"ok": True, "data": {"user_email": user_email, "subscription_id": subscription_id}}


def _ensure_subscription(token: str, user_email: str) -> dict[str, Any]:
    webhook_url = (os.getenv("OUTLOOK_WEBHOOK_URL") or "").strip()
    if not webhook_url:
        return {"ok": False, "error": "OUTLOOK_WEBHOOK_URL is not configured"}

    expires = (datetime.now(timezone.utc) + timedelta(days=2)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    resp = _outlook(token, "POST", "/subscriptions", json={
        "changeType": "created",
        "notificationUrl": webhook_url,
        "resource": "/me/messages",
        "expirationDateTime": expires,
        "clientState": f"kalendra::{user_email.lower()}",
    })
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


def _send_initial_email(
    token: str, to_email: str, sender_name: str,
    recipient_name: str | None, context: str,
    custom_subject: str | None = None, custom_body: str | None = None,
    tone: str | None = None,
) -> dict[str, Any]:
    if custom_subject and custom_body:
        subject = custom_subject.strip()
        body = custom_body.strip()
    else:
        try:
            subject, body = generate_initial_email(
                sender_name=sender_name,
                recipient_name=(recipient_name or "").strip() or "there",
                context=(context or "").strip() or "schedule a meeting",
                tone=tone,
            )
        except Exception as exc:
            log_event("initial_email_llm_failed", error=str(exc), to=to_email)
            return {"ok": False, "error": f"initial_email_llm_failed:{exc}", "status_code": 502}

    draft_resp = _outlook(token, "POST", "/me/messages", json={
        "subject": subject,
        "body": {"contentType": "Text", "content": body},
        "toRecipients": [{"emailAddress": {"address": to_email}}],
    })
    if draft_resp.status_code not in (200, 201):
        err_text = _extract_graph_error(draft_resp)
        log_event("outlook_initial_email_draft_failed", to=to_email, status_code=draft_resp.status_code, response=err_text[:500])
        return {"ok": False, "status_code": draft_resp.status_code, "error": f"outlook_message_create_failed:{err_text}"}

    draft = draft_resp.json()
    message_id = draft.get("id")
    conversation_id = draft.get("conversationId")
    if not message_id:
        return {"ok": False, "status_code": 502, "error": "outlook_message_create_missing_id"}

    send_resp = _outlook(token, "POST", f"/me/messages/{message_id}/send")
    if send_resp.status_code not in (200, 202):
        err_text = _extract_graph_error(send_resp)
        log_event("outlook_initial_email_send_failed", to=to_email, status_code=send_resp.status_code, response=err_text[:500])
        return {"ok": False, "status_code": send_resp.status_code, "error": f"outlook_message_send_failed:{err_text}"}

    thread_id = conversation_id or message_id
    log_event("email_sent", action="initiate_flow_outlook", thread_id=thread_id, to=to_email)
    return {"ok": True, "thread_id": thread_id, "message_id": message_id}


def _extract_graph_error(resp) -> str:
    try:
        payload = resp.json()
        if isinstance(payload, dict):
            msg = payload.get("error", {}).get("message")
            if msg:
                return str(msg)
    except Exception:
        pass
    return resp.text[:1000]


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
    return bool(re.search(
        r"\b(book it anyway|schedule anyway|still book|still schedule|conflict is fine|"
        r"i am okay with conflict|go ahead anyway|go ahead|do it|book it|yes|sure|"
        r"that('?s| is) fine|okay|ok|no problem|that works|works for me|"
        r"you can do it|please do|proceed|confirm it|lock it in)\b", latest))


def _guardrail_reply(thread_messages: list[str]) -> str | None:
    latest = (thread_messages[-1] or "").lower() if thread_messages else ""
    parts = latest.split("\n", 1)
    text = parts[1].strip() if len(parts) == 2 else latest
    if re.search(r"\b(trump|epstein|politics|crypto price|stock tips|weather|sports score|movie|music|news)\b", text):
        return "I can help with meeting scheduling in this thread. Please share a time window that works for you and I will coordinate it."
    if re.search(r"\b(who is in your meeting|last meeting with|all bookings|share your calendar)\b", text):
        return "I can only share high-level availability for scheduling and cannot share private meeting details. If helpful, I can propose open time slots for our meeting."
    return None


def _send_reply(token: str, user_email: str, message_id: str, comment: str, refresh_fn: Callable) -> bool:
    resp = _outlook(token, "POST", f"/me/messages/{message_id}/reply", refresh_fn, json={"comment": comment})
    if resp.status_code not in (200, 202):
        log_event("outlook_reply_send_failed", user_email=user_email, status_code=resp.status_code)
        return False
    return True


def _handle_finalization(
    token: str, user_email: str, from_email: str, message_id: str,
    thread_id: str, thread_doc_id: str, thread_data: dict,
    finalized_payload: dict, convo: list[str], refresh_fn: Callable,
) -> tuple[bool, str | None]:
    finalized_payload = normalize_finalized_event_times(
        finalized_payload,
        user_timezone=(thread_data.get("user_timezone") or "").strip() or None,
        user_timezone_offset_minutes=thread_data.get("user_timezone_offset_minutes"),
    )
    conflict = find_conflicting_event(
        access_token=token, event_data=finalized_payload, provider="outlook",
        refresh_access_token=refresh_fn,
        user_timezone=(thread_data.get("user_timezone") or "").strip() or None,
        user_timezone_offset_minutes=thread_data.get("user_timezone_offset_minutes"),
    )
    if conflict and not _latest_message_allows_conflict(convo):
        raw_start = conflict.get("startLocal") or conflict.get("start") or ""
        conflict_start = format_event_time(raw_start) if raw_start else "that time"
        return False, f"I have a conflict at {conflict_start}. Should I book this anyway, or would you prefer a different slot?"

    existing_attendees = finalized_payload.get("attendees") or []
    normalized = {str(a).strip().lower() for a in existing_attendees if isinstance(a, str) and str(a).strip()}
    normalized.update({user_email.lower(), from_email.lower()})
    finalized_payload["attendees"] = sorted(normalized)

    create_result = create_calendar_event(token, finalized_payload, provider="outlook", refresh_access_token=refresh_fn)
    if create_result.get("ok"):
        friendly_time = format_event_time(finalized_payload.get("startTime", ""))
        confirm_text = f"All set! We're confirmed for {friendly_time}. A calendar invite is on its way to you."
        _send_reply(token, user_email, message_id, confirm_text, refresh_fn)
        send_flow_completed(
            user_email=user_email,
            recipient_name=thread_data.get("recipient_name", ""),
            recipient_email=from_email,
            meeting_title=finalized_payload.get("summary", ""),
            start_time=finalized_payload.get("startTime", ""),
            end_time=finalized_payload.get("endTime", ""),
        )
        delete_thread(thread_doc_id)
        log_event("thread_finalized", thread_doc_id=thread_doc_id, provider="outlook")
        return True, None
    else:
        log_event("thread_finalize_create_event_failed", thread_doc_id=thread_doc_id, provider="outlook", error=create_result.get("error"))
        return False, "I tried to book the meeting but ran into a technical issue. Let me try again shortly — no action needed on your end."


def initiate_outlook_email_flow(
    outlook_access_token: str, sender_name: str, recipient_email: str,
    recipient_name: str | None, context: str,
    refresh_token: str | None = None,
    email_subject: str | None = None, email_body: str | None = None,
    tone: str | None = None,
    user_timezone: str | None = None, user_timezone_offset_minutes: int | None = None,
    token_manager: TokenManager | None = None,
) -> dict[str, Any]:
    token = (outlook_access_token or "").strip()
    if not token:
        return {"ok": False, "error": "outlook_access_token is required", "status_code": 400}
    if not recipient_email or not sender_name:
        return {"ok": False, "error": "sender_name and recipient_email are required", "status_code": 400}

    profile_resp = _outlook(token, "GET", "/me?$select=mail,userPrincipalName")
    if profile_resp.status_code != 200:
        return {"ok": False, "error": "failed_to_fetch_outlook_profile", "status_code": 401}
    p = profile_resp.json()
    user_email = (p.get("mail") or p.get("userPrincipalName") or "").lower().strip()
    if not user_email:
        return {"ok": False, "error": "outlook_profile_missing_email", "status_code": 401}

    # Store tokens keyed on the Outlook profile email so the webhook handler
    # (which extracts user_email from clientState) can retrieve them later.
    rt = (refresh_token or "").strip()
    if token_manager and rt:
        existing_rt = token_manager.get_existing_refresh_token(user_email, "outlook")
        token_manager.store_tokens(
            user_email=user_email,
            provider="outlook",
            access_token=token,
            refresh_token=existing_rt or rt,
            expires_in_seconds=3600,
        )
    elif token_manager:
        existing_rt = token_manager.get_existing_refresh_token(user_email, "outlook")
        if existing_rt:
            token_manager.store_tokens(
                user_email=user_email,
                provider="outlook",
                access_token=token,
                refresh_token=existing_rt,
                expires_in_seconds=3600,
            )

    send_result = _send_initial_email(token=token, to_email=recipient_email, sender_name=sender_name,
                                      recipient_name=recipient_name, context=context or "",
                                      custom_subject=email_subject, custom_body=email_body, tone=tone)
    if not send_result.get("ok"):
        return {"ok": False, "error": send_result.get("error", "failed_to_send_initial_outlook_email"),
                "status_code": int(send_result.get("status_code") or 502)}

    thread_id = str(send_result.get("thread_id") or "").strip()
    if not thread_id:
        return {"ok": False, "error": "failed_to_resolve_outlook_thread_id", "status_code": 502}

    created = create_thread(provider="outlook", user_email=user_email, data={
        "gmail_thread_id": thread_id, "status": "active", "state": "open", "turn_count": 0,
        "context": context or "", "sender_name": sender_name,
        "recipient_email": recipient_email.lower(), "recipient_name": (recipient_name or "").strip(),
        "summary": (email_subject or "").strip(),
        "user_timezone": (user_timezone or "").strip(), "user_timezone_offset_minutes": user_timezone_offset_minutes,
        "attendees": [recipient_email.lower()], "source": "node_execute_initiate_email_flow",
        "initiated_at": datetime.now(timezone.utc),
    })
    if not created.get("ok"):
        return {"ok": False, "error": created.get("error", "thread_create_failed"), "status_code": 500}

    watch_result = _ensure_subscription(token=token, user_email=user_email)
    if not watch_result.get("ok"):
        return {"ok": False, "error": watch_result.get("error", "outlook_watch_setup_failed"), "status_code": 502}

    return {"ok": True, "data": {"status": "ok", "thread_id": thread_id,
                                  "watch_ok": True, "watch_error": None}}


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

        def refresh_fn(u=user_email):
            return token_manager.get_fresh_token(u, "outlook")

        msg_resp = _outlook(token, "GET",
                            f"/me/messages/{message_id}?$select=id,conversationId,subject,from,bodyPreview,internetMessageId",
                            refresh_fn)
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

        snippet = (msg.get("bodyPreview") or "").strip()
        convo = list(thread_data.get("conversation_messages") or [])
        if snippet:
            convo.append(f"From: {from_email}\n{snippet}")
        convo = convo[-30:]
        if not convo:
            continue

        guardrail_text = _guardrail_reply(convo)
        if guardrail_text:
            if _send_reply(token, user_email, message_id, guardrail_text, refresh_fn):
                replied += 1
                save_thread(thread_doc_id, {
                    "gmail_thread_id": thread_id, "status": "active", "state": "open",
                    "turn_count": turn_count + 1, "last_message_id": message_id,
                    "attendees": list(set((thread_data.get("attendees") or []) + [from_email])),
                    "conversation_messages": (convo + [f"From: {user_email}\n{guardrail_text}"])[-30:],
                })
            continue

        if turn_count >= 10:
            closing_text = "It seems we're having trouble finding a time. Feel free to reply whenever you have a slot that works, and I'll get it booked right away."
            if _send_reply(token, user_email, message_id, closing_text, refresh_fn):
                replied += 1
                save_thread(thread_doc_id, {
                    "turn_count": turn_count + 1, "last_message_id": message_id,
                    "conversation_messages": (convo + [f"From: {user_email}\n{closing_text}"])[-30:],
                })
            continue

        reply_text, finalized_payload = generate_scheduling_reply(
            thread_messages=convo, access_token=token, provider="outlook",
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
                token, user_email, from_email, message_id, thread_id,
                thread_doc_id, thread_data, finalized_payload, convo, refresh_fn,
            )
            if did_finalize:
                finalized += 1
                replied += 1
                continue
            elif fallback_text:
                reply_text = fallback_text

        if not reply_text:
            continue
        if _send_reply(token, user_email, message_id, reply_text, refresh_fn):
            send_flow_update(user_email=user_email, recipient_name=thread_data.get("recipient_name", ""), recipient_email=from_email,
                             meeting_title=strip_reply_prefix(msg.get("subject", "")))
            replied += 1
            save_thread(thread_doc_id, {
                "gmail_thread_id": thread_id, "status": "active", "state": "open",
                "turn_count": turn_count + 1, "last_message_id": message_id,
                "attendees": list(set((thread_data.get("attendees") or []) + [from_email])),
                "conversation_messages": (convo + [f"From: {user_email}\n{reply_text}"])[-30:],
            })

    return {"ok": True, "data": {"replied_count": replied, "finalized_count": finalized}}
