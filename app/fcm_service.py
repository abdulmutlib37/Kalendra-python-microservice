"""
FCM push notification service for the Kalendra email scheduling flow.

Reads the user's FCM device token from Firestore (stored by the Flutter app)
and sends notifications via Firebase Admin SDK messaging.

Two notification types:
  - send_flow_update: "There has been development in your email scheduling flow"
    Triggered after every successful agent reply (not the initial email).
  - send_flow_completed: "Your email scheduling flow has been successfully completed"
    Triggered after the meeting is booked and the thread is finalized.
"""

from __future__ import annotations

import os
from typing import Any

import firebase_admin.messaging as fcm_messaging

from app.firestore_client import get_db
from app.logging_config import log_event

FCM_TOKENS_COLLECTION = os.getenv("FCM_TOKENS_COLLECTION", "fcm_tokens")


def _get_fcm_token(user_email: str) -> str | None:
    """Retrieve the stored FCM device token for a user."""
    try:
        doc = get_db().collection(FCM_TOKENS_COLLECTION).document(user_email.lower().strip()).get()
        if not doc.exists:
            log_event("fcm_token_not_found", user_email=user_email)
            return None
        data = doc.to_dict() or {}
        token = str(data.get("fcm_token") or "").strip()
        if not token:
            log_event("fcm_token_empty", user_email=user_email)
            return None
        return token
    except Exception as exc:
        log_event("fcm_token_fetch_failed", user_email=user_email, error=str(exc))
        return None


def _send_fcm(token: str, title: str, body: str, data: dict[str, str] | None = None) -> bool:
    """Send an FCM message to a single device token."""
    try:
        message = fcm_messaging.Message(
            notification=fcm_messaging.Notification(title=title, body=body),
            data=data or {},
            token=token,
            android=fcm_messaging.AndroidConfig(
                priority="high",
                notification=fcm_messaging.AndroidNotification(
                    channel_id="kalendra_email_flow",
                    priority="high",
                ),
            ),
            apns=fcm_messaging.APNSConfig(
                payload=fcm_messaging.APNSPayload(
                    aps=fcm_messaging.Aps(sound="default"),
                ),
            ),
        )
        fcm_messaging.send(message)
        return True
    except Exception as exc:
        log_event("fcm_send_failed", error=str(exc))
        return False


def send_flow_update(
    user_email: str,
    recipient_name: str,
    recipient_email: str,
) -> None:
    """
    Send "There has been development in your email scheduling flow" notification.
    Called after each successful agent reply (skipping the initial outbound email).
    """
    token = _get_fcm_token(user_email)
    if not token:
        return

    name_display = (recipient_name or "").strip() or recipient_email
    body = (
        f"Recipient Name: {name_display}\n"
        f"Recipient Email: {recipient_email}"
    )
    sent = _send_fcm(
        token=token,
        title="There has been development in your email scheduling flow",
        body=body,
        data={
            "type": "email_flow_update",
            "recipient_name": name_display,
            "recipient_email": recipient_email,
        },
    )
    if sent:
        log_event("fcm_flow_update_sent", user_email=user_email, recipient_email=recipient_email)


def send_flow_completed(
    user_email: str,
    recipient_name: str,
    recipient_email: str,
) -> None:
    """
    Send "Your email scheduling flow has been successfully completed" notification.
    Called after the meeting is booked and the Firestore thread is deleted/finalized.
    """
    token = _get_fcm_token(user_email)
    if not token:
        return

    name_display = (recipient_name or "").strip() or recipient_email
    body = (
        f"Recipient Name: {name_display}\n"
        f"Recipient Email: {recipient_email}"
    )
    sent = _send_fcm(
        token=token,
        title="Your email scheduling flow has been successfully completed",
        body=body,
        data={
            "type": "email_flow_completed",
            "recipient_name": name_display,
            "recipient_email": recipient_email,
        },
    )
    if sent:
        log_event("fcm_flow_completed_sent", user_email=user_email, recipient_email=recipient_email)
