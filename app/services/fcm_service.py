from __future__ import annotations

from typing import Any

import firebase_admin.messaging as fcm_messaging

from app.firestore_client import get_db
from app.logging_config import log_event


def _get_fcm_token(user_email: str) -> str | None:
    try:
        doc = get_db().collection("user_management").document(user_email.lower().strip()).get()
        if not doc.exists:
            log_event("fcm_token_not_found", user_email=user_email)
            return None
        data = doc.to_dict() or {}
        token = str((data.get("fcmToken") or {}).get("token") or "").strip()
        if not token:
            log_event("fcm_token_empty", user_email=user_email)
            return None
        return token
    except Exception as exc:
        log_event("fcm_token_fetch_failed", user_email=user_email, error=str(exc))
        return None


def _send_fcm(token: str, title: str, body: str, data: dict[str, str] | None = None) -> bool:
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


def send_flow_completed(
    user_email: str,
    recipient_name: str,
    recipient_email: str,
    meeting_title: str = "",
    start_time: str = "",
    end_time: str = "",
) -> None:
    """Fires when a meeting is confirmed and added to calendar."""
    token = _get_fcm_token(user_email)
    if not token:
        return
    name_display = (recipient_name or "").strip() or recipient_email
    title_display = (meeting_title or "").strip()

    from app.utils.time_utils import format_event_time
    friendly_time = format_event_time(start_time) if start_time else ""
    if friendly_time:
        # Split into day and time parts for the body template
        # format_event_time returns e.g. "Monday, Jun 9 at 2:00 PM"
        parts = friendly_time.split(" at ", 1)
        day_part = parts[0].strip() if parts else friendly_time
        time_part = parts[1].strip() if len(parts) > 1 else ""
        body = f"{name_display} confirmed for {day_part} at {time_part}. Added to your calendar."
    else:
        body = f"{name_display} confirmed. Added to your calendar."

    notif_title = f"{title_display} is locked in with {name_display}" if title_display else f"Locked in with {name_display}"

    sent = _send_fcm(
        token=token,
        title=notif_title,
        body=body,
        data={
            "type": "email_flow_completed",
            "recipient_name": name_display,
            "recipient_email": recipient_email.lower(),
            "meeting_title": title_display,
            "sender_email": user_email.lower(),
            "start_time": (start_time or "").strip(),
            "end_time": (end_time or "").strip(),
        },
    )
    if sent:
        log_event("fcm_flow_completed_sent", user_email=user_email, recipient_email=recipient_email)


def send_intent_notification(
    user_email: str,
    recipient_name: str,
    recipient_email: str,
    meeting_title: str,
    intent: str,
    proposed_day: str = "",
    proposed_time: str = "",
    conflict_day: str = "",
    conflict_time: str = "",
) -> None:
    """
    Fires a contextual notification based on classified recipient intent.

    Handled intents:
      new_time_suggestion  — recipient proposed a specific time
      reschedule_booked    — post-booking reschedule request
      calendar_conflict    — conflicting event detected while booking
      stalled              — 3+ rounds with no agreed time
    """
    token = _get_fcm_token(user_email)
    if not token:
        return

    name_display = (recipient_name or "").strip() or recipient_email
    title_display = (meeting_title or "").strip()

    if intent == "new_time_suggestion":
        notif_title = f"{name_display} suggested a new time"
        if proposed_day and proposed_time:
            notif_body = f"They proposed {proposed_day} at {proposed_time} for {title_display}. Works for you?"
        elif proposed_day:
            notif_body = f"They proposed {proposed_day} for {title_display}. Works for you?"
        else:
            notif_body = f"They proposed a new time for {title_display}. Works for you?"

    elif intent == "reschedule_booked":
        notif_title = f"{name_display} wants to move {title_display}"
        notif_body = "They've asked to reschedule. I'll hold off until you decide, tap to review options."

    elif intent == "calendar_conflict":
        day_label = conflict_day or "that day"
        time_label = conflict_time or "that time"
        notif_title = f"Heads up — conflict on {day_label}"
        notif_body = f"Something new landed at {time_label} while I was booking {title_display}. Which takes priority?"

    elif intent == "needs_clarification":
        notif_title = f"{name_display}'s reply needs your read"
        notif_body = f"I'm not sure how to respond — their message about {title_display} is a bit unclear." if title_display else f"I'm not sure how to respond — their message is a bit unclear."

    elif intent == "stalled":
        notif_title = f"Scheduling {title_display} is taking a while"
        notif_body = f"3 rounds in with {name_display} — no match yet. Want me to widen your availability or hand this back to you?"

    elif intent == "cancelled":
        notif_title = f"{name_display} cancelled {title_display}" if title_display else f"{name_display} cancelled the meeting"
        notif_body = "They've declined the meeting. I've stopped scheduling — tap to review or restart."

    else:
        return

    sent = _send_fcm(
        token=token,
        title=notif_title,
        body=notif_body,
        data={
            "type": "email_flow_update",
            "intent": intent,
            "recipient_name": name_display,
            "recipient_email": recipient_email.lower(),
            "meeting_title": title_display,
            "sender_email": user_email.lower(),
        },
    )
    if sent:
        log_event("fcm_intent_notification_sent", user_email=user_email, recipient_email=recipient_email, intent=intent)
