"""
Firestore repository — CRUD helpers for each collection.

Collections:
  threads              keyed by thread_id
  processed_messages   keyed by provider_messageId  (TTL via expires_at)
  watch_state          keyed by user_id

Every function uses structured logging with the current correlation_id
and returns a consistent dict: {"ok": bool, "data": ...} or {"ok": bool, "error": ...}.
"""

from datetime import datetime, timedelta, timezone

from google.cloud.firestore_v1 import SERVER_TIMESTAMP

from app.firestore_client import get_db
from app.logging_config import log_event


# ---------------------------------------------------------------------------
# threads
# ---------------------------------------------------------------------------

def save_thread(thread_id: str, data: dict) -> dict:
    try:
        doc = {
            **data,
            "created_at": data.get("created_at", SERVER_TIMESTAMP),
            "updated_at": SERVER_TIMESTAMP,
        }
        get_db().collection("threads").document(thread_id).set(doc, merge=True)
        log_event("thread_saved", thread_id=thread_id)
        return {"ok": True, "data": {"thread_id": thread_id}}
    except Exception as e:
        log_event("thread_save_failed", thread_id=thread_id, error=str(e))
        return {"ok": False, "error": str(e)}


def get_thread(thread_id: str) -> dict:
    try:
        snap = get_db().collection("threads").document(thread_id).get()
        if snap.exists:
            log_event("thread_fetched", thread_id=thread_id)
            return {"ok": True, "data": snap.to_dict()}
        log_event("thread_not_found", thread_id=thread_id)
        return {"ok": False, "error": "not_found"}
    except Exception as e:
        log_event("thread_fetch_failed", thread_id=thread_id, error=str(e))
        return {"ok": False, "error": str(e)}


# ---------------------------------------------------------------------------
# processed_messages
# ---------------------------------------------------------------------------

def mark_message_processed(provider_message_id: str, thread_id: str, user_id: str) -> dict:
    try:
        now = datetime.now(timezone.utc)
        doc = {
            "user_id": user_id,
            "thread_id": thread_id,
            "processed_at": now,
            "expires_at": now + timedelta(hours=24),
        }
        get_db().collection("processed_messages").document(provider_message_id).set(doc)
        log_event("message_marked_processed", provider_message_id=provider_message_id, thread_id=thread_id)
        return {"ok": True, "data": {"provider_message_id": provider_message_id}}
    except Exception as e:
        log_event("message_mark_failed", provider_message_id=provider_message_id, error=str(e))
        return {"ok": False, "error": str(e)}


def is_message_processed(provider_message_id: str) -> dict:
    try:
        snap = get_db().collection("processed_messages").document(provider_message_id).get()
        processed = snap.exists
        log_event("message_processed_check", provider_message_id=provider_message_id, processed=processed)
        return {"ok": True, "data": {"processed": processed}}
    except Exception as e:
        log_event("message_check_failed", provider_message_id=provider_message_id, error=str(e))
        return {"ok": False, "error": str(e)}


# ---------------------------------------------------------------------------
# watch_state
# ---------------------------------------------------------------------------

def update_watch_state(user_id: str, data: dict) -> dict:
    try:
        doc = {
            **data,
            "created_at": data.get("created_at", SERVER_TIMESTAMP),
            "updated_at": SERVER_TIMESTAMP,
        }
        get_db().collection("watch_state").document(user_id).set(doc, merge=True)
        log_event("watch_state_updated", user_id=user_id)
        return {"ok": True, "data": {"user_id": user_id}}
    except Exception as e:
        log_event("watch_state_update_failed", user_id=user_id, error=str(e))
        return {"ok": False, "error": str(e)}


def get_watch_state(user_id: str) -> dict:
    try:
        snap = get_db().collection("watch_state").document(user_id).get()
        if snap.exists:
            log_event("watch_state_fetched", user_id=user_id)
            return {"ok": True, "data": snap.to_dict()}
        log_event("watch_state_not_found", user_id=user_id)
        return {"ok": False, "error": "not_found"}
    except Exception as e:
        log_event("watch_state_fetch_failed", user_id=user_id, error=str(e))
        return {"ok": False, "error": str(e)}
