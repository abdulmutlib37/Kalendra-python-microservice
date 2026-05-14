from __future__ import annotations

import threading
from datetime import datetime, timezone

from google.api_core.exceptions import AlreadyExists
from google.cloud import firestore as g_firestore
from google.cloud.firestore_v1 import SERVER_TIMESTAMP

from app.firestore_client import get_db
from app.logging_config import log_event

ROOT_DOC = "a2h-emailing/config"
_counter_lock = threading.Lock()


def _threads_col():
    return get_db().document(ROOT_DOC).collection("threads")


def _thread_index_col():
    return get_db().document(ROOT_DOC).collection("thread_index")


def _thread_lookup_doc_id(provider: str, user_email: str, gmail_thread_id: str) -> str:
    return f"{provider.lower()}::{user_email.lower()}::{gmail_thread_id.strip()}"


def _parse_thread_doc_id(doc_id: str) -> tuple[str, str] | None:
    try:
        parts = doc_id.split("::")
        if len(parts) < 3:
            return None
        return parts[0].lower(), parts[1].lower()
    except Exception:
        return None


def _upsert_thread_lookup(provider: str, user_email: str, gmail_thread_id: str, thread_doc_id: str) -> None:
    if not gmail_thread_id:
        return
    lookup_id = _thread_lookup_doc_id(provider, user_email, gmail_thread_id)
    _thread_index_col().document(lookup_id).set(
        {
            "provider": provider.lower(),
            "user_email": user_email.lower(),
            "gmail_thread_id": gmail_thread_id,
            "thread_doc_id": thread_doc_id,
            "updated_at": SERVER_TIMESTAMP,
        },
        merge=True,
    )


def _delete_thread_lookup(provider: str, user_email: str, gmail_thread_id: str) -> None:
    if not gmail_thread_id:
        return
    lookup_id = _thread_lookup_doc_id(provider, user_email, gmail_thread_id)
    _thread_index_col().document(lookup_id).delete()


def _next_thread_id(provider: str, user_email: str) -> str:
    db = get_db()
    counter_ref = db.document(ROOT_DOC)
    with _counter_lock:
        transaction = db.transaction()

        @g_firestore.transactional
        def _increment(txn):
            snap = counter_ref.get(transaction=txn)
            data = snap.to_dict() or {} if snap.exists else {}
            current = data.get("thread_counter", 0)
            new_val = current + 1
            if snap.exists:
                txn.update(counter_ref, {"thread_counter": new_val, "updated_at": SERVER_TIMESTAMP})
            else:
                txn.set(counter_ref, {"thread_counter": new_val, "created_at": SERVER_TIMESTAMP})
            return new_val

        counter = _increment(transaction)
    return f"{provider.lower()}::{user_email.lower()}::{counter}"


def make_thread_doc_id(provider: str, user_email: str, thread_id: int | str) -> str:
    return f"{provider.lower()}::{user_email.lower()}::{thread_id}"


def save_thread(doc_id: str, data: dict) -> dict:
    try:
        doc = {**data, "created_at": data.get("created_at", SERVER_TIMESTAMP), "updated_at": SERVER_TIMESTAMP}
        _threads_col().document(doc_id).set(doc, merge=True)
        parsed = _parse_thread_doc_id(doc_id)
        provider = (data.get("provider") or (parsed[0] if parsed else "")).lower()
        user_email = (data.get("user_email") or (parsed[1] if parsed else "")).lower()
        gmail_thread_id = str(data.get("gmail_thread_id", "")).strip()
        if provider and user_email and gmail_thread_id:
            _upsert_thread_lookup(provider=provider, user_email=user_email, gmail_thread_id=gmail_thread_id, thread_doc_id=doc_id)
        log_event("thread_saved", thread_doc_id=doc_id)
        return {"ok": True, "data": {"thread_doc_id": doc_id}}
    except Exception as e:
        log_event("thread_save_failed", thread_doc_id=doc_id, error=str(e))
        return {"ok": False, "error": str(e)}


def get_thread(doc_id: str) -> dict:
    try:
        snap = _threads_col().document(doc_id).get()
        if snap.exists:
            log_event("thread_fetched", thread_doc_id=doc_id)
            return {"ok": True, "data": snap.to_dict()}
        log_event("thread_not_found", thread_doc_id=doc_id)
        return {"ok": False, "error": "not_found"}
    except Exception as e:
        log_event("thread_fetch_failed", thread_doc_id=doc_id, error=str(e))
        return {"ok": False, "error": str(e)}


def delete_thread(doc_id: str) -> dict:
    try:
        provider = ""
        user_email = ""
        gmail_thread_id = ""
        parsed = _parse_thread_doc_id(doc_id)
        if parsed:
            provider, user_email = parsed
        snap = _threads_col().document(doc_id).get()
        if snap.exists:
            data = snap.to_dict() or {}
            provider = (data.get("provider") or provider).lower()
            user_email = (data.get("user_email") or user_email).lower()
            gmail_thread_id = str(data.get("gmail_thread_id", "")).strip()
        _threads_col().document(doc_id).delete()
        if provider and user_email and gmail_thread_id:
            _delete_thread_lookup(provider, user_email, gmail_thread_id)
        log_event("thread_deleted", thread_doc_id=doc_id)
        return {"ok": True}
    except Exception as e:
        log_event("thread_delete_failed", thread_doc_id=doc_id, error=str(e))
        return {"ok": False, "error": str(e)}


def create_thread(provider: str, user_email: str, data: dict) -> dict:
    try:
        doc_id = _next_thread_id(provider, user_email)
        return save_thread(doc_id, {**data, "provider": provider.lower(), "user_email": user_email.lower()})
    except Exception as e:
        log_event("thread_create_failed", error=str(e))
        return {"ok": False, "error": str(e)}


def find_thread_by_gmail_thread(provider: str, user_email: str, gmail_thread_id: str) -> dict:
    provider = provider.lower()
    user_email = user_email.lower()
    gmail_thread_id = str(gmail_thread_id).strip()
    try:
        lookup_id = _thread_lookup_doc_id(provider, user_email, gmail_thread_id)
        lookup_snap = _thread_index_col().document(lookup_id).get()
        if lookup_snap.exists:
            lookup_data = lookup_snap.to_dict() or {}
            thread_doc_id = lookup_data.get("thread_doc_id")
            if thread_doc_id:
                thread_snap = _threads_col().document(thread_doc_id).get()
                if thread_snap.exists:
                    thread_data = thread_snap.to_dict() or {}
                    return {"ok": True, "data": {"thread_doc_id": thread_doc_id, **thread_data}}
    except Exception as e:
        log_event("thread_lookup_index_read_failed", provider=provider, user_email=user_email, gmail_thread_id=gmail_thread_id, error=str(e))

    prefix = f"{provider}::{user_email}::"
    try:
        for doc in _threads_col().stream():
            if not doc.id.startswith(prefix):
                continue
            data = doc.to_dict() or {}
            if str(data.get("gmail_thread_id", "")).strip() == gmail_thread_id:
                _upsert_thread_lookup(provider=provider, user_email=user_email, gmail_thread_id=gmail_thread_id, thread_doc_id=doc.id)
                return {"ok": True, "data": {"thread_doc_id": doc.id, **data}}
        return {"ok": False, "error": "not_found"}
    except Exception as e:
        log_event("thread_find_by_gmail_thread_failed", provider=provider, user_email=user_email, gmail_thread_id=gmail_thread_id, error=str(e))
        return {"ok": False, "error": str(e)}


def mark_thread_message_processed(thread_doc_id: str, message_id: str) -> dict:
    try:
        processed_ref = (
            _threads_col().document(thread_doc_id).collection("processed_messages").document(message_id)
        )
        processed_ref.create({"message_id": message_id, "processed_at": datetime.now(timezone.utc)})
        return {"ok": True, "data": {"already_processed": False}}
    except AlreadyExists:
        return {"ok": True, "data": {"already_processed": True}}
    except Exception as e:
        log_event("thread_message_processed_mark_failed", thread_doc_id=thread_doc_id, message_id=message_id, error=str(e))
        return {"ok": False, "error": str(e)}


def find_recent_active_thread_by_recipient(provider: str, user_email: str, recipient_email: str) -> dict:
    provider = provider.lower()
    user_email = user_email.lower()
    recipient_email = recipient_email.lower().strip()
    prefix = f"{provider}::{user_email}::"
    try:
        candidates: list[tuple[str, dict]] = []
        for doc in _threads_col().stream():
            if not doc.id.startswith(prefix):
                continue
            data = doc.to_dict() or {}
            if str(data.get("recipient_email", "")).lower().strip() != recipient_email:
                continue
            if str(data.get("status", "")).lower().strip() != "active":
                continue
            if str(data.get("state", "open")).lower().strip() not in {"open", "active"}:
                continue
            candidates.append((doc.id, data))
        if len(candidates) == 1:
            doc_id, data = candidates[0]
            return {"ok": True, "data": {"thread_doc_id": doc_id, **data}}
        if len(candidates) > 1:
            return {"ok": False, "error": "ambiguous"}
        return {"ok": False, "error": "not_found"}
    except Exception as e:
        log_event("thread_find_recent_active_by_recipient_failed", provider=provider, user_email=user_email, recipient_email=recipient_email, error=str(e))
        return {"ok": False, "error": str(e)}


def find_single_active_thread_for_user(provider: str, user_email: str) -> dict:
    provider = provider.lower()
    user_email = user_email.lower()
    prefix = f"{provider}::{user_email}::"
    try:
        candidates: list[tuple[str, dict]] = []
        for doc in _threads_col().stream():
            if not doc.id.startswith(prefix):
                continue
            data = doc.to_dict() or {}
            if str(data.get("status", "")).lower().strip() != "active":
                continue
            if str(data.get("state", "open")).lower().strip() not in {"open", "active"}:
                continue
            candidates.append((doc.id, data))
        if len(candidates) == 1:
            doc_id, data = candidates[0]
            return {"ok": True, "data": {"thread_doc_id": doc_id, **data}}
        if len(candidates) > 1:
            return {"ok": False, "error": "ambiguous"}
        return {"ok": False, "error": "not_found"}
    except Exception as e:
        log_event("thread_find_single_active_for_user_failed", provider=provider, user_email=user_email, error=str(e))
        return {"ok": False, "error": str(e)}
