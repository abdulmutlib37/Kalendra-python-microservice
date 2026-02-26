"""
Firestore repository — CRUD helpers for the a2h-emailing collection.

Structure:
  a2h-emailing (top-level doc: "config")
    ├── threads        doc ID = provider::userEmail::threadID
    └── watch_state    doc ID = provider::userEmail

Every function uses structured logging with the current correlation_id
and returns a consistent dict: {"ok": bool, "data": ...} or {"ok": bool, "error": ...}.
"""

import threading
from datetime import datetime, timezone

from google.cloud import firestore as g_firestore
from google.cloud.firestore_v1 import SERVER_TIMESTAMP

from app.firestore_client import get_db
from app.logging_config import log_event

ROOT_DOC = "a2h-emailing/config"
_counter_lock = threading.Lock()


def _threads_col():
    return get_db().document(ROOT_DOC).collection("threads")


def _watch_state_col():
    return get_db().document(ROOT_DOC).collection("watch_state")


def _next_thread_id(provider: str, user_email: str) -> str:
    """Atomically increment a counter to produce a unique thread ID."""
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


def init_root_doc() -> None:
    """Ensure the a2h_emailing/config root document exists."""
    ref = get_db().document(ROOT_DOC)
    if not ref.get().exists:
        ref.set({"thread_counter": 0, "created_at": SERVER_TIMESTAMP})


# ---------------------------------------------------------------------------
# threads  (doc ID = provider::userEmail::threadID)
# ---------------------------------------------------------------------------

def save_thread(doc_id: str, data: dict) -> dict:
    try:
        doc = {
            **data,
            "created_at": data.get("created_at", SERVER_TIMESTAMP),
            "updated_at": SERVER_TIMESTAMP,
        }
        _threads_col().document(doc_id).set(doc, merge=True)
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


def create_thread(provider: str, user_email: str, data: dict) -> dict:
    """Create a new thread with an auto-incremented ID."""
    try:
        doc_id = _next_thread_id(provider, user_email)
        return save_thread(doc_id, {**data, "provider": provider.lower(), "user_email": user_email.lower()})
    except Exception as e:
        log_event("thread_create_failed", error=str(e))
        return {"ok": False, "error": str(e)}


# ---------------------------------------------------------------------------
# watch_state  (doc ID = provider::userEmail)
# ---------------------------------------------------------------------------

def _watch_doc_id(provider: str, user_email: str) -> str:
    return f"{provider.lower()}::{user_email.lower()}"


def update_watch_state(provider: str, user_email: str, data: dict) -> dict:
    doc_id = _watch_doc_id(provider, user_email)
    try:
        doc = {
            **data,
            "updated_at": SERVER_TIMESTAMP,
        }
        _watch_state_col().document(doc_id).set(doc, merge=True)
        log_event("watch_state_updated", watch_doc_id=doc_id)
        return {"ok": True, "data": {"watch_doc_id": doc_id}}
    except Exception as e:
        log_event("watch_state_update_failed", watch_doc_id=doc_id, error=str(e))
        return {"ok": False, "error": str(e)}


def get_watch_state(provider: str, user_email: str) -> dict:
    doc_id = _watch_doc_id(provider, user_email)
    try:
        snap = _watch_state_col().document(doc_id).get()
        if snap.exists:
            log_event("watch_state_fetched", watch_doc_id=doc_id)
            return {"ok": True, "data": snap.to_dict()}
        log_event("watch_state_not_found", watch_doc_id=doc_id)
        return {"ok": False, "error": "not_found"}
    except Exception as e:
        log_event("watch_state_fetch_failed", watch_doc_id=doc_id, error=str(e))
        return {"ok": False, "error": str(e)}


def stream_watch_states(provider: str | None = None):
    """Yield (doc_id, data) for all watch_state documents, optionally filtered by provider."""
    col = _watch_state_col()
    docs = col.stream()
    for doc in docs:
        if provider and not doc.id.startswith(f"{provider.lower()}::"):
            continue
        yield doc.id, doc.to_dict() or {}
