from __future__ import annotations

from google.cloud.firestore_v1 import SERVER_TIMESTAMP

from app.firestore_client import get_db
from app.logging_config import log_event

ROOT_DOC = "a2h-emailing/config"


def _watch_state_col():
    return get_db().document(ROOT_DOC).collection("watch_state")


def _watch_doc_id(provider: str, user_email: str) -> str:
    return f"{provider.lower()}::{user_email.lower()}"


def init_root_doc() -> None:
    ref = get_db().document(ROOT_DOC)
    if not ref.get().exists:
        ref.set({"thread_counter": 0, "created_at": SERVER_TIMESTAMP})


def update_watch_state(provider: str, user_email: str, data: dict) -> dict:
    doc_id = _watch_doc_id(provider, user_email)
    try:
        _watch_state_col().document(doc_id).set({**data, "updated_at": SERVER_TIMESTAMP}, merge=True)
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
    col = _watch_state_col()
    for doc in col.stream():
        if provider and not doc.id.startswith(f"{provider.lower()}::"):
            continue
        yield doc.id, doc.to_dict() or {}
