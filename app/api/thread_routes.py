from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException

from app.logging_config import log_event
from app.repository import find_recent_active_thread_by_recipient
from app.repository.thread_repo import _threads_col, save_thread
from app.services.gmail_service import get_thread_messages_structured as gmail_messages
from app.services.gmail_service import compute_thread_status
from app.services.outlook_service import get_thread_messages_structured as outlook_messages
from app.token_manager import TokenManager

router = APIRouter()


def _scan_active_threads(user_email: str, recipient_email: str) -> tuple[dict | None, str | None]:
    """
    Fallback scan: find the most recently created active thread for this user
    whose recipient_email loosely matches. Used when the exact lookup misses
    (e.g. FCM payload had empty/different recipient_email before new deploy).
    """
    candidates = []
    try:
        for doc in _threads_col().stream():
            data = doc.to_dict() or {}
            if str(data.get("user_email", "")).lower() != user_email:
                continue
            if str(data.get("status", "")).lower() != "active":
                continue
            if str(data.get("state", "open")).lower() not in {"open", "active"}:
                continue
            stored_recip = str(data.get("recipient_email", "")).lower().strip()
            # Accept if recipient matches or if we have nothing better (empty recip_email fallback)
            if not recipient_email or stored_recip == recipient_email or recipient_email in stored_recip:
                try:
                    counter = int(doc.id.rsplit("::", 1)[1])
                except Exception:
                    counter = -1
                provider = str(data.get("provider", "google")).lower()
                candidates.append((counter, provider, data))
    except Exception as e:
        log_event("email_thread_scan_failed", user_email=user_email, error=str(e))
        return None, None

    if not candidates:
        return None, None

    candidates.sort(key=lambda x: x[0], reverse=True)
    _, provider_found, thread_data = candidates[0]
    return thread_data, provider_found


def create_router(token_manager: TokenManager) -> APIRouter:
    @router.get("/list-active-threads")
    async def list_active_threads(user_email: str, status_filter: str = ""):
        """
        Return scheduling threads for a given user.
        Query params:
          user_email     — the sender/account email (required)
          status_filter  — optional comma-separated list of thread_status values to include
                           (initiated, negotiating_time, scheduled, reschedule_requested,
                            meeting_executed, cancelled). Omit for all non-cancelled threads.
        """
        email = user_email.strip().lower()
        if not email:
            raise HTTPException(status_code=400, detail="user_email is required")

        requested_statuses: set[str] = set()
        if status_filter.strip():
            requested_statuses = {s.strip() for s in status_filter.split(",") if s.strip()}

        # By default only exclude cancelled; meeting_executed stays visible until the user dismisses
        default_exclude = {"cancelled"}

        results = []
        cutoff = datetime.now(timezone.utc) - timedelta(days=60)
        try:
            for doc in _threads_col().stream():
                data = doc.to_dict() or {}
                if str(data.get("user_email", "")).lower() != email:
                    continue
                raw_status = str(data.get("status", "")).lower()
                # Skip threads that have no recognised status
                if raw_status not in {"active", "completed", "cancelled"}:
                    continue
                updated = data.get("updated_at")
                if updated and hasattr(updated, "tzinfo"):
                    updated_aware = updated if updated.tzinfo else updated.replace(tzinfo=timezone.utc)
                    if updated_aware < cutoff:
                        continue

                # Recompute thread_status live so it stays accurate even without cron
                ts = compute_thread_status(data)
                # Persist if it changed (lazy update)
                if data.get("thread_status") != ts:
                    save_thread(doc.id, {"thread_status": ts})

                if requested_statuses:
                    if ts not in requested_statuses:
                        continue
                else:
                    if ts in default_exclude:
                        continue

                updated_iso = updated.isoformat() if hasattr(updated, "isoformat") else str(updated or "")
                results.append({
                    "thread_doc_id": doc.id,
                    "recipient_email": data.get("recipient_email", ""),
                    "recipient_name": data.get("recipient_name", ""),
                    "summary": data.get("summary", data.get("subject", "")),
                    "context": data.get("context", ""),
                    "status": raw_status,
                    "thread_status": ts,
                    "turn_count": data.get("turn_count", 0),
                    "updated_at": updated_iso,
                    "sender_email": email,
                    "last_message_snippet": data.get("last_message_snippet", ""),
                    "finalized_start_time": data.get("finalized_start_time", ""),
                    "finalized_summary": data.get("finalized_summary", ""),
                })
        except Exception as e:
            log_event("list_active_threads_failed", user_email=email, error=str(e))
            raise HTTPException(status_code=500, detail="Failed to list active threads")

        results.sort(key=lambda x: x["updated_at"], reverse=True)
        return {"success": True, "threads": results}

    @router.get("/email-thread")
    async def get_email_thread(
        sender_email: str,
        recipient_email: str = "",
        meeting_title: str = "",
        access_token: str = "",
        thread_doc_id: str = "",
    ):
        """
        Return the email conversation for a specific scheduling flow.

        Query params:
          sender_email    — the Kalendra account email
          thread_doc_id   — preferred: direct Firestore doc ID for exact lookup
          recipient_email — fallback if thread_doc_id not provided
          meeting_title   — optional, for logging only
        """
        user_email = sender_email.strip().lower()
        recip_email = recipient_email.strip().lower()
        doc_id = thread_doc_id.strip()

        if not user_email:
            raise HTTPException(status_code=400, detail="sender_email is required")

        log_event("email_thread_fetch_requested", sender_email=user_email, recipient_email=recip_email, thread_doc_id=doc_id)

        thread_data = None
        provider_found = None

        # Primary lookup: direct by thread_doc_id (works for any status)
        if doc_id:
            snap = _threads_col().document(doc_id).get()
            if snap.exists:
                data = snap.to_dict() or {}
                thread_data = {"thread_doc_id": doc_id, **data}
                provider_found = str(data.get("provider", "google")).lower()

        # Fallback: scan by recipient_email (active threads only)
        if not thread_data:
            log_event("email_thread_recipient_lookup_miss", sender_email=user_email, recipient_email=recip_email)
            thread_data, provider_found = _scan_active_threads(user_email, recip_email)

        if not thread_data:
            log_event("email_thread_not_found", sender_email=user_email, recipient_email=recip_email)
            raise HTTPException(status_code=404, detail="No email flow thread found for this recipient")

        messages: list[dict] = []

        if provider_found == "google":
            gmail_thread_id = str(thread_data.get("gmail_thread_id", "")).strip()
            if not gmail_thread_id:
                log_event("email_thread_missing_gmail_id", sender_email=user_email)
                raise HTTPException(status_code=500, detail="Thread found but gmail_thread_id is missing")

            # Prefer the token passed directly by the client; fall back to stored token.
            token = access_token.strip() or token_manager.get_fresh_token(user_email=user_email, provider="google")
            if not token:
                log_event("email_thread_no_token", sender_email=user_email, provider="google")
                raise HTTPException(status_code=500, detail="Could not obtain a valid Google access token")

            messages = gmail_messages(token=token, thread_id=gmail_thread_id)

        else:
            messages = outlook_messages(thread_data=thread_data)

        log_event("email_thread_fetched", sender_email=user_email, provider=provider_found, message_count=len(messages))

        return {
            "success": True,
            "thread": messages,
            "meta": {
                "provider": provider_found,
                "recipient_email": thread_data.get("recipient_email", ""),
                "recipient_name": thread_data.get("recipient_name", ""),
                "meeting_title": meeting_title,
                "status": thread_data.get("status", ""),
                "turn_count": thread_data.get("turn_count", 0),
            },
        }

    return router
