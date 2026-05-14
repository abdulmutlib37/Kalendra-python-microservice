from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter

from app.logging_config import log_event
from app.repository import stream_watch_states
from app.services.gmail_service import renew_gmail_watch
from app.services.outlook_service import renew_outlook_watch
from app.token_manager import TokenManager

router = APIRouter()


def create_router(token_manager: TokenManager) -> APIRouter:
    @router.post("/renew-watches")
    async def renew_watches():
        threshold = datetime.now(timezone.utc) + timedelta(days=1)
        renewed = 0
        failed = 0
        failures: list[dict] = []

        for provider in ("google", "outlook"):
            for doc_id, data in stream_watch_states(provider=provider):
                expires_at = data.get("expires_at")
                if not expires_at:
                    continue
                if isinstance(expires_at, datetime) and expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)
                if not isinstance(expires_at, datetime) or expires_at > threshold:
                    continue

                parts = doc_id.split("::", 1)
                user_email = parts[1] if len(parts) == 2 else doc_id
                if not user_email:
                    continue

                result = (
                    renew_gmail_watch(user_email=user_email, token_manager=token_manager)
                    if provider == "google"
                    else renew_outlook_watch(user_email=user_email, token_manager=token_manager)
                )
                if result.get("ok"):
                    renewed += 1
                else:
                    failed += 1
                    failures.append({"user_email": user_email, "provider": provider, "error": result.get("error")})

        log_event("watch_renewal_batch_complete", renewed=renewed, failed=failed)
        return {"ok": True, "renewed": renewed, "failed": failed, "failures": failures}

    return router
