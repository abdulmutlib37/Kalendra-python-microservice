from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException
from pydantic import BaseModel

from app.logging_config import log_event
from app.services.agent_service import generate_initial_email
from app.services.gmail_service import initiate_google_email_flow
from app.services.outlook_service import initiate_outlook_email_flow
from app.token_manager import TokenManager

router = APIRouter()


class InitiateEmailFlowRequest(BaseModel):
    provider: str | None = "google"
    access_token: str | None = None
    google_access_token: str | None = None
    outlook_access_token: str | None = None
    sender_name: str
    recipient_email: str
    recipient_name: str | None = None
    context: str | None = None
    email_subject: str | None = None
    email_body: str | None = None
    user_timezone: str | None = None
    user_timezone_offset_minutes: int | None = None


def create_router(token_manager: TokenManager) -> APIRouter:
    @router.post("/initiate-email-flow")
    async def initiate_email_flow(payload: InitiateEmailFlowRequest):
        provider = (payload.provider or "google").lower().strip()
        if provider == "google":
            has_google = bool((payload.access_token or payload.google_access_token or "").strip())
            has_outlook = bool((payload.outlook_access_token or "").strip())
            if not has_google and has_outlook:
                provider = "outlook"

        if provider not in {"google", "outlook"}:
            raise HTTPException(status_code=400, detail="provider must be google or outlook")

        if provider == "google":
            token = (payload.access_token or payload.google_access_token or "").strip()
            result = initiate_google_email_flow(
                google_access_token=token,
                sender_name=payload.sender_name,
                recipient_email=payload.recipient_email,
                recipient_name=payload.recipient_name,
                context=payload.context or "",
                email_subject=payload.email_subject,
                email_body=payload.email_body,
                user_timezone=(payload.user_timezone or "").strip() or None,
                user_timezone_offset_minutes=payload.user_timezone_offset_minutes,
                token_manager=token_manager,
            )
        else:
            token = (payload.access_token or payload.outlook_access_token or "").strip()
            result = initiate_outlook_email_flow(
                outlook_access_token=token,
                sender_name=payload.sender_name,
                recipient_email=payload.recipient_email,
                recipient_name=payload.recipient_name,
                context=payload.context or "",
                email_subject=payload.email_subject,
                email_body=payload.email_body,
                user_timezone=(payload.user_timezone or "").strip() or None,
                user_timezone_offset_minutes=payload.user_timezone_offset_minutes,
                token_manager=token_manager,
            )

        if not result.get("ok"):
            status_code = int(result.get("status_code") or 500)
            raise HTTPException(status_code=status_code, detail=result.get("error", "initiate_email_flow_failed"))

        data = result.get("data", {})
        return {"status": data.get("status", "ok"), "thread_id": data.get("thread_id"), "watch_ok": data.get("watch_ok", False)}

    @router.post("/generate-email-draft")
    async def generate_email_draft(
        sender_name: str = Body(...),
        recipient_name: str = Body(...),
        context: str = Body(...),
        preferred_subject: str | None = Body(default=None),
    ):
        try:
            subject, body = generate_initial_email(
                sender_name=sender_name,
                recipient_name=recipient_name or "there",
                context=context or "schedule a meeting",
                preferred_subject=preferred_subject,
            )
            return {"subject": subject, "body": body}
        except Exception as exc:
            log_event("email_draft_generation_failed", error=str(exc))
            raise HTTPException(status_code=500, detail=f"Failed to generate email draft: {str(exc)}")

    return router
