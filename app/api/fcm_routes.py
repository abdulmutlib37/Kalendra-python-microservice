from __future__ import annotations

import os
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.firestore_client import get_db
from app.logging_config import log_event

router = APIRouter()


class RegisterFcmTokenRequest(BaseModel):
    user_email: str
    fcm_token: str


@router.post("/register-fcm-token")
async def register_fcm_token(payload: RegisterFcmTokenRequest):
    user_email = payload.user_email.strip().lower()
    fcm_token = payload.fcm_token.strip()
    if not user_email or not fcm_token:
        raise HTTPException(status_code=400, detail="user_email and fcm_token are required")
    try:
        get_db().collection(os.getenv("FCM_TOKENS_COLLECTION", "fcm_tokens")).document(user_email).set(
            {"fcm_token": fcm_token, "updated_at": datetime.now(timezone.utc)},
            merge=True,
        )
        log_event("fcm_token_registered", user_email=user_email)
        return {"ok": True}
    except Exception as exc:
        log_event("fcm_token_register_failed", user_email=user_email, error=str(exc))
        raise HTTPException(status_code=500, detail="Failed to store FCM token")
