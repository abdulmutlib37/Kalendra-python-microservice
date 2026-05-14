from __future__ import annotations

import os

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import PlainTextResponse

from app.logging_config import log_event
from app.services.gmail_service import process_gmail_push, verify_pubsub_auth_header
from app.services.outlook_service import process_outlook_push
from app.token_manager import TokenManager

router = APIRouter()


def create_router(token_manager: TokenManager) -> APIRouter:
    @router.post("/gmail/push")
    async def gmail_push(payload: dict, request: Request):
        require_pubsub_auth = os.getenv("REQUIRE_PUBSUB_AUTH", "false").lower() == "true"
        if require_pubsub_auth:
            expected_audience = os.getenv("PUBSUB_PUSH_AUDIENCE") or str(request.url)
            verify = verify_pubsub_auth_header(
                auth_header=request.headers.get("Authorization"),
                expected_audience=expected_audience,
            )
            if not verify.get("ok"):
                log_event("gmail_push_auth_failed", error=verify.get("error"))
                raise HTTPException(status_code=401, detail="Unauthorized Pub/Sub push")

        result = process_gmail_push(push_payload=payload, token_manager=token_manager)
        if not result.get("ok"):
            return {"ok": False, "error": result.get("error")}
        return result

    @router.get("/outlook/push")
    async def outlook_push_validation(request: Request):
        token = request.query_params.get("validationToken")
        if token:
            return PlainTextResponse(token)
        return {"ok": True}

    @router.post("/outlook/push")
    async def outlook_push(request: Request, payload: dict | None = None):
        token = request.query_params.get("validationToken")
        if token:
            return PlainTextResponse(token)
        result = process_outlook_push(push_payload=(payload or {}), token_manager=token_manager)
        if not result.get("ok"):
            return {"ok": False, "error": result.get("error")}
        return result

    return router
