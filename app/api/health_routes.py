from __future__ import annotations

import os

import httpx
from fastapi import APIRouter, HTTPException

from app.logging_config import log_event

router = APIRouter()

NODE_BACKEND_URL = os.getenv("NODE_BACKEND_URL", "http://localhost:8080")


async def _call_node(path: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{NODE_BACKEND_URL}{path}")
            resp.raise_for_status()
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail=f"Cannot reach Node backend at {NODE_BACKEND_URL}")
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))


@router.get("/")
async def root():
    return {
        "service": "calendai-py-service",
        "status": "running",
        "endpoints": {
            "health": "GET /health",
            "renew_watches": "POST /renew-watches",
            "gmail_push": "POST /gmail/push",
            "outlook_push": "GET/POST /outlook/push",
            "initiate_email_flow": "POST /initiate-email-flow",
            "generate_email_draft": "POST /generate-email-draft",
            "store_tokens": "POST /store-tokens",
            "revoke_token": "POST /revoke-user-token",
            "register_fcm": "POST /register-fcm-token",
            "docs": "GET /docs",
        },
    }


@router.get("/health")
async def health():
    log_event("health_check")
    try:
        node_health = await _call_node("/health")
        node_status = node_health.get("status", "unknown")
    except HTTPException:
        node_health = {"status": "unreachable"}
        node_status = "unreachable"
    log_event("health_check_complete", python_status="ok", node_status=node_status)
    return {
        "python_service": {"status": "ok", "service": "calendai-py-service"},
        "node_service": node_health,
    }
