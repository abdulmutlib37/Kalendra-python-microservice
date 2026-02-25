"""
CalendAI Python Microservice — FastAPI entry point.

Routes:
  /hello          hello world + Node backend info
  /health         combined health check (Python + Node)
  /test/init      seed sample Firestore documents
  /test/update    update the sample thread
"""

import os
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from app.logging_config import (
    get_logger,
    get_or_create_correlation_id,
    log_event,
    setup_logging,
)
from app.token_manager import TokenManager

# ── Bootstrap ────────────────────────────────────────────────────────────────

setup_logging()
logger = get_logger()

app = FastAPI(title="CalendAI Python Microservice")

NODE_BACKEND_URL = os.getenv("NODE_BACKEND_URL", "http://localhost:8080")
token_manager = TokenManager()


# ── Middleware ───────────────────────────────────────────────────────────────

@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
    """Attach a correlation_id to every request for log traceability."""
    cid = get_or_create_correlation_id(request.headers.get("X-Request-ID"))
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = cid
    return response


# ── Helpers ──────────────────────────────────────────────────────────────────

async def _call_node(path: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{NODE_BACKEND_URL}{path}")
            resp.raise_for_status()
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(
            status_code=502,
            detail=f"Cannot reach Node backend at {NODE_BACKEND_URL}",
        )
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))


class TokenStoreRequest(BaseModel):
    user_email: str
    provider: str = Field(pattern="^(google|outlook)$")
    access_token: str
    refresh_token: str
    expires_in_seconds: int = 3600


class TokenActionRequest(BaseModel):
    user_email: str
    provider: str = Field(pattern="^(google|outlook)$")


# ── Core routes ──────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {
        "service": "calendai-py-service",
        "status": "running",
        "endpoints": {
            "hello": "GET /hello",
            "health": "GET /health",
            "test_init": "GET /test/init — seed sample Firestore data",
            "test_update": "GET /test/update — update sample thread",
            "test_token_store": "POST /test/token-store — store encrypted OAuth tokens",
            "test_token_refresh": "POST /test/token-refresh — refresh Google/Outlook token",
            "revoke_user_token": "POST /revoke-user-token — revoke stored token",
            "docs": "GET /docs",
        },
    }


@app.get("/hello")
async def hello():
    log_event("hello_endpoint_hit")
    node_info = await _call_node("/")
    return {
        "message": "hello world",
        "service": "calendai-py-service",
        "node_backend": node_info,
    }


@app.get("/health")
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


# ── Test routes (Firestore CRUD) ─────────────────────────────────────────────

from app.repository import (
    get_thread,
    mark_message_processed,
    save_thread,
    update_watch_state,
)


@app.get("/test/init")
async def test_init():
    """Seed sample documents into all three Firestore collections."""
    log_event("test_init_started")

    now = datetime.now(timezone.utc).isoformat()

    thread_result = save_thread("thread_sample_001", {
        "user_id": "user_123",
        "recipient_email": "test@example.com",
        "provider": "google",
        "status": "active",
        "state": "open",
        "token_count": 0,
        "last_message_at": now,
    })

    message_result = mark_message_processed(
        provider_message_id="google_msg_001",
        thread_id="thread_sample_001",
        user_id="user_123",
    )

    watch_result = update_watch_state("user_123", {
        "email": "test@example.com",
        "provider": "google",
        "history_id": "12345",
        "watch_expiration": now,
        "expires_at": now,
        "last_sync_at": now,
    })

    log_event("test_init_complete")

    return {
        "thread": thread_result,
        "processed_message": message_result,
        "watch_state": watch_result,
    }


@app.get("/test/update")
async def test_update():
    """Update the sample thread: change status and increment token_count."""
    log_event("test_update_started")

    existing = get_thread("thread_sample_001")
    if not existing["ok"]:
        raise HTTPException(status_code=404, detail="Run /test/init first to create sample data")

    current_tokens = existing["data"].get("token_count", 0)

    result = save_thread("thread_sample_001", {
        "status": "replied",
        "token_count": current_tokens + 1,
    })

    updated = get_thread("thread_sample_001")

    log_event("test_update_complete",
              old_token_count=current_tokens,
              new_token_count=current_tokens + 1,
              new_status="replied")

    return {
        "update_result": result,
        "updated_thread": updated,
    }


@app.post("/test/token-store")
async def test_token_store(payload: TokenStoreRequest):
    """
    Stores encrypted tokens in Firestore.
    Useful for testing refresh flows with an intentionally short expiry.
    """
    ok = token_manager.store_tokens(
        user_email=str(payload.user_email),
        provider=payload.provider,
        access_token=payload.access_token,
        refresh_token=payload.refresh_token,
        expires_in_seconds=payload.expires_in_seconds,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to store tokens")
    return {"ok": True, "message": "Tokens stored (encrypted) in Firestore"}


@app.post("/test/token-refresh")
async def test_token_refresh(payload: TokenActionRequest):
    """
    Forces normal token retrieval path:
    - returns existing access token when still valid
    - refreshes and updates Firestore when expired/near expiry
    """
    token = token_manager.get_fresh_token(
        user_email=str(payload.user_email),
        provider=payload.provider,
    )
    if token is None:
        return {"ok": False, "message": "Refresh failed or token not found"}
    return {"ok": True, "access_token": token}


@app.post("/revoke-user-token")
async def revoke_user_token(payload: TokenActionRequest):
    """
    Flutter disconnection endpoint.
    Marks token record revoked and clears encrypted token fields.
    """
    ok = token_manager.revoke_token(
        user_email=str(payload.user_email),
        provider=payload.provider,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to revoke token")
    return {"ok": True, "message": "Token revoked"}


# ── Local dev runner ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    from dotenv import load_dotenv

    load_dotenv()
    port = int(os.getenv("PORT", "5000"))
    uvicorn.run("app.main:app", host="0.0.0.0", port=port, reload=True)
