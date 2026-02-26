"""
CalendAI Python Microservice — FastAPI entry point.

Firestore layout (under a2h-emailing/config):
  threads       doc ID = provider::userEmail::threadID
  watch_state   doc ID = provider::userEmail
"""

import os
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from app.logging_config import (
    get_logger,
    get_or_create_correlation_id,
    log_event,
    setup_logging,
)
from app.repository import (
    create_thread,
    get_thread,
    get_watch_state,
    init_root_doc,
    make_thread_doc_id,
    save_thread,
    stream_watch_states,
    update_watch_state,
)
from app.token_manager import TokenManager
from app.watch_service import process_gmail_push, renew_gmail_watch

# ── Bootstrap ────────────────────────────────────────────────────────────────

setup_logging()
logger = get_logger()

app = FastAPI(title="CalendAI Python Microservice")

NODE_BACKEND_URL = os.getenv("NODE_BACKEND_URL", "http://localhost:8080")
token_manager = TokenManager()


@app.on_event("startup")
async def on_startup():
    init_root_doc()


# ── Middleware ───────────────────────────────────────────────────────────────

@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
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


# ── Request models ───────────────────────────────────────────────────────────

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
        "firestore_layout": "a2h-emailing/config  →  threads | watch_state",
        "endpoints": {
            "hello": "GET /hello",
            "health": "GET /health",
            "test_init": "GET /test/init — seed sample Firestore data",
            "test_update": "GET /test/update — update sample thread",
            "renew_watches": "POST /renew-watches — renew expiring Gmail watches",
            "gmail_push": "POST /gmail/push — process Gmail push and reply 'Noted'",
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

@app.get("/test/init")
async def test_init():
    """Seed sample documents into threads and watch_state."""
    log_event("test_init_started")

    thread_doc_id = make_thread_doc_id("google", "test@example.com", 1)
    thread_result = save_thread(thread_doc_id, {
        "provider": "google",
        "user_email": "test@example.com",
        "recipient_email": "recipient@example.com",
        "status": "active",
        "state": "open",
    })

    watch_result = update_watch_state(
        provider="google",
        user_email="test@example.com",
        data={
            "history_id": "12345",
            "watch_expiration": datetime.now(timezone.utc) + timedelta(days=7),
            "expires_at": datetime.now(timezone.utc) + timedelta(days=6),
        },
    )

    log_event("test_init_complete")

    return {
        "thread": thread_result,
        "watch_state": watch_result,
    }


@app.get("/test/update")
async def test_update():
    """Update the sample thread status."""
    log_event("test_update_started")

    thread_doc_id = make_thread_doc_id("google", "test@example.com", 1)
    existing = get_thread(thread_doc_id)
    if not existing["ok"]:
        raise HTTPException(status_code=404, detail="Run /test/init first to create sample data")

    result = save_thread(thread_doc_id, {"status": "replied"})
    updated = get_thread(thread_doc_id)

    log_event("test_update_complete", new_status="replied")

    return {
        "update_result": result,
        "updated_thread": updated,
    }


# ── Token endpoints ──────────────────────────────────────────────────────────

@app.post("/test/token-store")
async def test_token_store(payload: TokenStoreRequest):
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
    token = token_manager.get_fresh_token(
        user_email=str(payload.user_email),
        provider=payload.provider,
    )
    if token is None:
        return {"ok": False, "message": "Refresh failed or token not found"}
    return {"ok": True, "access_token": token}


@app.post("/revoke-user-token")
async def revoke_user_token(payload: TokenActionRequest):
    ok = token_manager.revoke_token(
        user_email=str(payload.user_email),
        provider=payload.provider,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to revoke token")
    return {"ok": True, "message": "Token revoked"}


# ── Watch renewal ────────────────────────────────────────────────────────────

@app.post("/renew-watches")
async def renew_watches():
    """
    Cloud Scheduler target (Google-only for now).
    Finds watch_state rows expiring within 1 day and renews Gmail watches.
    """
    threshold = datetime.now(timezone.utc) + timedelta(days=1)

    renewed = 0
    failed = 0
    failures: list[dict] = []

    for doc_id, data in stream_watch_states(provider="google"):
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

        result = renew_gmail_watch(user_email=user_email, token_manager=token_manager)
        if result.get("ok"):
            renewed += 1
        else:
            failed += 1
            failures.append({"user_email": user_email, "error": result.get("error")})

    log_event("watch_renewal_batch_complete", renewed=renewed, failed=failed)
    return {"ok": True, "renewed": renewed, "failed": failed, "failures": failures}


@app.post("/gmail/push")
async def gmail_push(payload: dict):
    """
    Gmail Pub/Sub push webhook.
    For testing behavior, replies in-thread with "Noted" on new messages.
    """
    result = process_gmail_push(push_payload=payload, token_manager=token_manager)
    if not result.get("ok"):
        return {"ok": False, "error": result.get("error")}
    return result


# ── Local dev runner ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    from dotenv import load_dotenv

    load_dotenv()
    port = int(os.getenv("PORT", "5000"))
    uvicorn.run("app.main:app", host="0.0.0.0", port=port, reload=True)
