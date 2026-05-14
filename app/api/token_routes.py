from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.token_manager import TokenManager

router = APIRouter()


class TokenStoreRequest(BaseModel):
    user_email: str
    provider: str = Field(pattern="^(google|outlook)$")
    access_token: str
    refresh_token: str
    expires_in_seconds: int = 3600


class TokenActionRequest(BaseModel):
    user_email: str
    provider: str = Field(pattern="^(google|outlook)$")


def create_router(token_manager: TokenManager) -> APIRouter:
    @router.post("/store-tokens")
    async def store_tokens(payload: TokenStoreRequest):
        ok = token_manager.store_tokens(
            user_email=str(payload.user_email),
            provider=payload.provider,
            access_token=payload.access_token,
            refresh_token=payload.refresh_token,
            expires_in_seconds=payload.expires_in_seconds,
        )
        if not ok:
            raise HTTPException(status_code=500, detail="Failed to store tokens")
        return {"ok": True}

    @router.post("/revoke-user-token")
    async def revoke_user_token(payload: TokenActionRequest):
        ok = token_manager.revoke_token(
            user_email=str(payload.user_email),
            provider=payload.provider,
        )
        if not ok:
            raise HTTPException(status_code=500, detail="Failed to revoke token")
        return {"ok": True, "message": "Token revoked"}

    @router.post("/test/token-store")
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

    @router.post("/test/token-refresh")
    async def test_token_refresh(payload: TokenActionRequest):
        token = token_manager.get_fresh_token(
            user_email=str(payload.user_email),
            provider=payload.provider,
        )
        if token is None:
            return {"ok": False, "message": "Refresh failed or token not found"}
        return {"ok": True, "access_token": token}

    return router
