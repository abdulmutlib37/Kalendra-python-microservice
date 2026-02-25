"""
Token management for Google/Outlook OAuth tokens.

Features:
- Encrypts tokens at rest using Fernet.
- Stores encrypted tokens in Firestore collection `user_tokens`.
- Refreshes expired tokens automatically for Google and Outlook.
- Supports token revocation for disconnection flows.
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from cryptography.fernet import Fernet, InvalidToken
from google.cloud import secretmanager
from google.cloud.firestore_v1 import SERVER_TIMESTAMP

from app.firestore_client import get_db
from app.logging_config import log_event


class TokenManager:
    COLLECTION = "user_tokens"
    REFRESH_BUFFER_SECONDS = 300

    def __init__(self) -> None:
        self.db = get_db()
        self._cipher: Optional[Fernet] = None

    def _doc_id(self, user_email: str, provider: str) -> str:
        return f"{provider.lower()}::{user_email.lower()}"

    def _get_fernet_key(self) -> str:
        # Prefer direct env injection for runtime speed.
        key = os.getenv("FERNET_KEY") or os.getenv("TOKEN_ENCRYPTION_KEY")
        if key:
            return key

        # Fallback: fetch from Secret Manager.
        secret_name = os.getenv("TOKEN_ENCRYPTION_SECRET_NAME")
        if not secret_name:
            raise ValueError(
                "Missing encryption key. Set FERNET_KEY or TOKEN_ENCRYPTION_SECRET_NAME."
            )

        if secret_name.startswith("projects/"):
            resource = secret_name
        else:
            project_id = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
            if not project_id:
                # Firestore client knows the active project in Cloud Run/ADC contexts.
                project_id = getattr(self.db, "project", None)
            if not project_id:
                raise ValueError(
                    "Missing project id for Secret Manager lookup. "
                    "Set GCP_PROJECT/GOOGLE_CLOUD_PROJECT."
                )
            resource = f"projects/{project_id}/secrets/{secret_name}/versions/latest"

        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(request={"name": resource})
        return response.payload.data.decode("utf-8")

    def _cipher_instance(self) -> Fernet:
        if self._cipher is None:
            key = self._get_fernet_key()
            self._cipher = Fernet(key.encode("utf-8"))
        return self._cipher

    def _encrypt(self, value: str) -> str:
        return self._cipher_instance().encrypt(value.encode("utf-8")).decode("utf-8")

    def _decrypt(self, value: str) -> Optional[str]:
        try:
            return self._cipher_instance().decrypt(value.encode("utf-8")).decode("utf-8")
        except InvalidToken:
            log_event("token_decrypt_failed")
            return None

    def store_tokens(
        self,
        user_email: str,
        provider: str,
        access_token: str,
        refresh_token: str,
        expires_in_seconds: int = 3600,
        scope: Optional[str] = None,
    ) -> bool:
        try:
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in_seconds)
            doc = {
                "user_email": user_email.lower(),
                "provider": provider.lower(),
                "access_token_encrypted": self._encrypt(access_token),
                "refresh_token_encrypted": self._encrypt(refresh_token),
                "scope": scope,
                "status": "active",
                "expires_at": expires_at,
                "updated_at": SERVER_TIMESTAMP,
                "created_at": SERVER_TIMESTAMP,
            }
            self.db.collection(self.COLLECTION).document(
                self._doc_id(user_email, provider)
            ).set(doc, merge=True)
            log_event("token_stored", provider=provider, user_email=user_email.lower())
            return True
        except Exception as exc:
            log_event(
                "token_store_failed",
                provider=provider,
                user_email=user_email.lower(),
                error=str(exc),
            )
            return False

    def get_fresh_token(self, user_email: str, provider: str) -> Optional[str]:
        provider = provider.lower()
        doc_ref = self.db.collection(self.COLLECTION).document(self._doc_id(user_email, provider))

        try:
            snap = doc_ref.get()
            if not snap.exists:
                log_event("token_not_found", provider=provider, user_email=user_email.lower())
                return None

            data = snap.to_dict() or {}
            if data.get("status") == "revoked":
                log_event("token_revoked_state", provider=provider, user_email=user_email.lower())
                return None

            access_token = self._decrypt(data.get("access_token_encrypted", ""))
            refresh_token = self._decrypt(data.get("refresh_token_encrypted", ""))
            expires_at = data.get("expires_at")

            if not access_token or not refresh_token:
                log_event("token_decrypt_or_missing_failed", provider=provider, user_email=user_email.lower())
                return None

            now = datetime.now(timezone.utc)
            if expires_at and isinstance(expires_at, datetime):
                # Firestore timestamps may be naive in some environments; normalize to UTC.
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)

                if expires_at - timedelta(seconds=self.REFRESH_BUFFER_SECONDS) > now:
                    log_event("token_reused_not_expired", provider=provider, user_email=user_email.lower())
                    return access_token

            log_event("token_refresh_started", provider=provider, user_email=user_email.lower())
            refreshed = self._refresh_access_token(provider, refresh_token)
            if not refreshed:
                log_event("token_refresh_failed", provider=provider, user_email=user_email.lower())
                return None

            new_access = refreshed["access_token"]
            new_refresh = refreshed.get("refresh_token") or refresh_token
            expires_in = int(refreshed.get("expires_in", 3600))

            ok = self.store_tokens(
                user_email=user_email,
                provider=provider,
                access_token=new_access,
                refresh_token=new_refresh,
                expires_in_seconds=expires_in,
                scope=refreshed.get("scope"),
            )
            if not ok:
                return None

            log_event("token_refresh_success", provider=provider, user_email=user_email.lower())
            return new_access
        except Exception as exc:
            log_event(
                "token_refresh_exception",
                provider=provider,
                user_email=user_email.lower(),
                error=str(exc),
            )
            return None

    def revoke_token(self, user_email: str, provider: str) -> bool:
        provider = provider.lower()
        try:
            doc_ref = self.db.collection(self.COLLECTION).document(self._doc_id(user_email, provider))
            doc_ref.set(
                {
                    "status": "revoked",
                    "access_token_encrypted": None,
                    "refresh_token_encrypted": None,
                    "expires_at": None,
                    "revoked_at": SERVER_TIMESTAMP,
                    "updated_at": SERVER_TIMESTAMP,
                },
                merge=True,
            )
            log_event("token_revoked", provider=provider, user_email=user_email.lower())
            return True
        except Exception as exc:
            log_event(
                "token_revoke_failed",
                provider=provider,
                user_email=user_email.lower(),
                error=str(exc),
            )
            return False

    def _refresh_access_token(self, provider: str, refresh_token: str) -> Optional[dict]:
        if provider == "google":
            return self._refresh_google(refresh_token)
        if provider == "outlook":
            return self._refresh_outlook(refresh_token)
        log_event("token_refresh_provider_unsupported", provider=provider)
        return None

    def _refresh_google(self, refresh_token: str) -> Optional[dict]:
        client_id = os.getenv("GOOGLE_CLIENT_ID")
        client_secret = os.getenv("GOOGLE_CLIENT_SECRET")
        if not client_id or not client_secret:
            log_event("google_refresh_config_missing")
            return None

        data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        }
        try:
            response = httpx.post("https://oauth2.googleapis.com/token", data=data, timeout=15.0)
            if response.status_code != 200:
                log_event(
                    "google_refresh_http_error",
                    status_code=response.status_code,
                    response=response.text[:500],
                )
                return None
            return response.json()
        except Exception as exc:
            log_event("google_refresh_exception", error=str(exc))
            return None

    def _refresh_outlook(self, refresh_token: str) -> Optional[dict]:
        client_id = os.getenv("OUTLOOK_CLIENT_ID")
        client_secret = os.getenv("OUTLOOK_CLIENT_SECRET")
        tenant = os.getenv("OUTLOOK_TENANT", "common")
        scope = os.getenv(
            "OUTLOOK_SCOPE",
            "openid profile offline_access User.Read Mail.ReadWrite Mail.Send",
        )
        if not client_id or not client_secret:
            log_event("outlook_refresh_config_missing")
            return None

        url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
        data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "scope": scope,
        }
        try:
            response = httpx.post(url, data=data, timeout=15.0)
            if response.status_code != 200:
                log_event(
                    "outlook_refresh_http_error",
                    status_code=response.status_code,
                    response=response.text[:500],
                )
                return None
            return response.json()
        except Exception as exc:
            log_event("outlook_refresh_exception", error=str(exc))
            return None
