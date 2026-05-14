from __future__ import annotations

from typing import Callable

import httpx


def request_with_refresh(
    method: str,
    url: str,
    token: str,
    refresh_fn: Callable[[], str | None] | None = None,
    **kwargs,
) -> httpx.Response:
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"
    headers["Content-Type"] = "application/json"
    resp = httpx.request(method=method, url=url, headers=headers, timeout=20.0, **kwargs)
    if resp.status_code == 401 and refresh_fn:
        try:
            refreshed = (refresh_fn() or "").strip()
        except Exception:
            refreshed = ""
        if refreshed:
            headers["Authorization"] = f"Bearer {refreshed}"
            resp = httpx.request(method=method, url=url, headers=headers, timeout=20.0, **kwargs)
    return resp
