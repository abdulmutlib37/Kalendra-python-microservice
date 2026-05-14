from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo


def format_event_time(iso_str: str) -> str:
    if not iso_str:
        return "the agreed time"
    try:
        raw = iso_str.strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        return dt.strftime("%A, %B %d at %I:%M %p").replace(" 0", " ")
    except Exception:
        return iso_str


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def local_tzinfo(user_timezone: str | None, user_timezone_offset_minutes: int | None):
    if user_timezone:
        try:
            return ZoneInfo(user_timezone)
        except Exception:
            pass
    if user_timezone_offset_minutes is not None:
        try:
            return timezone(timedelta(minutes=int(user_timezone_offset_minutes)))
        except Exception:
            return None
    return None


def normalize_finalized_event_times(
    event_data: dict[str, Any],
    user_timezone: str | None = None,
    user_timezone_offset_minutes: int | None = None,
) -> dict[str, Any]:
    if not isinstance(event_data, dict):
        return event_data
    tz = local_tzinfo(user_timezone, user_timezone_offset_minutes)
    if not tz:
        return event_data
    out = dict(event_data)
    for key in ("startTime", "endTime"):
        raw = out.get(key)
        if not raw or not isinstance(raw, str):
            continue
        dt = parse_iso_datetime(raw)
        if not dt:
            continue
        is_utc_or_naive = (dt.tzinfo is None) or (dt.utcoffset() == timedelta(0))
        if not is_utc_or_naive:
            continue
        naive = dt.replace(tzinfo=None)
        localized = naive.replace(tzinfo=tz)
        out[key] = localized.isoformat()
    return out


def localize_dt(dt: datetime, user_timezone: str | None, user_timezone_offset_minutes: int | None) -> str:
    if user_timezone:
        try:
            return dt.astimezone(ZoneInfo(user_timezone)).strftime("%a, %b %d, %I:%M %p")
        except Exception:
            pass
    if user_timezone_offset_minutes is not None:
        try:
            local_dt = dt + timedelta(minutes=int(user_timezone_offset_minutes))
            return local_dt.strftime("%a, %b %d, %I:%M %p")
        except Exception:
            pass
    return dt.astimezone(timezone.utc).strftime("%a, %b %d, %I:%M %p UTC")
