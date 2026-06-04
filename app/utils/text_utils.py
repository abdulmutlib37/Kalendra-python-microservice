from __future__ import annotations

import re


def normalize_email_body_format(body: str) -> str:
    if not body:
        return ""
    normalized = body.replace("\r\n", "\n").replace("\r", "\n")
    lines: list[str] = []
    blank_pending = False
    for raw_line in normalized.split("\n"):
        line = raw_line.strip()
        if not line:
            blank_pending = bool(lines)
            continue
        bullet_match = re.match(r"^(?:[-*]|•|\d+[.)])\s+(.*)$", line)
        if bullet_match:
            line = f"- {bullet_match.group(1).strip()}"
        if blank_pending and lines and lines[-1] != "":
            lines.append("")
        lines.append(line)
        blank_pending = False
    return "\n".join(lines).strip()


def strip_quoted_reply(text: str) -> str:
    """Remove quoted reply blocks from an email body, keeping only the new message content."""
    lines = text.splitlines()
    clean: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Any line starting with > is quoted
        if stripped.startswith(">"):
            break

        # --- or ___ separator lines
        if re.match(r"^[-_]{3,}$", stripped):
            break

        # "On <date>, <name> wrote:" — may span 1–3 lines ending with "wrote:"
        if re.match(r"^on\s+", stripped, re.IGNORECASE):
            lookahead = " ".join(lines[i:i + 4]).strip()
            if re.search(r"wrote:\s*$", lookahead, re.IGNORECASE):
                break

        # "From: " header at the start of a forwarded block (only stop if we already have content)
        if re.match(r"^from\s*:", stripped, re.IGNORECASE) and clean:
            break

        clean.append(line)
        i += 1

    return "\n".join(clean).strip()


def strip_reply_prefix(subject: str) -> str:
    return re.sub(r"^(re|fwd?)\s*:\s*", "", subject.strip(), flags=re.IGNORECASE).strip()


def strip_leading_greeting(body: str) -> str:
    lines = body.split("\n")
    while lines and not lines[0].strip():
        lines = lines[1:]
    if lines and lines[0].strip().lower().startswith(("hi ", "hello ", "dear ")):
        lines = lines[1:]
        while lines and not lines[0].strip():
            lines = lines[1:]
        return "\n".join(lines).strip() or body
    return body


def extract_subject_from_context(context: str) -> str:
    text = (context or "").strip()
    if not text:
        return "Meeting Scheduling"
    quoted = re.search(r'titled\s+"([^"]+)"', text, flags=re.IGNORECASE)
    if quoted and quoted.group(1).strip():
        return quoted.group(1).strip()
    title_word = re.search(r"\btitle(?:d)?\s+([^\n\r,.!?]+)", text, flags=re.IGNORECASE)
    if title_word and title_word.group(1).strip():
        return title_word.group(1).strip().strip(" .,:;-")
    subject_like = re.search(r"subject\s*[:=-]\s*([^\n\r]+)", text, flags=re.IGNORECASE)
    if subject_like and subject_like.group(1).strip():
        return subject_like.group(1).strip().strip(" .,:;-")
    cleaned = re.sub(r"\s+", " ", text).strip(" .,:;-")
    if not cleaned:
        return "Meeting Scheduling"
    return " ".join(cleaned.split()[:8]).strip()
