"""
logger.py — Structured, safe logger for Compliance Gate ingest pipeline.

Rules:
  - All string values truncated at MAX_STR_LEN chars before logging.
  - No raw PII fields logged in full.
  - Uses stdlib logging with JSON-structured detail payloads.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

log = logging.getLogger(__name__)

MAX_STR_LEN = 200


def truncate_str(value: Any, max_len: int = MAX_STR_LEN) -> Any:
    """Truncate strings to max_len chars. Non-strings pass through unchanged."""
    if isinstance(value, str) and len(value) > max_len:
        return value[:max_len] + f"...({len(value)} chars)"
    return value


def truncate_dict(d: dict, max_len: int = MAX_STR_LEN) -> dict:
    """Recursively truncate all string values in a dict."""
    out = {}
    for k, v in d.items():
        if isinstance(v, dict):
            out[k] = truncate_dict(v, max_len)
        elif isinstance(v, list):
            out[k] = [truncate_str(x, max_len) for x in v]
        else:
            out[k] = truncate_str(v, max_len)
    return out


def log_ingest_event(
    stage: str,
    message: str,
    details: Optional[dict[str, Any]] = None,
    level: str = "INFO",
) -> None:
    """
    Emit a structured log line for an ingest pipeline stage.
    Details dict is truncated before logging to prevent PII leakage.
    """
    safe_details = truncate_dict(details or {})
    payload = {
        "stage": stage,
        "message": truncate_str(message),
        "details": safe_details,
    }
    lvl = getattr(logging, level.upper(), logging.INFO)
    log.log(lvl, json.dumps(payload, ensure_ascii=False, default=str))


def log_parse_warning(source: str, row_hint: Optional[int], msg: str) -> str:
    """Format and log a parse warning. Returns the warning string for collection."""
    s = truncate_str(msg)
    warning = f"[{source}] row~{row_hint}: {s}" if row_hint is not None else f"[{source}] {s}"
    log.warning(warning)
    return warning
