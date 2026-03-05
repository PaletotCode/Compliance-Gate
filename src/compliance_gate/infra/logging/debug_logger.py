"""
debug_logger.py — Development memory-buffer logger for the Compliance Gate frontend validation dashboard.

Provides a ring-buffer to capture structured logs (stages, rows processed) and data samples
during the CSV load and MasterMap join processes. The frontend calls /debug/logs to view these.
"""

from collections import deque
from datetime import datetime
from typing import Any, Dict, List, Optional

# Ring buffer for logs (prevent memory leak, keep last N events)
_log_buffer: deque[Dict[str, Any]] = deque(maxlen=2000)

# Buffer specifically to hold sample strings from the final join
_sample_buffer: deque[Dict[str, Any]] = deque(maxlen=50)

def add_event(stage: str, message: str, details: Optional[Dict[str, Any]] = None):
    """
    Append a structured log event to the in-memory ring buffer.
    """
    event = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "stage": stage,
        "message": message,
        "details": details or {}
    }
    _log_buffer.append(event)


def add_sample(record: Dict[str, Any]):
    """
    Keep a sample of the computed machine records (truncated for safety).
    """
    # Truncate strings to prevent massive payloads if any text field is huge
    safe_record = {}
    for k, v in record.items():
        if isinstance(v, str) and len(v) > 200:
            safe_record[k] = v[:197] + "..."
        else:
            safe_record[k] = v
            
    _sample_buffer.append(safe_record)


def get_logs(limit: int = 200) -> List[Dict[str, Any]]:
    """Return the last `limit` events from the log buffer."""
    as_list = list(_log_buffer)
    # Return the most recent ones (end of the list)
    return as_list[-limit:]


def get_samples(limit: int = 50) -> List[Dict[str, Any]]:
    """Return the parsed samples."""
    as_list = list(_sample_buffer)
    return as_list[-limit:]


def clear_buffers():
    """Clear the buffers (useful if triggering a manual reload)."""
    _log_buffer.clear()
    _sample_buffer.clear()
