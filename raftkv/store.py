"""The key-value state machine applied on top of the committed Raft log.

This is intentionally simple -- a dict guarded by a lock -- because the
interesting/hard part of this project is the consensus layer, not the
data structure. Any deterministic state machine could sit here instead.
"""
from __future__ import annotations

import threading
from typing import Any, Optional


class KVStore:
    def __init__(self):
        self._data: dict[str, Any] = {}
        self._lock = threading.Lock()
        self.applied_index = 0

    def apply(self, index: int, command: dict) -> Any:
        """Apply one committed log entry's command. Must be called with
        indexes in strictly increasing order (the Raft node guarantees
        this). Returns whatever a client waiting on this entry should
        see as the result."""
        with self._lock:
            if index <= self.applied_index:
                return None  # already applied (e.g. replayed on restart)
            op = command.get("op")
            result = None
            if op == "put":
                self._data[command["key"]] = command["value"]
                result = {"ok": True}
            elif op == "delete":
                self._data.pop(command["key"], None)
                result = {"ok": True}
            elif op == "noop":
                result = {"ok": True}
            else:
                result = {"ok": False, "error": f"unknown op {op!r}"}
            self.applied_index = index
            return result

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            return self._data.get(key)

    def snapshot(self) -> dict:
        with self._lock:
            return dict(self._data)
