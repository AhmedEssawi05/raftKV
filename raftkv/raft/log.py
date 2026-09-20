"""The replicated log, backed by an append-only write-ahead log (WAL) on
disk so a node can recover its log and term/vote state after a crash.

Log indices are 1-based, matching the Raft paper. Index 0 is a sentinel
"nothing committed yet" entry with term 0.
"""
from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class LogEntry:
    term: int
    index: int
    command: Any  # e.g. {"op": "put", "key": ..., "value": ...}

    def to_dict(self) -> dict:
        return {"term": self.term, "index": self.index, "command": self.command}

    @staticmethod
    def from_dict(d: dict) -> "LogEntry":
        return LogEntry(term=d["term"], index=d["index"], command=d["command"])


SENTINEL = LogEntry(term=0, index=0, command=None)


class RaftLog:
    """In-memory log with an optional durable WAL.

    If ``wal_path`` is None the log is purely in-memory (used by tests
    that want deterministic, fast, filesystem-free runs). Otherwise every
    mutation is appended to the WAL file before the in-memory state is
    considered committed to disk, and the log is replayed from that file
    on construction.
    """

    def __init__(self, wal_path: Optional[str] = None):
        self._entries: list[LogEntry] = []  # 1-based; _entries[i-1] == index i
        self._lock = threading.Lock()
        self.wal_path = wal_path
        if wal_path:
            os.makedirs(os.path.dirname(wal_path) or ".", exist_ok=True)
            self._replay()

    # -- persistence -------------------------------------------------
    def _replay(self) -> None:
        if not os.path.exists(self.wal_path):
            return
        with open(self.wal_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                if record["op"] == "append":
                    self._entries.append(LogEntry.from_dict(record["entry"]))
                elif record["op"] == "truncate_from":
                    # drop every entry with index >= record["index"]
                    cutoff = record["index"]
                    self._entries = [e for e in self._entries if e.index < cutoff]

    def _wal_write(self, record: dict) -> None:
        if not self.wal_path:
            return
        with open(self.wal_path, "a") as f:
            f.write(json.dumps(record) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def compact_wal(self) -> None:
        """Rewrite the WAL to just the current in-memory entries.

        The naive append-only WAL above grows forever across truncations;
        this collapses it back down to one 'append' record per live
        entry. Safe to call any time (e.g. periodically, or on startup).
        """
        if not self.wal_path:
            return
        with self._lock:
            tmp = self.wal_path + ".compact"
            with open(tmp, "w") as f:
                for e in self._entries:
                    f.write(json.dumps({"op": "append", "entry": e.to_dict()}) + "\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.wal_path)

    # -- reads ---------------------------------------------------------
    def last_index(self) -> int:
        with self._lock:
            return self._entries[-1].index if self._entries else 0

    def last_term(self) -> int:
        with self._lock:
            return self._entries[-1].term if self._entries else 0

    def term_at(self, index: int) -> int:
        if index == 0:
            return 0
        with self._lock:
            if index < 1 or index > len(self._entries):
                return -1
            return self._entries[index - 1].term

    def get(self, index: int) -> Optional[LogEntry]:
        with self._lock:
            if index < 1 or index > len(self._entries):
                return None
            return self._entries[index - 1]

    def entries_from(self, start_index: int) -> list[LogEntry]:
        """All entries with index >= start_index, in order."""
        with self._lock:
            if start_index < 1:
                start_index = 1
            return list(self._entries[start_index - 1 :])

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    # -- writes ----------------------------------------------------------
    def append(self, term: int, command: Any) -> LogEntry:
        with self._lock:
            index = (self._entries[-1].index if self._entries else 0) + 1
            entry = LogEntry(term=term, index=index, command=command)
            self._entries.append(entry)
        self._wal_write({"op": "append", "entry": entry.to_dict()})
        return entry

    def append_entry(self, entry: LogEntry) -> None:
        """Append a fully-formed entry (used when replicating a leader's
        entries onto a follower)."""
        with self._lock:
            self._entries.append(entry)
        self._wal_write({"op": "append", "entry": entry.to_dict()})

    def truncate_from(self, index: int) -> None:
        """Delete every entry with index >= ``index`` (used when a
        follower's log conflicts with the leader's)."""
        with self._lock:
            self._entries = [e for e in self._entries if e.index < index]
        self._wal_write({"op": "truncate_from", "index": index})
