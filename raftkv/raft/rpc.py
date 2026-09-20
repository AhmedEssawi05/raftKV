"""Wire messages for the Raft protocol (Figure 2 of the Raft paper).

Every message is a plain, JSON-serializable dataclass. Keeping them dumb
data (no behavior) is what lets the same messages flow through an
in-memory queue in tests and through real HTTP requests in production.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class LogEntryWire:
    """Wire form of a log entry (see raftkv.raft.log.LogEntry)."""

    term: int
    index: int
    command: Any


@dataclass
class RequestVote:
    """Sent by a candidate to gather votes."""

    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int

    def to_dict(self) -> dict:
        return {"type": "RequestVote", **asdict(self)}


@dataclass
class RequestVoteReply:
    term: int
    vote_granted: bool
    voter_id: str

    def to_dict(self) -> dict:
        return {"type": "RequestVoteReply", **asdict(self)}


@dataclass
class AppendEntries:
    """Sent by the leader; also doubles as the heartbeat when
    ``entries`` is empty."""

    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list = field(default_factory=list)  # list[LogEntryWire]
    leader_commit: int = 0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["type"] = "AppendEntries"
        return d


@dataclass
class AppendEntriesReply:
    term: int
    success: bool
    follower_id: str
    # Fast backtracking of next_index on conflict (Raft paper §5.3 optimization).
    conflict_index: int = -1
    conflict_term: int = -1
    match_index: int = -1

    def to_dict(self) -> dict:
        return {"type": "AppendEntriesReply", **asdict(self)}


# A "Message" as routed by a Transport: (recipient node id, payload).
Message = tuple


def envelope(dest: str, payload) -> tuple[str, Any]:
    return (dest, payload)
