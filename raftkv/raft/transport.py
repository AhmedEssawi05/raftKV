"""Pluggable transports for delivering Raft messages between nodes.

``InMemoryTransport`` is a simple queue-based transport used by the unit
tests (and by a `Cluster` test harness) to simulate a network -- including
dropped and delayed messages -- without any real sockets, so tests run in
milliseconds and never flake on timing.

``HTTPTransport`` is what the real multi-process cluster uses: it POSTs
JSON to ``http://host:port/raft`` on each peer, using nothing but the
standard library so the project has zero runtime dependencies.
"""
from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from collections import defaultdict, deque
from dataclasses import asdict
from typing import Callable, Optional

from raftkv.raft.rpc import (
    AppendEntries,
    AppendEntriesReply,
    RequestVote,
    RequestVoteReply,
)

_MESSAGE_TYPES = {
    "RequestVote": RequestVote,
    "RequestVoteReply": RequestVoteReply,
    "AppendEntries": AppendEntries,
    "AppendEntriesReply": AppendEntriesReply,
}


def decode_message(payload: dict):
    kind = payload["type"]
    cls = _MESSAGE_TYPES[kind]
    fields = {k: v for k, v in payload.items() if k != "type"}
    return cls(**fields)


def encode_message(message) -> dict:
    return message.to_dict()


class InMemoryTransport:
    """A deterministic, in-process "network" connecting several nodes.

    Messages sent via ``send()`` land in a per-destination queue and are
    only delivered when the test harness calls ``deliver_all()`` or
    ``deliver_one()``. This makes race conditions reproducible: nothing
    happens except when the test says it happens.
    """

    def __init__(self):
        self._queues: dict[str, deque] = defaultdict(deque)
        self._lock = threading.Lock()
        self.dropped: set[tuple[str, str]] = set()  # (src, dst) pairs to drop
        self.sent_log: list[tuple[str, str, object]] = []

    def send(self, src: str, dest: str, message) -> None:
        if (src, dest) in self.dropped:
            return
        with self._lock:
            self._queues[dest].append((src, message))
        self.sent_log.append((src, dest, message))

    def partition(self, a: str, b: str) -> None:
        """Simulate a network partition: drop messages both ways between
        a and b until healed."""
        self.dropped.add((a, b))
        self.dropped.add((b, a))

    def heal(self, a: str, b: str) -> None:
        self.dropped.discard((a, b))
        self.dropped.discard((b, a))

    def deliver_all(self, dest: str) -> list[tuple[str, object]]:
        with self._lock:
            items = list(self._queues[dest])
            self._queues[dest].clear()
        return items

    def pending_count(self, dest: str) -> int:
        with self._lock:
            return len(self._queues[dest])


class HTTPTransport:
    """Sends Raft RPCs over HTTP to peers at known addresses.

    ``peer_addrs`` maps node id -> "host:port". Sends are fire-and-forget
    from the caller's point of view (errors are swallowed, matching
    Raft's assumption that messages can be lost); the reply, if any,
    comes back asynchronously as a normal incoming request handled by the
    server, not as this call's return value.
    """

    def __init__(self, peer_addrs: dict[str, str], timeout: float = 1.0):
        self.peer_addrs = peer_addrs
        self.timeout = timeout

    def send(self, src: str, dest: str, message) -> None:
        addr = self.peer_addrs.get(dest)
        if not addr:
            return
        body = json.dumps(encode_message(message)).encode()
        req = urllib.request.Request(
            f"http://{addr}/raft",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            urllib.request.urlopen(req, timeout=self.timeout).read()
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
            # Peer is down or unreachable -- Raft is designed to tolerate
            # this, so we just drop the message and move on.
            pass
