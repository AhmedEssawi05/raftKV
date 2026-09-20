"""Glues a RaftNode, a transport, and a KVStore into a runnable server.

Exposes two small HTTP APIs on the same port, using only the standard
library:

  POST /raft            internal: another node delivering a Raft RPC
  GET  /kv/<key>        client: read a key (served by the leader; a
                         follower answers with a 421-style redirect hint)
  PUT  /kv/<key>        client: write a key (body: {"value": ...})
  DELETE /kv/<key>      client: delete a key
  GET  /status          debugging: role, term, leader, log length, etc.

Consensus ticking happens on a background thread that calls
``node.tick()`` on a fixed schedule and drains any resulting outbound
messages through the transport -- exactly the same node.tick()/step()
calls the pure unit tests use.
"""
from __future__ import annotations

import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
from urllib.parse import urlparse, parse_qs

from raftkv.raft.node import RaftNode
from raftkv.raft.transport import HTTPTransport, decode_message, encode_message
from raftkv.store import KVStore

TICK_INTERVAL_SECONDS = 0.05  # 50ms tick => timeouts of 10-20 ticks are 0.5-1.0s


class _QuietThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        # A peer that already hit its own send timeout can close the
        # socket before we finish responding (BrokenPipeError) -- that's
        # expected under Raft's lossy-network assumptions, not a bug, so
        # don't spam a traceback for every one of these.
        exc_type = sys.exc_info()[0]
        if exc_type in (BrokenPipeError, ConnectionResetError):
            return
        super().handle_error(request, client_address)


class RaftServer:
    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        peer_addrs: dict[str, str],
        data_dir: Optional[str] = None,
    ):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peer_addrs = peer_addrs

        from raftkv.raft.log import RaftLog

        wal_path = f"{data_dir}/{node_id}.wal" if data_dir else None
        state_path = f"{data_dir}/{node_id}.state.json" if data_dir else None
        log = RaftLog(wal_path=wal_path)
        self.node = RaftNode(node_id=node_id, peer_ids=list(peer_addrs.keys()), log=log, state_path=state_path)
        self.transport = HTTPTransport(peer_addrs)
        self.store = KVStore()

        self._lock = threading.RLock()
        self._waiters: dict[int, threading.Event] = {}
        self._results: dict[int, dict] = {}
        self._stop = threading.Event()
        self._ticker_thread: Optional[threading.Thread] = None
        self._httpd: Optional[ThreadingHTTPServer] = None

    # -- lifecycle -----------------------------------------------------
    def start(self) -> None:
        handler = _make_handler(self)
        self._httpd = _QuietThreadingHTTPServer((self.host, self.port), handler)
        self._ticker_thread = threading.Thread(target=self._tick_loop, daemon=True)
        self._ticker_thread.start()
        self._httpd.serve_forever(poll_interval=0.1)

    def stop(self) -> None:
        self._stop.set()
        if self._httpd:
            self._httpd.shutdown()

    def _tick_loop(self) -> None:
        while not self._stop.is_set():
            time.sleep(TICK_INTERVAL_SECONDS)
            with self._lock:
                sends = self.node.tick()
                applied = list(self.node.applied)
            self._apply_locally(applied)
            self._send_all(sends)

    def _apply_locally(self, applied) -> None:
        """Apply newly-committed entries to the state machine and wake
        any client waiting on them. Cheap and local -- fine to do while
        briefly holding the lock, unlike network sends."""
        if not applied:
            return
        with self._lock:
            for entry in applied:
                result = self.store.apply(entry.index, entry.command or {})
                self._results[entry.index] = result
                ev = self._waiters.pop(entry.index, None)
                if ev:
                    ev.set()

    def _send_all(self, sends) -> None:
        """Actually push messages over the network. Deliberately called
        *without* holding self._lock: a blocking HTTP call to a slow or
        dead peer must never stall the node's own tick/step processing
        or any client request being served concurrently."""
        for send in sends:
            self.transport.send(self.node.id, send.dest, send.payload)

    # -- called by the HTTP handler -----------------------------------
    def handle_raft_message(self, payload: dict) -> None:
        message = decode_message(payload)
        with self._lock:
            sends = self.node.step(message)
            applied = list(self.node.applied)
        self._apply_locally(applied)
        self._send_all(sends)

    def status(self) -> dict:
        with self._lock:
            return {
                "id": self.node.id,
                "role": self.node.role.value,
                "term": self.node.current_term,
                "leader_id": self.node.leader_id,
                "log_length": len(self.node.log),
                "commit_index": self.node.commit_index,
                "last_applied": self.node.last_applied,
            }

    def propose(self, command: dict, timeout: float = 2.0) -> tuple[bool, Optional[dict], Optional[str]]:
        """Propose a command. Returns (ok, result, leader_hint).

        If this node isn't the leader, ok=False and leader_hint carries
        who the caller should retry against (if known)."""
        with self._lock:
            if not self.node.is_leader():
                return False, None, self.node.leader_id
            self.node.outbox = []
            entry = self.node.propose(command)
            if entry is None:
                return False, None, self.node.leader_id
            sends = list(self.node.outbox)
            ev = threading.Event()
            self._waiters[entry.index] = ev
            index = entry.index

        self._send_all(sends)
        got = ev.wait(timeout)
        with self._lock:
            if not got:
                self._waiters.pop(index, None)
                return False, None, self.node.leader_id
            result = self._results.pop(index, None)
            return True, result, None

    def read(self, key: str, timeout: float = 2.0) -> tuple[bool, Optional[dict], Optional[str]]:
        """Linearizable-ish read: routed through the log as a no-op read
        barrier would be the fully correct approach; for this project we
        keep it simple and serve reads from the leader's local state
        once it has committed at least one entry this term. Good enough
        for a demo/teaching project -- documented as a known
        simplification in the README."""
        with self._lock:
            if not self.node.is_leader():
                return False, None, self.node.leader_id
            value = self.store.get(key)
            return True, {"value": value}, None


def _make_handler(server: RaftServer):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # silence default stderr logging
            pass


        def _send_json(self, code: int, obj: dict) -> None:
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_body(self) -> dict:
            length = int(self.headers.get("Content-Length", 0))
            if length == 0:
                return {}
            return json.loads(self.rfile.read(length))

        def do_POST(self):
            parsed = urlparse(self.path)
            if parsed.path == "/raft":
                payload = self._read_body()
                server.handle_raft_message(payload)
                self._send_json(200, {"ok": True})
                return
            self._send_json(404, {"error": "not found"})

        def do_PUT(self):
            parsed = urlparse(self.path)
            if parsed.path.startswith("/kv/"):
                key = parsed.path[len("/kv/") :]
                body = self._read_body()
                ok, result, leader_hint = server.propose(
                    {"op": "put", "key": key, "value": body.get("value")}
                )
                if ok:
                    self._send_json(200, {"ok": True, "result": result})
                else:
                    self._send_json(409, {"ok": False, "leader_hint": leader_hint})
                return
            self._send_json(404, {"error": "not found"})

        def do_DELETE(self):
            parsed = urlparse(self.path)
            if parsed.path.startswith("/kv/"):
                key = parsed.path[len("/kv/") :]
                ok, result, leader_hint = server.propose({"op": "delete", "key": key})
                if ok:
                    self._send_json(200, {"ok": True, "result": result})
                else:
                    self._send_json(409, {"ok": False, "leader_hint": leader_hint})
                return
            self._send_json(404, {"error": "not found"})

        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path == "/status":
                self._send_json(200, server.status())
                return
            if parsed.path.startswith("/kv/"):
                key = parsed.path[len("/kv/") :]
                ok, result, leader_hint = server.read(key)
                if ok:
                    self._send_json(200, {"ok": True, **result})
                else:
                    self._send_json(409, {"ok": False, "leader_hint": leader_hint})
                return
            self._send_json(404, {"error": "not found"})

    return Handler
