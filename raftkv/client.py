"""A tiny client for talking to a raftkv cluster.

Handles the one bit of protocol the caller shouldn't have to think
about: if you ask a follower to write, it tells you who the leader is
(or might be), and this client retries against that node -- falling
back to trying every known node if it doesn't know.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Optional


class RaftKVClient:
    def __init__(self, addrs: list[str], timeout: float = 2.0, retries: int = 5):
        self.addrs = list(addrs)
        self.timeout = timeout
        self.retries = retries
        self._last_leader: Optional[str] = addrs[0] if addrs else None

    def _request(self, method: str, addr: str, path: str, body: Optional[dict] = None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"http://{addr}{path}",
            data=data,
            headers={"Content-Type": "application/json"} if data else {},
            method=method,
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return resp.status, json.loads(resp.read())

    def _try_all(self, method: str, path: str, body: Optional[dict] = None) -> dict:
        order = ([self._last_leader] if self._last_leader else []) + [
            a for a in self.addrs if a != self._last_leader
        ]
        last_error: Optional[Exception] = None
        for _ in range(self.retries):
            for addr in order:
                try:
                    status, resp = self._request(method, addr, path, body)
                except (urllib.error.URLError, ConnectionError, TimeoutError, OSError) as e:
                    last_error = e
                    continue
                except urllib.error.HTTPError as e:
                    try:
                        resp = json.loads(e.read())
                    except Exception:
                        resp = {}
                    status = e.code
                if status == 200 and resp.get("ok"):
                    self._last_leader = addr
                    return resp
                hint = resp.get("leader_hint") if isinstance(resp, dict) else None
                if hint and hint in self.addrs_by_id:
                    order = [self.addrs_by_id[hint]] + order
        raise ConnectionError(f"no reachable leader among {self.addrs}: {last_error}")

    # id->addr map is optional; when unset, leader hints (node ids) can't
    # be turned back into addresses, so the client just round-robins.
    addrs_by_id: dict = {}

    def put(self, key: str, value: Any) -> dict:
        return self._try_all("PUT", f"/kv/{key}", {"value": value})

    def get(self, key: str) -> Any:
        resp = self._try_all("GET", f"/kv/{key}")
        return resp.get("value")

    def delete(self, key: str) -> dict:
        return self._try_all("DELETE", f"/kv/{key}")

    def status(self, addr: str) -> dict:
        _, resp = self._request("GET", addr, "/status")
        return resp
