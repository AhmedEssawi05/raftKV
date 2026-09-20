#!/usr/bin/env python3
"""End-to-end demo: spins up a real 3-node raftkv cluster as separate OS
processes talking real HTTP on localhost, writes/reads through it, kills
the leader process outright, and shows the remaining two nodes elect a
new leader and keep serving writes -- with zero data loss.

Run: python3 scripts/demo.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PEERS = "n1=127.0.0.1:8001,n2=127.0.0.1:8002,n3=127.0.0.1:8003"
NODE_ADDRS = {
    "n1": "127.0.0.1:8001",
    "n2": "127.0.0.1:8002",
    "n3": "127.0.0.1:8003",
}


def http(method: str, addr: str, path: str, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"http://{addr}{path}", data=data, method=method)
    with urllib.request.urlopen(req, timeout=2) as resp:
        return json.loads(resp.read())


def status(addr: str):
    try:
        return http("GET", addr, "/status")
    except Exception as e:
        return {"error": str(e)}


def find_leader(timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        for node_id, addr in NODE_ADDRS.items():
            st = status(addr)
            if st.get("role") == "leader":
                return node_id, addr
        time.sleep(0.2)
    raise RuntimeError("no leader elected in time")


def main():
    data_dir = "/tmp/raftkv-demo-data"
    subprocess.run(["rm", "-rf", data_dir])
    os.makedirs(data_dir, exist_ok=True)

    procs = {}
    print("== starting 3-node cluster ==")
    for node_id in NODE_ADDRS:
        procs[node_id] = subprocess.Popen(
            [sys.executable, str(ROOT / "cli.py"), "serve", "--id", node_id,
             "--peers", PEERS, "--data-dir", data_dir],
            cwd=ROOT,
        )
    try:
        time.sleep(1.0)

        leader_id, leader_addr = find_leader()
        print(f"leader elected: {leader_id} ({leader_addr})")

        print("\n== writing foo=bar through the leader ==")
        result = http("PUT", leader_addr, "/kv/foo", {"value": "bar"})
        print("PUT foo=bar ->", result)

        print("\n== reading it back from the leader ==")
        # NOTE: reads are served from the leader's local state in this
        # simplified implementation (see README's "known simplifications").
        result = http("GET", leader_addr, "/kv/foo")
        print("GET foo ->", result)

        print(f"\n== killing the leader ({leader_id}, pid {procs[leader_id].pid}) ==")
        procs[leader_id].kill()
        procs[leader_id].wait()
        del procs[leader_id]

        print("waiting for the remaining nodes to elect a new leader...")
        remaining_addrs = {nid: a for nid, a in NODE_ADDRS.items() if nid != leader_id}
        deadline = time.time() + 15
        new_leader_id = None
        while time.time() < deadline:
            for nid, addr in remaining_addrs.items():
                st = status(addr)
                if st.get("role") == "leader":
                    new_leader_id = nid
                    break
            if new_leader_id:
                break
            time.sleep(0.2)
        assert new_leader_id, "failover did not happen in time"
        print(f"new leader elected: {new_leader_id}")

        print("\n== writing another key through the new leader ==")
        result = http("PUT", remaining_addrs[new_leader_id], "/kv/after_failover", {"value": 42})
        print("PUT after_failover=42 ->", result)

        result = http("GET", remaining_addrs[new_leader_id], "/kv/foo")
        print("GET foo (written before the crash) ->", result)
        assert result["value"] == "bar", "lost data across a leader failure!"

        print("\n== demo complete: no data was lost across the leader failure ==")
    finally:
        print("\nshutting down remaining nodes...")
        for p in procs.values():
            p.kill()
        for p in procs.values():
            p.wait()


if __name__ == "__main__":
    main()
