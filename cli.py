#!/usr/bin/env python3
"""Command-line entry point for raftkv.

Usage:
    # start a 3-node cluster (run each in its own terminal/process)
    python cli.py serve --id n1 --port 8001 --peers n1=localhost:8001,n2=localhost:8002,n3=localhost:8003 --data-dir ./data
    python cli.py serve --id n2 --port 8002 --peers n1=localhost:8001,n2=localhost:8002,n3=localhost:8003 --data-dir ./data
    python cli.py serve --id n3 --port 8003 --peers n1=localhost:8001,n2=localhost:8002,n3=localhost:8003 --data-dir ./data

    # talk to the cluster
    python cli.py put --nodes localhost:8001,localhost:8002,localhost:8003 foo bar
    python cli.py get --nodes localhost:8001,localhost:8002,localhost:8003 foo
    python cli.py status --nodes localhost:8001,localhost:8002,localhost:8003
"""
from __future__ import annotations

import argparse
import json
import sys

from raftkv.client import RaftKVClient
from raftkv.server import RaftServer


def _parse_peers(spec: str) -> dict[str, str]:
    """Parse "id1=host:port,id2=host:port,..." into a dict."""
    out = {}
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        node_id, addr = part.split("=", 1)
        out[node_id.strip()] = addr.strip()
    return out


def cmd_serve(args: argparse.Namespace) -> None:
    all_peers = _parse_peers(args.peers)
    self_addr = all_peers.pop(args.id, None)
    host = args.host
    port = args.port
    if self_addr:
        host = self_addr.split(":")[0] or host
        port = int(self_addr.split(":")[1])
    server = RaftServer(node_id=args.id, host=host, port=port, peer_addrs=all_peers, data_dir=args.data_dir)
    print(f"[{args.id}] listening on {host}:{port}, peers={list(all_peers)}", file=sys.stderr)
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()


def cmd_put(args: argparse.Namespace) -> None:
    client = RaftKVClient(args.nodes.split(","))
    result = client.put(args.key, args.value)
    print(json.dumps(result))


def cmd_get(args: argparse.Namespace) -> None:
    client = RaftKVClient(args.nodes.split(","))
    value = client.get(args.key)
    print(json.dumps({"key": args.key, "value": value}))


def cmd_delete(args: argparse.Namespace) -> None:
    client = RaftKVClient(args.nodes.split(","))
    result = client.delete(args.key)
    print(json.dumps(result))


def cmd_status(args: argparse.Namespace) -> None:
    client = RaftKVClient(args.nodes.split(","))
    for addr in args.nodes.split(","):
        try:
            print(json.dumps(client.status(addr)))
        except Exception as e:
            print(json.dumps({"addr": addr, "error": str(e)}))


def main() -> None:
    parser = argparse.ArgumentParser(prog="raftkv")
    sub = parser.add_subparsers(dest="command", required=True)

    p_serve = sub.add_parser("serve", help="run one cluster node")
    p_serve.add_argument("--id", required=True)
    p_serve.add_argument("--peers", required=True, help="id=host:port,id=host:port,...")
    p_serve.add_argument("--host", default="0.0.0.0")
    p_serve.add_argument("--port", type=int, default=8000)
    p_serve.add_argument("--data-dir", default=None, help="directory for WAL + state; omit for in-memory only")
    p_serve.set_defaults(func=cmd_serve)

    p_put = sub.add_parser("put", help="write a key")
    p_put.add_argument("--nodes", required=True, help="comma-separated host:port list")
    p_put.add_argument("key")
    p_put.add_argument("value")
    p_put.set_defaults(func=cmd_put)

    p_get = sub.add_parser("get", help="read a key")
    p_get.add_argument("--nodes", required=True)
    p_get.add_argument("key")
    p_get.set_defaults(func=cmd_get)

    p_del = sub.add_parser("delete", help="delete a key")
    p_del.add_argument("--nodes", required=True)
    p_del.add_argument("key")
    p_del.set_defaults(func=cmd_delete)

    p_status = sub.add_parser("status", help="print each node's raft status")
    p_status.add_argument("--nodes", required=True)
    p_status.set_defaults(func=cmd_status)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
