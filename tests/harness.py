"""A deterministic multi-node test harness.

Wires several RaftNode instances together over an InMemoryTransport and
drives them in lockstep: one call to ``tick_all()`` advances every node's
logical clock by one tick and fully delivers whatever messages that
produced, before returning. Nothing happens between calls, so tests are
100% reproducible and fast (no real sleeping).
"""
from __future__ import annotations

import random

from raftkv.raft.node import RaftNode, Role
from raftkv.raft.transport import InMemoryTransport
from raftkv.store import KVStore


class Cluster:
    def __init__(self, node_ids, seed: int = 0, election_timeout_ticks=(10, 20), heartbeat_interval_ticks=3):
        self.transport = InMemoryTransport()
        self.nodes: dict[str, RaftNode] = {}
        self.stores: dict[str, KVStore] = {}
        rng = random.Random(seed)
        for nid in node_ids:
            peers = [n for n in node_ids if n != nid]
            self.nodes[nid] = RaftNode(
                node_id=nid,
                peer_ids=peers,
                election_timeout_ticks=election_timeout_ticks,
                heartbeat_interval_ticks=heartbeat_interval_ticks,
                rng=random.Random(rng.random()),
            )
            self.stores[nid] = KVStore()

    def alive_ids(self):
        return list(self.nodes.keys())

    def tick_all(self) -> None:
        for nid, node in self.nodes.items():
            sends = node.tick()
            self._dispatch(nid, sends)
            self._apply(nid)
        self._deliver_pending()

    def _dispatch(self, src: str, sends) -> None:
        for send in sends:
            self.transport.send(src, send.dest, send.payload)

    def _deliver_pending(self) -> None:
        # Deliver messages breadth-first until quiescent for this round.
        changed = True
        while changed:
            changed = False
            for nid, node in list(self.nodes.items()):
                pending = self.transport.deliver_all(nid)
                for src, message in pending:
                    sends = node.step(message)
                    self._dispatch(nid, sends)
                    self._apply(nid)
                    changed = True

    def _apply(self, nid: str) -> None:
        node = self.nodes[nid]
        for entry in node.applied:
            self.stores[nid].apply(entry.index, entry.command or {})

    def run_ticks(self, n: int) -> None:
        for _ in range(n):
            self.tick_all()

    def leaders(self) -> list[str]:
        return [nid for nid, n in self.nodes.items() if n.role == Role.LEADER]

    def leader(self) -> str:
        ls = self.leaders()
        assert len(ls) == 1, f"expected exactly one leader, got {ls}"
        return ls[0]

    def run_until_leader(self, max_ticks: int = 200) -> str:
        for _ in range(max_ticks):
            self.tick_all()
            ls = self.leaders()
            if len(ls) == 1:
                return ls[0]
        raise AssertionError("no leader elected within max_ticks")

    def propose(self, command, max_ticks: int = 200):
        leader_id = self.run_until_leader()
        entry = self.nodes[leader_id].propose(command)
        assert entry is not None
        self._dispatch(leader_id, self.nodes[leader_id].outbox)
        self._apply(leader_id)
        for _ in range(max_ticks):
            self.tick_all()
            if all(n.commit_index >= entry.index for n in self.nodes.values() if self._reachable(leader_id, n.id)):
                break
        return entry

    def _reachable(self, a, b):
        return (a, b) not in self.transport.dropped

    def partition(self, group_a, group_b) -> None:
        for a in group_a:
            for b in group_b:
                self.transport.partition(a, b)

    def heal_all(self) -> None:
        self.transport.dropped.clear()
