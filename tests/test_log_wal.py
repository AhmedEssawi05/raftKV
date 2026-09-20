"""Tests for the write-ahead log: durability and crash recovery."""
import os

from raftkv.raft.log import RaftLog
from raftkv.raft.node import RaftNode


def test_in_memory_log_basic_ops():
    log = RaftLog()
    e1 = log.append(term=1, command={"op": "put", "key": "a", "value": 1})
    e2 = log.append(term=1, command={"op": "put", "key": "b", "value": 2})
    assert e1.index == 1 and e2.index == 2
    assert log.last_index() == 2
    assert log.last_term() == 1
    assert log.term_at(1) == 1
    assert log.term_at(0) == 0
    assert log.term_at(99) == -1


def test_truncate_from_drops_suffix():
    log = RaftLog()
    for i in range(5):
        log.append(term=1, command=i)
    log.truncate_from(3)
    assert log.last_index() == 2
    assert log.get(3) is None


def test_wal_replay_recovers_log_after_restart(tmp_path):
    wal_path = str(tmp_path / "n1.wal")
    log1 = RaftLog(wal_path=wal_path)
    log1.append(term=1, command={"op": "put", "key": "a", "value": 1})
    log1.append(term=1, command={"op": "put", "key": "b", "value": 2})
    log1.append(term=2, command={"op": "put", "key": "c", "value": 3})

    # Simulate a crash + restart: build a fresh RaftLog over the same file.
    log2 = RaftLog(wal_path=wal_path)
    assert log2.last_index() == 3
    assert log2.term_at(3) == 2
    assert log2.get(2).command == {"op": "put", "key": "b", "value": 2}


def test_wal_replay_honors_truncation(tmp_path):
    wal_path = str(tmp_path / "n1.wal")
    log1 = RaftLog(wal_path=wal_path)
    for i in range(5):
        log1.append(term=1, command=i)
    log1.truncate_from(3)  # drop indices 3,4,5

    log2 = RaftLog(wal_path=wal_path)
    assert log2.last_index() == 2


def test_node_persists_term_and_vote_across_restart(tmp_path):
    state_path = str(tmp_path / "n1.state.json")
    node1 = RaftNode(node_id="n1", peer_ids=["n2", "n3"], state_path=state_path)
    node1._become_candidate()
    term_after_election = node1.current_term
    assert node1.voted_for == "n1"

    node2 = RaftNode(node_id="n1", peer_ids=["n2", "n3"], state_path=state_path)
    assert node2.current_term == term_after_election
    assert node2.voted_for == "n1"


def test_compact_wal_keeps_log_semantically_equal(tmp_path):
    wal_path = str(tmp_path / "n1.wal")
    log1 = RaftLog(wal_path=wal_path)
    for i in range(5):
        log1.append(term=1, command=i)
    log1.truncate_from(4)
    log1.compact_wal()

    log2 = RaftLog(wal_path=wal_path)
    assert log2.last_index() == 3
    assert [e.command for e in log2.entries_from(1)] == [0, 1, 2]
