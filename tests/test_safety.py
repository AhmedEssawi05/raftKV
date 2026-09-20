"""Tests aimed squarely at Raft's core safety guarantee: once an entry is
committed (visible to a client as applied), no future leader can ever
overwrite or lose it -- the Leader Completeness property."""
from tests.harness import Cluster


def test_committed_entry_survives_leader_crash_and_reelection():
    c = Cluster(["n1", "n2", "n3", "n4", "n5"], seed=20)
    c.propose({"op": "put", "key": "durable", "value": "yes"})

    old_leader = c.leader()
    # Simulate a crash: permanently partition the old leader away from everyone.
    others = [n for n in c.alive_ids() if n != old_leader]
    c.partition([old_leader], others)

    new_leader = None
    for _ in range(300):
        c.tick_all()
        ls = [n for n in c.leaders() if n in others]
        if ls:
            new_leader = ls[0]
            break
    assert new_leader is not None

    # The committed entry must still be there under the new leader.
    assert c.stores[new_leader].get("durable") == "yes"

    # And new writes should keep working under the new leader, even while
    # the old (crashed) leader is still partitioned off and unaware.
    entry = c.nodes[new_leader].propose({"op": "put", "key": "after_failover", "value": 1})
    assert entry is not None
    for _ in range(50):
        c.tick_all()
    for nid in others:
        assert c.stores[nid].get("after_failover") == 1

    # Healing the partition should make the old leader step down and catch up.
    c.heal_all()
    for _ in range(300):
        c.tick_all()
        if len(c.leaders()) == 1:
            break
    assert len(c.leaders()) == 1
    assert c.stores[old_leader].get("after_failover") == 1


def test_new_leader_never_loses_a_committed_entry_across_many_trials():
    """Run several independent scenarios (different seeds / cluster
    sizes) of committing an entry, failing the leader over, and
    re-checking durability -- a fuzz-style safety check."""
    for seed in range(5):
        c = Cluster(["n1", "n2", "n3"], seed=100 + seed)
        c.propose({"op": "put", "key": "x", "value": seed})
        leader1 = c.leader()
        others = [n for n in c.alive_ids() if n != leader1]
        c.partition([leader1], others)
        for _ in range(300):
            c.tick_all()
            if [n for n in c.leaders() if n in others]:
                break
        for nid in others:
            assert c.stores[nid].get("x") == seed, f"seed={seed}: lost committed entry on {nid}"
        c.heal_all()


def test_commit_index_never_moves_backward():
    c = Cluster(["n1", "n2", "n3"], seed=30)
    seen = {nid: 0 for nid in c.alive_ids()}
    for i in range(20):
        c.propose({"op": "put", "key": f"k{i}", "value": i})
        for nid, node in c.nodes.items():
            assert node.commit_index >= seen[nid], "commit_index must be monotonically non-decreasing"
            seen[nid] = node.commit_index
