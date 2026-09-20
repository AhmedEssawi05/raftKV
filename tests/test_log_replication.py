from tests.harness import Cluster


def test_command_replicates_to_all_nodes():
    c = Cluster(["n1", "n2", "n3"], seed=10)
    entry = c.propose({"op": "put", "key": "foo", "value": "bar"})
    for nid in c.alive_ids():
        assert c.stores[nid].get("foo") == "bar", f"{nid} missing the committed write"
        assert c.nodes[nid].commit_index >= entry.index


def test_multiple_commands_apply_in_order():
    c = Cluster(["n1", "n2", "n3"], seed=11)
    for i in range(10):
        c.propose({"op": "put", "key": f"k{i}", "value": i})
    for nid in c.alive_ids():
        for i in range(10):
            assert c.stores[nid].get(f"k{i}") == i


def test_minority_partition_cannot_make_progress():
    """A leader that loses contact with the majority must not be able to
    commit new entries (no split-brain writes)."""
    c = Cluster(["n1", "n2", "n3", "n4", "n5"], seed=12)
    leader = c.run_until_leader()
    others = [n for n in c.alive_ids() if n != leader]
    minority = [leader, others[0]]  # leader + 1 follower = 2 of 5, not a majority
    majority = [n for n in c.alive_ids() if n not in minority]
    c.partition(minority, majority)

    # The old leader can still *try* to propose, but it must not commit.
    entry = c.nodes[leader].propose({"op": "put", "key": "should_not_commit", "value": 1})
    assert entry is not None
    for _ in range(50):
        c.tick_all()
    assert c.nodes[leader].commit_index < entry.index
    for nid in majority:
        assert c.stores[nid].get("should_not_commit") is None

    c.heal_all()


def test_follower_log_conflict_is_repaired():
    """A follower that falls behind (or has extra uncommitted garbage in
    its log) must be brought back in sync by the leader, not left
    diverged, once connectivity is restored."""
    c = Cluster(["n1", "n2", "n3"], seed=13)
    leader = c.run_until_leader()
    lagging = [n for n in c.alive_ids() if n != leader][0]

    c.partition([lagging], [n for n in c.alive_ids() if n != lagging])
    c.propose({"op": "put", "key": "a", "value": 1})
    c.propose({"op": "put", "key": "b", "value": 2})
    assert c.stores[lagging].get("a") is None  # didn't see it yet

    c.heal_all()
    for _ in range(100):
        c.tick_all()

    assert c.stores[lagging].get("a") == 1
    assert c.stores[lagging].get("b") == 2
    # Logs across the cluster should be identical where committed.
    leader_log_len = c.nodes[leader].log.last_index()
    assert c.nodes[lagging].log.last_index() == leader_log_len
    for i in range(1, leader_log_len + 1):
        terms = {c.nodes[n].log.term_at(i) for n in c.alive_ids()}
        assert len(terms) == 1, f"log entry {i} diverged: {terms}"
