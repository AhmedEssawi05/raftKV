from raftkv.raft.node import Role
from tests.harness import Cluster


def test_single_leader_elected():
    c = Cluster(["n1", "n2", "n3"], seed=1)
    leader = c.run_until_leader()
    assert leader in c.nodes
    leaders = c.leaders()
    assert len(leaders) == 1
    # Everyone should agree on the same term for the winning leader.
    term = c.nodes[leader].current_term
    for n in c.nodes.values():
        assert n.current_term == term


def test_reelection_after_leader_partition():
    c = Cluster(["n1", "n2", "n3"], seed=2)
    old_leader = c.run_until_leader()

    # Cut the old leader off from everyone else.
    others = [n for n in c.alive_ids() if n != old_leader]
    c.partition([old_leader], others)

    # Run long enough for the majority side to time out and elect anew.
    new_leader = None
    for _ in range(300):
        c.tick_all()
        majority_leaders = [n for n in c.leaders() if n in others]
        if majority_leaders:
            new_leader = majority_leaders[0]
            break
    assert new_leader is not None
    assert new_leader != old_leader

    # The old leader should have a *higher or equal* term after healing
    # and should step down / catch up, never keep a competing leadership.
    c.heal_all()
    for _ in range(300):
        c.tick_all()
        if len(c.leaders()) == 1:
            break
    assert len(c.leaders()) == 1


def test_only_one_leader_per_term_ever():
    """Across many random runs, no term should ever have two leaders --
    this is Raft's Election Safety property."""
    for seed in range(10):
        c = Cluster(["n1", "n2", "n3", "n4", "n5"], seed=seed)
        leader_by_term = {}
        for _ in range(400):
            c.tick_all()
            for n in c.nodes.values():
                if n.role == Role.LEADER:
                    prev = leader_by_term.get(n.current_term)
                    assert prev in (None, n.id), (
                        f"seed={seed} term={n.current_term} had leaders {prev} and {n.id}"
                    )
                    leader_by_term[n.current_term] = n.id


def test_candidate_with_stale_log_cannot_win():
    """A node whose log is behind should not be able to win an election
    against nodes with a more up-to-date log (Election Restriction,
    Raft paper §5.4.1): a voter must refuse a candidate whose log is
    less up-to-date than its own."""
    from raftkv.raft.rpc import RequestVote

    c = Cluster(["n1", "n2", "n3"], seed=3)
    leader = c.run_until_leader()
    c.propose({"op": "put", "key": "k", "value": "v"})

    up_to_date_voter = [n for n in c.alive_ids() if n != leader][0]
    assert c.nodes[up_to_date_voter].log.last_index() >= 1

    # A hypothetical candidate campaigning with an empty, stale log should
    # be denied a vote by a node that already has entries.
    stale_request = RequestVote(
        term=c.nodes[up_to_date_voter].current_term + 1,
        candidate_id="ghost-candidate",
        last_log_index=0,
        last_log_term=0,
    )
    replies = c.nodes[up_to_date_voter].step(stale_request)
    assert len(replies) == 1
    reply_payload = replies[0].payload
    assert reply_payload.vote_granted is False
