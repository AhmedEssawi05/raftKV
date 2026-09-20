# raftkv

A small, from-scratch implementation of the **Raft** consensus protocol,
backing a replicated key-value store. No consensus libraries, no
frameworks — just the standard library, built to understand (and prove)
how distributed systems stay correct when nodes crash, restart, or lose
touch with each other mid-write.

Built as a learning/portfolio project because I enjoy building things
from first principles rather than gluing libraries together.

## What's actually implemented

This isn't a toy that only works on the happy path — it implements the
parts of Raft that make it *safe*, not just the parts that make it work
when nothing goes wrong:

- **Leader election** with randomized timeouts, term numbers, and the
  majority-vote rule.
- **Log replication** with the `prevLogIndex`/`prevLogTerm` consistency
  check, conflicting-entry truncation, and the §5.3 fast-backtrack
  optimization (a follower tells the leader which whole term to skip
  back over, instead of retrying one index at a time).
- **The election restriction** (§5.4.1): a node refuses to vote for a
  candidate whose log is less up-to-date than its own, so a partition
  that fell behind can never win an election and roll back committed
  data.
- **The leader commit rule** (§5.4.2): a leader only commits an entry
  once it's replicated on a majority *and* was created in the leader's
  own current term — the subtle rule that keeps an old, uncommitted
  entry from silently becoming "committed" via a future leader.
- **Crash recovery**: current term, vote, and the full log are persisted
  to a write-ahead log on disk and replayed on restart, so a restarted
  node can't accidentally vote twice in a term it already voted in.
- **A real transport**: nodes talk over plain HTTP (stdlib only, zero
  runtime dependencies) so you can actually run a 3-node cluster as
  three OS processes, kill one, and watch the other two fail over.

## Architecture

```
                     ┌─────────────┐
   client  ───────▶  │  RaftServer │  (HTTP: /kv/*, /status, /raft)
  (cli.py /          │             │
   client.py)        │  ┌────────┐ │        ┌────────┐
                      │  │RaftNode│◀┼───────▶│ peer 2 │
                      │  │(pure   │ │  HTTP  └────────┘
                      │  │ FSM)   │ │        ┌────────┐
                      │  └───┬────┘ │◀──────▶│ peer 3 │
                      │      │      │        └────────┘
                      │      ▼      │
                      │  ┌────────┐ │
                      │  │ KVStore│ │  (applied once committed)
                      │  └────────┘ │
                      └─────────────┘
                        │        │
                        ▼        ▼
                   n1.wal   n1.state.json   (WAL + persisted term/vote)
```

The key design choice: **`raftkv/raft/node.py` contains zero I/O.** It's
a pure state machine — `tick()` advances logical time, `step(message)`
handles one incoming RPC, and both return plain data (messages to send,
entries newly safe to apply). That's what makes it possible to unit-test
leader election and log replication *deterministically*: the tests drive
dozens of election/failure scenarios in milliseconds, with simulated
network partitions, and there is nothing timing-dependent to flake.

```
raftkv/
  raft/
    node.py        the Raft state machine itself (pure, no I/O)
    log.py         replicated log + write-ahead log persistence
    rpc.py         RequestVote / AppendEntries message types
    transport.py   InMemoryTransport (tests) + HTTPTransport (real cluster)
  store.py         the key-value state machine
  server.py        wires node + transport + store into an HTTP server
  client.py        client library (follows leader redirects)
cli.py             `raftkv serve|put|get|delete|status`
tests/
  harness.py           deterministic multi-node test cluster
  test_election.py     leader election + the election-safety property
  test_log_replication.py   replication, conflict repair, partitions
  test_safety.py       leader failover never loses committed data
  test_log_wal.py       crash recovery via the write-ahead log
scripts/demo.py    real 3-process cluster demo, including a live failover
```

## Try it

```bash
pip install -e ".[dev]"
pytest -v                 # 22 tests, runs in well under a second
python scripts/demo.py    # spins up a real 3-node cluster, kills the
                           # leader mid-flight, shows the failover live
```

Sample output from `scripts/demo.py`:

```
== starting 3-node cluster ==
leader elected: n3 (127.0.0.1:8003)

== writing foo=bar through the leader ==
PUT foo=bar -> {'ok': True, 'result': {'ok': True}}

== killing the leader (n3, pid 889) ==
waiting for the remaining nodes to elect a new leader...
new leader elected: n2

== writing another key through the new leader ==
PUT after_failover=42 -> {'ok': True, 'result': {'ok': True}}
GET foo (written before the crash) -> {'ok': True, 'value': 'bar'}

== demo complete: no data was lost across the leader failure ==
```

### Running your own cluster by hand

```bash
PEERS="n1=localhost:8001,n2=localhost:8002,n3=localhost:8003"
python cli.py serve --id n1 --peers $PEERS --data-dir ./data &
python cli.py serve --id n2 --peers $PEERS --data-dir ./data &
python cli.py serve --id n3 --peers $PEERS --data-dir ./data &

python cli.py put    --nodes localhost:8001,localhost:8002,localhost:8003 foo bar
python cli.py get    --nodes localhost:8001,localhost:8002,localhost:8003 foo
python cli.py status --nodes localhost:8001,localhost:8002,localhost:8003
```

## Testing philosophy

The hard part of Raft isn't the happy path, it's the edge cases: what
happens when a leader is partitioned away but doesn't know it yet? What
happens when a follower's log has uncommitted garbage from a
since-deposed leader? `tests/harness.py` builds a small in-memory
"network" so these scenarios can be driven directly:

```python
c = Cluster(["n1", "n2", "n3", "n4", "n5"])
c.propose({"op": "put", "key": "durable", "value": "yes"})

leader = c.leader()
c.partition([leader], [n for n in c.alive_ids() if n != leader])  # simulate a crash
# ... run ticks until the majority side elects a new leader ...
# the committed entry is still there, and new writes still work
```

`test_election.py::test_only_one_leader_per_term_ever` runs this kind of
scenario across many random seeds and asserts that Raft's **Election
Safety** property — at most one leader per term, ever — never breaks.

## Known simplifications

This is a teaching/portfolio-scale implementation, and a few corners are
cut on purpose, called out here rather than hidden:

- **Reads** are served from the leader's local state rather than going
  through a proper read-index/lease-read barrier, so a *very* recently
  displaced leader could in theory serve one stale read before it
  notices it lost leadership. Writes are fully linearizable; this is a
  read-side simplification.
- **No log compaction/snapshotting** — the WAL grows forever (a
  `compact_wal()` helper exists for the truncated-entries case but isn't
  wired into a snapshotting policy). Fine for a demo; a production
  system would snapshot the state machine and truncate the log.
- **No cluster membership changes** (adding/removing nodes) — the peer
  set is fixed at startup.

## Why this project

I wanted something on my resume that isn't another CRUD app — a project
where correctness is genuinely hard to get right, where "it works on my
machine" isn't good enough, and where the tests have to work harder than
the code they're testing. Raft is a great fit: the algorithm is famous
for being designed specifically to be *understandable* (that's the title
of the original paper), which makes it a good way to demonstrate real
distributed-systems fundamentals without needing a PhD's worth of
background to explain what's going on.

Reference: Diego Ongaro and John Ousterhout, ["In Search of an
Understandable Consensus Algorithm"](https://raft.github.io/raft.pdf)
(the Raft paper).
