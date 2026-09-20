"""raftkv: a small, from-scratch implementation of the Raft consensus
protocol backing a replicated key-value store.

The package is split so the consensus logic can be tested without any
real networking or timers:

- ``raftkv.raft.rpc``       message types exchanged between nodes
- ``raftkv.raft.log``       the replicated log (with an on-disk WAL)
- ``raftkv.raft.node``      the Raft state machine itself (pure, driven by
                             ``tick()`` / ``step()`` calls -- no I/O)
- ``raftkv.raft.transport`` pluggable transports: an in-memory one for
                             deterministic tests, and an HTTP one for a
                             real multi-process cluster
- ``raftkv.store``          the key-value state machine applied on top
                             of the committed log
- ``raftkv.server``         glues a RaftNode + transport + store into a
                             runnable server with a small HTTP client API
- ``raftkv.client``         a client library that finds the leader and
                             retries on redirects
"""

__version__ = "0.1.0"
