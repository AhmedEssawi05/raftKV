"""The Raft consensus state machine.

This module deliberately contains *no* I/O: no sockets, no real timers,
no threads. It is a pure state machine in the style of etcd/raft --
inputs go in through ``tick()`` and ``step()``, and outputs (messages to
send, and newly committed log entries to apply) come back out as plain
data. That is what makes it possible to test leader election, log
replication and the safety properties deterministically, tick by tick,
without any sleeping or flakiness -- see tests/test_election.py and
tests/test_log_replication.py.

A thin adapter (raftkv.server.RaftServer) drives this with a real clock
and a real transport for the actual running cluster.
"""
from __future__ import annotations

import json
import os
import random
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from raftkv.raft.log import LogEntry, RaftLog
from raftkv.raft.rpc import (
    AppendEntries,
    AppendEntriesReply,
    LogEntryWire,
    RequestVote,
    RequestVoteReply,
)


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class Send:
    """An outbound message the driving loop should hand to the transport."""

    dest: str
    payload: Any


class RaftNode:
    """One replica's Raft state machine.

    Parameters
    ----------
    node_id:
        This node's id.
    peer_ids:
        The ids of every *other* node in the cluster.
    log:
        A RaftLog instance (in-memory or WAL-backed).
    state_path:
        Optional path to persist ``current_term`` / ``voted_for`` across
        restarts. Losing this would let a restarted node vote twice in
        the same term, which can violate Raft's safety guarantee.
    election_timeout_ticks:
        (low, high) range; each node picks a random value in this range
        so followers don't all time out and start elections
        simultaneously (Raft paper §5.2).
    heartbeat_interval_ticks:
        How often a leader re-sends AppendEntries to keep followers from
        starting an election. Must be well below the election timeout.
    """

    def __init__(
        self,
        node_id: str,
        peer_ids: list[str],
        log: Optional[RaftLog] = None,
        state_path: Optional[str] = None,
        election_timeout_ticks: tuple[int, int] = (10, 20),
        heartbeat_interval_ticks: int = 3,
        rng: Optional[random.Random] = None,
    ):
        self.id = node_id
        self.peer_ids = list(peer_ids)
        self.log = log if log is not None else RaftLog()
        self.state_path = state_path
        self._rng = rng or random.Random()

        self.current_term = 0
        self.voted_for: Optional[str] = None
        self._load_state()

        self.role = Role.FOLLOWER
        self.leader_id: Optional[str] = None
        self.commit_index = 0
        self.last_applied = 0

        # Leader-only volatile state.
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        self._election_timeout_range = election_timeout_ticks
        self._heartbeat_interval = heartbeat_interval_ticks
        self._election_elapsed = 0
        self._election_timeout = self._random_timeout()
        self._heartbeat_elapsed = 0

        # Votes received this election (candidate only).
        self._votes: set[str] = set()

        # Outputs accumulated by the last tick()/step() call.
        self.outbox: list[Send] = []
        self.applied: list[LogEntry] = []

    # -- persistence -----------------------------------------------------
    def _load_state(self) -> None:
        if self.state_path and os.path.exists(self.state_path):
            with open(self.state_path) as f:
                data = json.load(f)
            self.current_term = data.get("current_term", 0)
            self.voted_for = data.get("voted_for")

    def _save_state(self) -> None:
        if not self.state_path:
            return
        os.makedirs(os.path.dirname(self.state_path) or ".", exist_ok=True)
        tmp = self.state_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"current_term": self.current_term, "voted_for": self.voted_for}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.state_path)

    # -- helpers -----------------------------------------------------
    def _random_timeout(self) -> int:
        lo, hi = self._election_timeout_range
        return self._rng.randint(lo, hi)

    def _reset_election_timer(self) -> None:
        self._election_elapsed = 0
        self._election_timeout = self._random_timeout()

    def _send(self, dest: str, payload: Any) -> None:
        self.outbox.append(Send(dest, payload))

    def _broadcast(self, payload_fn) -> None:
        for peer in self.peer_ids:
            self._send(peer, payload_fn(peer))

    def _become_follower(self, term: int, leader_id: Optional[str] = None) -> None:
        stepping_down = self.role != Role.FOLLOWER
        self.role = Role.FOLLOWER
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self._save_state()
        self.leader_id = leader_id
        self._votes.clear()
        if stepping_down or leader_id is not None:
            self._reset_election_timer()

    def _become_candidate(self) -> None:
        self.role = Role.CANDIDATE
        self.current_term += 1
        self.voted_for = self.id
        self._votes = {self.id}
        self.leader_id = None
        self._save_state()
        self._reset_election_timer()
        last_index = self.log.last_index()
        last_term = self.log.last_term()
        self._broadcast(
            lambda peer: RequestVote(
                term=self.current_term,
                candidate_id=self.id,
                last_log_index=last_index,
                last_log_term=last_term,
            )
        )

    def _become_leader(self) -> None:
        self.role = Role.LEADER
        self.leader_id = self.id
        last_index = self.log.last_index()
        self.next_index = {p: last_index + 1 for p in self.peer_ids}
        self.match_index = {p: 0 for p in self.peer_ids}
        self._heartbeat_elapsed = self._heartbeat_interval  # send immediately
        self._send_append_entries_to_all()

    # -- driving the state machine -----------------------------------
    def tick(self) -> list[Send]:
        """Advance logical time by one tick. Call this on a fixed
        schedule (e.g. every 50ms in real usage, or once per loop
        iteration in tests)."""
        self.outbox = []
        self.applied = []
        if self.role in (Role.FOLLOWER, Role.CANDIDATE):
            self._election_elapsed += 1
            if self._election_elapsed >= self._election_timeout:
                self._become_candidate()
        if self.role == Role.LEADER:
            self._heartbeat_elapsed += 1
            if self._heartbeat_elapsed >= self._heartbeat_interval:
                self._heartbeat_elapsed = 0
                self._send_append_entries_to_all()
        return self.outbox

    def step(self, message: Any) -> list[Send]:
        """Feed in one message received from a peer and return the
        resulting outbound messages."""
        self.outbox = []
        self.applied = []
        handler = {
            RequestVote: self._on_request_vote,
            RequestVoteReply: self._on_request_vote_reply,
            AppendEntries: self._on_append_entries,
            AppendEntriesReply: self._on_append_entries_reply,
        }.get(type(message))
        if handler is None:
            raise TypeError(f"unknown message type: {type(message)!r}")
        handler(message)
        return self.outbox

    # -- client interface -----------------------------------------------
    def propose(self, command: Any) -> Optional[LogEntry]:
        """Leader-only: append a new command to the log. Returns the new
        entry (not yet committed) or None if this node isn't the
        leader."""
        if self.role != Role.LEADER:
            return None
        entry = self.log.append(self.current_term, command)
        self.match_index[self.id] = entry.index
        self._send_append_entries_to_all()
        return entry

    def is_leader(self) -> bool:
        return self.role == Role.LEADER

    # -- RequestVote -----------------------------------------------------
    def _on_request_vote(self, msg: RequestVote) -> None:
        if msg.term > self.current_term:
            self._become_follower(msg.term)

        grant = False
        if msg.term < self.current_term:
            grant = False
        elif self.voted_for in (None, msg.candidate_id):
            our_last_term = self.log.last_term()
            our_last_index = self.log.last_index()
            log_ok = (msg.last_log_term > our_last_term) or (
                msg.last_log_term == our_last_term and msg.last_log_index >= our_last_index
            )
            if log_ok:
                grant = True
                self.voted_for = msg.candidate_id
                self._save_state()
                self._reset_election_timer()

        self._send(
            msg.candidate_id,
            RequestVoteReply(term=self.current_term, vote_granted=grant, voter_id=self.id),
        )

    def _on_request_vote_reply(self, msg: RequestVoteReply) -> None:
        if msg.term > self.current_term:
            self._become_follower(msg.term)
            return
        if self.role != Role.CANDIDATE or msg.term != self.current_term:
            return
        if msg.vote_granted:
            self._votes.add(msg.voter_id)
            if len(self._votes) > (len(self.peer_ids) + 1) // 2:
                self._become_leader()

    # -- AppendEntries -----------------------------------------------------
    def _on_append_entries(self, msg: AppendEntries) -> None:
        if msg.term > self.current_term:
            self._become_follower(msg.term, leader_id=msg.leader_id)
        elif msg.term == self.current_term:
            # Valid current leader: stay/become follower, reset timer.
            if self.role != Role.FOLLOWER:
                self._become_follower(msg.term, leader_id=msg.leader_id)
            else:
                self.leader_id = msg.leader_id
                self._reset_election_timer()

        if msg.term < self.current_term:
            self._send(
                msg.leader_id,
                AppendEntriesReply(term=self.current_term, success=False, follower_id=self.id),
            )
            return

        # Consistency check on prev_log_index/term.
        our_prev_term = self.log.term_at(msg.prev_log_index)
        if msg.prev_log_index > 0 and our_prev_term != msg.prev_log_term:
            if our_prev_term == -1:
                # We don't even have an entry there.
                conflict_index = self.log.last_index() + 1
                conflict_term = -1
            else:
                # Find the first index of the conflicting term to let the
                # leader skip back a whole term at once (§5.3 optimization).
                conflict_term = our_prev_term
                conflict_index = msg.prev_log_index
                while (
                    conflict_index > 1
                    and self.log.term_at(conflict_index - 1) == conflict_term
                ):
                    conflict_index -= 1
            self._send(
                msg.leader_id,
                AppendEntriesReply(
                    term=self.current_term,
                    success=False,
                    follower_id=self.id,
                    conflict_index=conflict_index,
                    conflict_term=conflict_term,
                ),
            )
            return

        # Append any new entries, truncating on conflict.
        index = msg.prev_log_index
        for wire in msg.entries:
            index += 1
            existing_term = self.log.term_at(index)
            if existing_term == wire["term"]:
                continue  # already have it, identical
            if existing_term != -1:
                # Conflict: delete this entry and everything after it.
                self.log.truncate_from(index)
            self.log.append_entry(LogEntry(term=wire["term"], index=wire["index"], command=wire["command"]))

        if msg.leader_commit > self.commit_index:
            self.commit_index = min(msg.leader_commit, self.log.last_index())
            self._advance_applied()

        self._send(
            msg.leader_id,
            AppendEntriesReply(
                term=self.current_term,
                success=True,
                follower_id=self.id,
                match_index=self.log.last_index(),
            ),
        )

    def _on_append_entries_reply(self, msg: AppendEntriesReply) -> None:
        if msg.term > self.current_term:
            self._become_follower(msg.term)
            return
        if self.role != Role.LEADER or msg.term != self.current_term:
            return

        if msg.success:
            self.match_index[msg.follower_id] = max(self.match_index.get(msg.follower_id, 0), msg.match_index)
            self.next_index[msg.follower_id] = self.match_index[msg.follower_id] + 1
            self._advance_commit_index()
        else:
            if msg.conflict_index >= 0:
                self.next_index[msg.follower_id] = max(1, msg.conflict_index)
            else:
                self.next_index[msg.follower_id] = max(1, self.next_index.get(msg.follower_id, 1) - 1)
            self._send_append_entries_to(msg.follower_id)

    # -- leader helpers -----------------------------------------------------
    def _send_append_entries_to_all(self) -> None:
        for peer in self.peer_ids:
            self._send_append_entries_to(peer)

    def _send_append_entries_to(self, peer: str) -> None:
        next_idx = self.next_index.get(peer, self.log.last_index() + 1)
        prev_index = next_idx - 1
        prev_term = self.log.term_at(prev_index)
        entries = [
            LogEntryWire(term=e.term, index=e.index, command=e.command).__dict__
            for e in self.log.entries_from(next_idx)
        ]
        self._send(
            peer,
            AppendEntries(
                term=self.current_term,
                leader_id=self.id,
                prev_log_index=prev_index,
                prev_log_term=prev_term if prev_term != -1 else 0,
                entries=entries,
                leader_commit=self.commit_index,
            ),
        )

    def _advance_commit_index(self) -> None:
        """A leader commits an entry once it's replicated on a majority
        AND the entry was created in the leader's *current* term (Raft
        paper §5.4.2 -- this restriction is what makes it safe)."""
        match_indexes = sorted([self.log.last_index()] + list(self.match_index.values()), reverse=True)
        majority_index = match_indexes[len(match_indexes) // 2]
        if majority_index > self.commit_index and self.log.term_at(majority_index) == self.current_term:
            self.commit_index = majority_index
            self._advance_applied()

    def _advance_applied(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.get(self.last_applied)
            if entry is not None:
                self.applied.append(entry)
