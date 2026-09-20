from raftkv.store import KVStore


def test_put_and_get():
    s = KVStore()
    s.apply(1, {"op": "put", "key": "a", "value": 1})
    assert s.get("a") == 1


def test_delete():
    s = KVStore()
    s.apply(1, {"op": "put", "key": "a", "value": 1})
    s.apply(2, {"op": "delete", "key": "a"})
    assert s.get("a") is None


def test_apply_is_idempotent_for_replayed_index():
    s = KVStore()
    s.apply(1, {"op": "put", "key": "a", "value": 1})
    s.apply(1, {"op": "put", "key": "a", "value": 999})  # replay of same index: ignored
    assert s.get("a") == 1


def test_unknown_op_reports_error_without_raising():
    s = KVStore()
    result = s.apply(1, {"op": "frobnicate"})
    assert result["ok"] is False


def test_snapshot_is_a_copy():
    s = KVStore()
    s.apply(1, {"op": "put", "key": "a", "value": 1})
    snap = s.snapshot()
    snap["a"] = 2
    assert s.get("a") == 1
