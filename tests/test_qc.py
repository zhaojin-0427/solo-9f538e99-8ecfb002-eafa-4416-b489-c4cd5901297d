"""质控隔离自测：最小停供方案、缺测/超限拒绝、快照语义、并发与重启。"""
from __future__ import annotations

import threading

import pyarrow as pa

from app.db import init_db
from app.main import app
from fastapi.testclient import TestClient

T = "2026-01-01T00:00:00Z"


def upload_file(client, path, version: int):
    with open(path, "rb") as fh:
        return client.post(
            "/ingest/parquet",
            data={"schema_version": str(version)},
            files={"file": (path.name, fh.read(), "application/vnd.apache.parquet")},
        )


def setup_indicator(client, name="浓度"):
    r = client.post("/schema/indicators", json={"name": name})
    assert r.status_code == 201, r.text
    return r.json()["indicator_id"], r.json()["new_version"]


def add_shard(client, make_parquet, version, name, rows, indicator_name="浓度"):
    """rows: [(station, sampled_at, value), ...]；value 为 None 表示 NULL 读数。"""
    f = make_parquet(name, {
        "station": [r[0] for r in rows],
        "sampled_at": [r[1] for r in rows],
        indicator_name: [r[2] for r in rows],
    }, types={"sampled_at": pa.string(), indicator_name: pa.float64()})
    r = upload_file(client, f, version)
    assert r.status_code == 200, r.text
    return r.json()["shard_id"]


def qc_key(station, indicator_id, tolerance=0.5, sampled_at=T):
    return {
        "station": station,
        "sampled_at": sampled_at,
        "indicator_id": indicator_id,
        "tolerance": tolerance,
    }


# ---------------------------------------------------------------------------
# 最小隔离方案与平局裁决
# ---------------------------------------------------------------------------

def test_isolate_minimal_and_snapshot_semantics(client, make_parquet):
    iid, v = setup_indicator(client)
    s1 = add_shard(client, make_parquet, v, "s1.parquet", [("A", T, 1.0)])
    s2 = add_shard(client, make_parquet, v, "s2.parquet", [("A", T, 5.0)])

    # 隔离前的旧快照
    old = client.get("/rows").json()
    assert old["total_rows"] == 2

    r = client.post("/qc/isolate", json={"keys": [qc_key("A", iid)]})
    assert r.status_code == 200, r.text
    body = r.json()
    # 两个方案 [s1] / [s2] 数量相同，取分片 ID 升序序列最小者
    assert body["isolated_shard_ids"] == [s1]
    assert body["isolated_sources"] == [{"shard_id": s1, "source_file": "s1.parquet"}]
    assert body["event_id"] == 1
    kr = body["key_results"][0]
    assert kr["min_value"] == 1.0 and kr["max_value"] == 5.0 and kr["spread"] == 4.0
    assert kr["within_tolerance"] is False
    assert {rd["shard_id"] for rd in kr["readings"]} == {s1, s2}

    # 新快照排除已隔离分片
    new = client.get("/rows").json()
    assert new["snapshot_id"] != old["snapshot_id"]
    assert new["total_rows"] == 1
    assert new["rows"][0]["浓度"] == 5.0
    meta = client.get(f"/snapshots/{new['snapshot_id']}").json()
    assert meta["shard_ids"] == [s2]

    # 旧快照仍可分页读取原文件（含被隔离分片的行）
    p1 = client.get("/rows", params={"snapshot_id": old["snapshot_id"], "page": 1, "page_size": 1}).json()
    p2 = client.get("/rows", params={"snapshot_id": old["snapshot_id"], "page": 2, "page_size": 1}).json()
    assert p1["total_rows"] == 2 and p2["total_rows"] == 2
    assert sorted([p1["rows"][0]["浓度"], p2["rows"][0]["浓度"]]) == [1.0, 5.0]

    # 隔离清单
    iso = client.get("/qc/isolations").json()
    assert iso["isolated_shard_ids"] == [s1]
    assert iso["isolated"][0]["source_file"] == "s1.parquet"


def test_intra_shard_contradiction_forces_isolation(client, make_parquet):
    """同一分片内相互矛盾的读数也计入：自相矛盾的分片必须被隔离。"""
    iid, v = setup_indicator(client)
    s1 = add_shard(client, make_parquet, v, "dup.parquet", [("A", T, 1.0), ("A", T, 5.0)])
    s2 = add_shard(client, make_parquet, v, "ok.parquet", [("A", T, 1.2)])

    r = client.post("/qc/isolate", json={"keys": [qc_key("A", iid)]})
    assert r.status_code == 200, r.text
    body = r.json()
    # s1 内部极差 4.0 已超容差，任何保留 s1 的方案都不可行
    assert body["isolated_shard_ids"] == [s1]
    reading = next(rd for rd in body["key_results"][0]["readings"] if rd["shard_id"] == s1)
    assert reading["values"] == [1.0, 5.0]  # 分片内全部读数都计入
    assert client.get("/rows").json()["total_rows"] == 1


def test_joint_minimum_prefers_shared_shard(client, make_parquet):
    """两个键联合优化：隔离共享分片（1 个）优于各隔离一个（2 个）。"""
    iid, v = setup_indicator(client)
    add_shard(client, make_parquet, v, "k1a.parquet", [("A", T, 1.0)])
    s2 = add_shard(client, make_parquet, v, "shared.parquet", [("A", T, 5.0), ("B", T, 1.0)])
    add_shard(client, make_parquet, v, "k2b.parquet", [("B", T, 5.0)])

    r = client.post("/qc/isolate", json={"keys": [qc_key("A", iid), qc_key("B", iid)]})
    assert r.status_code == 200, r.text
    assert r.json()["isolated_shard_ids"] == [s2]


def test_tie_break_lexicographic_size_two(client, make_parquet):
    """数量相同（2 个）时取升序序列最小：[1,3] < [1,4] < [2,3] < [2,4]。"""
    iid, v = setup_indicator(client)
    s1 = add_shard(client, make_parquet, v, "a1.parquet", [("A", T, 1.0)])
    add_shard(client, make_parquet, v, "a2.parquet", [("A", T, 5.0)])
    s3 = add_shard(client, make_parquet, v, "b1.parquet", [("B", T, 1.0)])
    add_shard(client, make_parquet, v, "b2.parquet", [("B", T, 5.0)])

    r = client.post("/qc/isolate", json={"keys": [qc_key("A", iid), qc_key("B", iid)]})
    assert r.status_code == 200, r.text
    assert r.json()["isolated_shard_ids"] == [s1, s3]


def test_multiple_readings_per_shard_counted(client, make_parquet):
    """分片内多条读数一起参与极差；保留内部一致的分片更优时会被选中。"""
    iid, v = setup_indicator(client)
    s1 = add_shard(client, make_parquet, v, "multi.parquet", [("A", T, 1.0), ("A", T, 1.1)])
    s2 = add_shard(client, make_parquet, v, "outlier.parquet", [("A", T, 5.0)])

    r = client.post("/qc/isolate", json={"keys": [qc_key("A", iid)]})
    assert r.status_code == 200, r.text
    # 隔离 s1 或 s2 都可行且数量相同 -> 升序最小 [s1]
    assert r.json()["isolated_shard_ids"] == [s1]
    assert client.get("/rows").json()["rows"][0]["浓度"] == 5.0


def test_consistent_keys_no_isolation(client, make_parquet):
    iid, v = setup_indicator(client)
    add_shard(client, make_parquet, v, "c1.parquet", [("A", T, 1.0)])
    add_shard(client, make_parquet, v, "c2.parquet", [("A", T, 1.2)])

    r = client.post("/qc/isolate", json={"keys": [qc_key("A", iid)]})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["isolated_shard_ids"] == []
    assert body["event_id"] is None
    assert body["key_results"][0]["within_tolerance"] is True
    # 无隔离 -> 无任何状态变化
    assert client.get("/qc/isolations").json()["isolated_shard_ids"] == []
    assert client.get("/rows").json()["total_rows"] == 2


def test_tolerance_boundary_inclusive(client, make_parquet):
    """极差恰好等于容差时不隔离（不超过容差即合格）。"""
    iid, v = setup_indicator(client)
    add_shard(client, make_parquet, v, "e1.parquet", [("A", T, 1.0)])
    add_shard(client, make_parquet, v, "e2.parquet", [("A", T, 1.5)])

    r = client.post("/qc/isolate", json={"keys": [qc_key("A", iid, tolerance=0.5)]})
    assert r.status_code == 200, r.text
    assert r.json()["isolated_shard_ids"] == []
    assert r.json()["key_results"][0]["spread"] == 0.5


def test_null_values_ignored_and_null_only_key_is_missing(client, make_parquet):
    """NULL 不计入读数；只有 NULL 的键视为缺测，整次拒绝。"""
    iid, v = setup_indicator(client)
    add_shard(client, make_parquet, v, "n1.parquet", [("A", T, None), ("A", T, 1.0)])
    s2 = add_shard(client, make_parquet, v, "n2.parquet", [("A", T, 1.1)])

    # NULL 行被忽略，两个分片各一条有效读数，极差 0.1 在容差内
    r = client.post("/qc/isolate", json={"keys": [qc_key("A", iid)]})
    assert r.status_code == 200, r.text
    assert r.json()["isolated_shard_ids"] == []
    vals = r.json()["key_results"][0]["readings"]
    assert {rd["shard_id"]: rd["values"] for rd in vals} == {1: [1.0], s2: [1.1]}

    # 另一个时刻只有 NULL -> 缺测 -> 409
    r = client.post("/qc/isolate", json={"keys": [qc_key("A", iid, sampled_at="2026-02-02T00:00:00Z")]})
    assert r.status_code == 409
    assert "缺测" in r.json()["detail"]


# ---------------------------------------------------------------------------
# 整次拒绝：缺测、超 12 个相关分片、无可行方案；失败不改变供数
# ---------------------------------------------------------------------------

def test_missing_key_rejects_without_state_change(client, make_parquet):
    iid, v = setup_indicator(client)
    add_shard(client, make_parquet, v, "m1.parquet", [("A", T, 1.0)])
    add_shard(client, make_parquet, v, "m2.parquet", [("A", T, 5.0)])

    keys = [qc_key("A", iid), qc_key("GhostStation", iid)]
    r = client.post("/qc/isolate", json={"keys": keys})
    assert r.status_code == 409, r.text
    assert "缺测" in r.json()["detail"]

    # 失败不改变供数：无隔离、行数不变、新快照仍含全部两个分片
    assert client.get("/qc/isolations").json()["isolated_shard_ids"] == []
    fresh = client.get("/rows").json()
    assert fresh["total_rows"] == 2


def test_more_than_12_relevant_shards_rejected(client, make_parquet):
    iid, v = setup_indicator(client)
    for i in range(13):
        add_shard(client, make_parquet, v, f"r{i}.parquet", [("A", T, float(i))])

    r = client.post("/qc/isolate", json={"keys": [qc_key("A", iid, tolerance=0.5)]})
    assert r.status_code == 409, r.text
    assert "12" in r.json()["detail"]
    assert client.get("/qc/isolations").json()["isolated_shard_ids"] == []
    assert client.get("/rows").json()["total_rows"] == 13


def test_infeasible_when_only_source_self_contradicts(client, make_parquet):
    """唯一来源自相矛盾：隔离它会丢光该键读数 -> 无可行方案，整次拒绝。"""
    iid, v = setup_indicator(client)
    add_shard(client, make_parquet, v, "only.parquet", [("A", T, 1.0), ("A", T, 5.0)])

    r = client.post("/qc/isolate", json={"keys": [qc_key("A", iid)]})
    assert r.status_code == 409, r.text
    assert "可行" in r.json()["detail"]
    assert client.get("/qc/isolations").json()["isolated_shard_ids"] == []
    assert client.get("/rows").json()["total_rows"] == 2


# ---------------------------------------------------------------------------
# 请求校验
# ---------------------------------------------------------------------------

def test_request_validation(client, make_parquet):
    iid, v = setup_indicator(client)
    add_shard(client, make_parquet, v, "v1.parquet", [("A", T, 1.0)])
    base = qc_key("A", iid)

    # 21 个键 -> 422；0 个键 -> 422
    assert client.post("/qc/isolate", json={"keys": [base] * 21}).status_code == 422
    assert client.post("/qc/isolate", json={"keys": []}).status_code == 422
    # 负容差 -> 422；NaN 容差（非标准 JSON 字面量，直接构造请求体）-> 422
    assert client.post("/qc/isolate", json={"keys": [{**base, "tolerance": -0.1}]}).status_code == 422
    nan_body = b'{"keys": [{"station": "A", "sampled_at": "2026-01-01T00:00:00Z", "indicator_id": 1, "tolerance": NaN}]}'
    r = client.post("/qc/isolate", content=nan_body, headers={"content-type": "application/json"})
    assert r.status_code == 422
    # 空白站点 -> 422
    assert client.post("/qc/isolate", json={"keys": [{**base, "station": "  "}]}).status_code == 422
    # 非法时刻 -> 409
    r = client.post("/qc/isolate", json={"keys": [{**base, "sampled_at": "not-a-time"}]})
    assert r.status_code == 409
    # 不存在的指标 -> 404
    r = client.post("/qc/isolate", json={"keys": [qc_key("A", 999)]})
    assert r.status_code == 404
    # 全部失败均不改变供数
    assert client.get("/qc/isolations").json()["isolated_shard_ids"] == []
    assert client.get("/rows").json()["total_rows"] == 1


def test_deleted_indicator_rejected(client, make_parquet):
    iid, v = setup_indicator(client, "氨氮")
    add_shard(client, make_parquet, v, "d1.parquet", [("A", T, 1.0)], indicator_name="氨氮")
    assert client.delete("/schema/indicators/氨氮").status_code == 200

    r = client.post("/qc/isolate", json={"keys": [qc_key("A", iid)]})
    assert r.status_code == 409
    assert "删除" in r.json()["detail"]
    assert client.get("/qc/isolations").json()["isolated_shard_ids"] == []


def test_timezone_offset_key_matches_utc(client, make_parquet):
    """带偏移量的键时刻归一到 UTC 后匹配。"""
    iid, v = setup_indicator(client)
    add_shard(client, make_parquet, v, "z1.parquet", [("A", T, 1.0)])
    add_shard(client, make_parquet, v, "z2.parquet", [("A", T, 5.0)])

    key = qc_key("A", iid, sampled_at="2026-01-01T02:00:00+02:00")  # 即 2026-01-01T00:00:00Z
    r = client.post("/qc/isolate", json={"keys": [key]})
    assert r.status_code == 200, r.text
    assert r.json()["isolated_shard_ids"] == [1]
    assert r.json()["key_results"][0]["sampled_at"] == "2026-01-01T00:00:00.000000Z"


# ---------------------------------------------------------------------------
# 交错与持久化
# ---------------------------------------------------------------------------

def test_isolation_interleaved_with_upload_and_snapshots(client, make_parquet):
    """隔离与上传、建快照交错：每个快照呈现某个完整时点，旧快照不受隔离影响。"""
    iid, v = setup_indicator(client)
    s1 = add_shard(client, make_parquet, v, "i1.parquet", [("A", T, 1.0)])
    s2 = add_shard(client, make_parquet, v, "i2.parquet", [("A", T, 5.0)])

    snap1 = client.get("/rows").json()["snapshot_id"]  # 含 s1+s2

    r = client.post("/qc/isolate", json={"keys": [qc_key("A", iid)]})
    assert r.json()["isolated_shard_ids"] == [s1]

    snap2 = client.get("/rows").json()["snapshot_id"]  # 只含 s2
    s3 = add_shard(client, make_parquet, v, "i3.parquet", [("B", T, 9.0)])
    snap3 = client.get("/rows").json()["snapshot_id"]  # 含 s2+s3，不含 s1

    m1 = client.get(f"/snapshots/{snap1}").json()
    m2 = client.get(f"/snapshots/{snap2}").json()
    m3 = client.get(f"/snapshots/{snap3}").json()
    assert m1["shard_ids"] == [s1, s2]
    assert m2["shard_ids"] == [s2]
    assert m3["shard_ids"] == [s2, s3]

    # 旧快照分页仍读原文件
    page = client.get("/rows", params={"snapshot_id": snap1, "page_size": 100}).json()
    assert page["total_rows"] == 2
    assert sorted(row["浓度"] for row in page["rows"]) == [1.0, 5.0]


def test_isolation_persists_across_restart(client, make_parquet):
    iid, v = setup_indicator(client)
    add_shard(client, make_parquet, v, "p1.parquet", [("A", T, 1.0)])
    add_shard(client, make_parquet, v, "p2.parquet", [("A", T, 5.0)])
    assert client.post("/qc/isolate", json={"keys": [qc_key("A", iid)]}).json()["isolated_shard_ids"] == [1]

    del client
    init_db()
    with TestClient(app) as client2:
        iso = client2.get("/qc/isolations").json()
        assert iso["isolated_shard_ids"] == [1]
        assert iso["isolated"][0]["source_file"] == "p1.parquet"
        fresh = client2.get("/rows").json()
        assert fresh["total_rows"] == 1
        assert fresh["rows"][0]["浓度"] == 5.0


def test_concurrent_isolate_upload_snapshot(client, make_parquet):
    """隔离、上传、建快照并发交错：全部完整完成，终态一致。"""
    iid, v = setup_indicator(client)
    add_shard(client, make_parquet, v, "base1.parquet", [("A", T, 1.0)])
    add_shard(client, make_parquet, v, "base2.parquet", [("A", T, 5.0)])

    barrier = threading.Barrier(4)
    errors: list[Exception] = []
    snapshot_ids: list[int] = []

    def isolator():
        try:
            barrier.wait()
            for _ in range(5):
                r = client.post("/qc/isolate", json={"keys": [qc_key("A", iid)]})
                assert r.status_code == 200, r.text
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def uploader(tag):
        try:
            barrier.wait()
            for i in range(4):
                add_shard(client, make_parquet, v, f"u{tag}-{i}.parquet",
                          [(f"U{tag}-{i}", T, float(i))])
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def snapper():
        try:
            barrier.wait()
            for _ in range(5):
                r = client.get("/rows")
                assert r.status_code == 200, r.text
                snapshot_ids.append(r.json()["snapshot_id"])
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [
        threading.Thread(target=isolator),
        threading.Thread(target=uploader, args=(1,)),
        threading.Thread(target=uploader, args=(2,)),
        threading.Thread(target=snapper),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors

    # 只有第一个隔离请求真正隔离分片 1；后续请求看到的一致（分片 2 单独在供）
    iso = client.get("/qc/isolations").json()
    assert iso["isolated_shard_ids"] == [1]

    # 终态新快照排除全部已隔离分片，且包含全部 9 个在供分片
    fresh = client.get("/rows").json()
    meta = client.get(f"/snapshots/{fresh['snapshot_id']}").json()
    assert 1 not in meta["shard_ids"]
    assert fresh["total_rows"] == 9

    # 并发期间建的每个快照都可完整分页（各自呈现某个完整时点）
    for sid in snapshot_ids:
        m = client.get(f"/snapshots/{sid}").json()
        page = client.get("/rows", params={"snapshot_id": sid, "page_size": 10000}).json()
        assert page["total_rows"] == len(m["shard_ids"])
        assert len(page["rows"]) == page["total_rows"]


def test_openapi_lists_qc_endpoints(client):
    spec = client.get("/openapi.json").json()
    assert "/qc/isolate" in spec["paths"]
    assert "/qc/isolations" in spec["paths"]
