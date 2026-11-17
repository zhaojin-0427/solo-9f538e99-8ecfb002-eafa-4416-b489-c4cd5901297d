"""质控隔离：找出最少应停供的来源分片并打标记。

业务规则
========
* 一次提交 1~20 个质控键（站点 + UTC 时刻 + 存续指标 ID + 非负绝对容差）。
* 只评估“在供”分片：已提交且未被隔离；已隔离分片已停供，不再参与评估。
* 同键读数取非 NULL（NaN 视为无效读数，同样不计）。
  * 相关分片 = 至少持有一条同键非 NULL 读数的在供分片，至多 12 个，超出整次拒绝；
  * 缺测键 = 任何在供分片都没有该键的非 NULL 读数，整次拒绝。
* 求解最小隔离方案：每个键在剩余分片中至少保留一条读数，且剩余读数的
  最大值减最小值不超过容差；同一分片内相互矛盾的读数也计入最大/最小值。
  隔离数量相同时，取分片 ID 升序序列最小者。无可行解整次拒绝。
* 评估与落库在调用方的写锁 + 立即事务内完成：与上传、建快照交错时彼此
  只能看到完整结果；任何失败整体回滚，供数不变。
* 隔离只打标记（``isolated_shards``），不删文件：之后新建的快照排除已隔离
  分片；旧快照固化的分片清单不变，仍可分页读取原文件。
"""
from __future__ import annotations

import itertools
import json
import math
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, NamedTuple, Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .config import INDICATOR_COLUMN_PREFIX, SAMPLED_AT_COLUMN, STATION_COLUMN
from .models import ConflictError, NotFound

MAX_QC_KEYS = 20
MAX_RELEVANT_SHARDS = 12


class QcKey(NamedTuple):
    """归一化后的质控键（sampled_at 已统一到 UTC）。"""

    station: str
    sampled_at: datetime
    indicator_id: int
    tolerance: float


# ---------------------------------------------------------------------------
# 输入归一化
# ---------------------------------------------------------------------------

def parse_utc_instant(raw: str) -> datetime:
    """解析 ISO-8601 时刻并归一到 UTC；无时区值按 UTC（与接入口径一致）。"""
    text = str(raw).strip()
    if not text:
        raise ConflictError("质控键的 sampled_at 不能为空")
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ConflictError(
            f"质控键的 sampled_at 必须是 ISO-8601 时间戳，非法值：{raw!r}"
        ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_instant(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# ---------------------------------------------------------------------------
# 读数扫描：从在供分片收集同键非 NULL 值
# ---------------------------------------------------------------------------

def _scan_shard(file_path: str, keys: List[QcKey]) -> Dict[int, List[float]]:
    """扫描单个分片文件，返回 {质控键序号: [非 NULL 读数]}（仅含有读数的键）。"""
    names = set(pq.read_schema(file_path).names)
    indicator_cols: Dict[int, str] = {}
    for key in keys:
        col = f"{INDICATOR_COLUMN_PREFIX}{key.indicator_id}"
        if col in names:
            indicator_cols[key.indicator_id] = col

    columns = [STATION_COLUMN, SAMPLED_AT_COLUMN, *sorted(set(indicator_cols.values()))]
    table = pq.read_table(file_path, columns=columns)
    stations = table.column(STATION_COLUMN).combine_chunks()
    times = table.column(SAMPLED_AT_COLUMN).combine_chunks()
    ts_type = times.type

    found: Dict[int, List[float]] = {}
    for idx, key in enumerate(keys):
        col = indicator_cols.get(key.indicator_id)
        if col is None:
            continue
        mask = pc.and_(
            pc.equal(stations, key.station),
            pc.equal(times, pa.scalar(key.sampled_at, type=ts_type)),
        )
        values = table.column(col).combine_chunks().filter(mask).to_pylist()
        vals = [v for v in values if v is not None and not math.isnan(v)]
        if vals:
            found[idx] = vals
    return found


# ---------------------------------------------------------------------------
# 最小隔离方案求解（相关分片 <= 12，枚举子集）
# ---------------------------------------------------------------------------

def _feasible(
    key_stats: List[Dict[int, tuple]],
    tolerances: List[float],
    cut: set,
) -> bool:
    """隔离 cut 后，每个键是否仍至少有一条读数且极差不超过容差。"""
    for stats, tol in zip(key_stats, tolerances):
        count = 0
        low = math.inf
        high = -math.inf
        for shard_id, (cnt, mn, mx) in stats.items():
            if shard_id in cut:
                continue
            count += cnt
            low = min(low, mn)
            high = max(high, mx)
        if count == 0 or high - low > tol:
            return False
    return True


def _solve(
    key_stats: List[Dict[int, tuple]],
    tolerances: List[float],
    relevant: List[int],
) -> Optional[List[int]]:
    """枚举相关分片子集，返回数量最少、同数量时分片 ID 升序序列最小的方案。"""
    shards = sorted(relevant)
    for size in range(len(shards) + 1):
        for combo in itertools.combinations(shards, size):
            if _feasible(key_stats, tolerances, set(combo)):
                return list(combo)
    return None


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def _require_active_indicators(conn: sqlite3.Connection, indicator_ids: List[int]) -> None:
    placeholders = ",".join("?" for _ in indicator_ids)
    rows = conn.execute(
        f"SELECT indicator_id, active FROM indicators WHERE indicator_id IN ({placeholders})",
        indicator_ids,
    ).fetchall()
    state = {int(r["indicator_id"]): int(r["active"]) for r in rows}
    for indicator_id in indicator_ids:
        if indicator_id not in state:
            raise NotFound(f"指标 indicator_id={indicator_id} 不存在，质控键要求存续指标 ID")
        if state[indicator_id] == 0:
            raise ConflictError(
                f"指标 indicator_id={indicator_id} 已被删除，不是存续指标，整次拒绝"
            )


def _key_brief(key: QcKey) -> str:
    return (
        f"(station={key.station!r}, sampled_at={_format_instant(key.sampled_at)}, "
        f"indicator_id={key.indicator_id})"
    )


def _key_audit(key: QcKey) -> dict:
    return {
        "station": key.station,
        "sampled_at": _format_instant(key.sampled_at),
        "indicator_id": key.indicator_id,
        "tolerance": key.tolerance,
    }


def isolate(conn: sqlite3.Connection, keys: List[QcKey]) -> dict:
    """评估并在需要时落库隔离分片。必须在调用方的写锁 + 立即事务内调用。

    :raises NotFound/ConflictError: 任何校验或求解失败；调用方回滚即可，供数不变。
    """
    if not 1 <= len(keys) <= MAX_QC_KEYS:
        raise ConflictError(f"质控键数量须在 1~{MAX_QC_KEYS} 之间，实际 {len(keys)}")

    # 1) 指标必须存续
    _require_active_indicators(conn, sorted({k.indicator_id for k in keys}))

    # 2) 扫描在供分片（已隔离分片已停供，不参与评估）
    shard_rows = conn.execute(
        "SELECT shard_id, file_path, original_name FROM shards "
        "WHERE shard_id NOT IN (SELECT shard_id FROM isolated_shards) "
        "ORDER BY shard_id"
    ).fetchall()
    source_files = {int(r["shard_id"]): r["original_name"] for r in shard_rows}

    readings: List[Dict[int, List[float]]] = [dict() for _ in keys]
    for row in shard_rows:
        shard_id = int(row["shard_id"])
        for idx, vals in _scan_shard(row["file_path"], keys).items():
            readings[idx].setdefault(shard_id, []).extend(vals)

    # 3) 缺测键整次拒绝
    missing = [i for i, per in enumerate(readings) if not per]
    if missing:
        desc = "、".join(f"第{i + 1}键{_key_brief(keys[i])}" for i in missing)
        raise ConflictError(
            f"缺测键：{desc} 在在供分片中没有任何非 NULL 读数，整次拒绝，供数不变"
        )

    # 4) 相关分片上限
    relevant = sorted({sid for per in readings for sid in per})
    if len(relevant) > MAX_RELEVANT_SHARDS:
        raise ConflictError(
            f"相关分片共 {len(relevant)} 个，超过上限 {MAX_RELEVANT_SHARDS} 个，整次拒绝，供数不变"
        )

    # 5) 求解最小隔离方案（分片内矛盾读数已通过 min/max 计入）
    key_stats = [
        {sid: (len(vals), min(vals), max(vals)) for sid, vals in per.items()}
        for per in readings
    ]
    tolerances = [k.tolerance for k in keys]
    solution = _solve(key_stats, tolerances, relevant)
    if solution is None:
        raise ConflictError(
            "不存在可行的隔离方案：有键的全部读数互相矛盾，"
            "无法在不丢光该键读数的前提下满足容差，整次拒绝，供数不变"
        )

    # 6) 落库（仅在确实隔离时分片写事件；空方案不产生任何状态变化）
    event_id: Optional[int] = None
    if solution:
        detail = json.dumps(
            {"keys": [_key_audit(k) for k in keys], "isolated_shard_ids": solution},
            ensure_ascii=False,
        )
        cur = conn.execute(
            "INSERT INTO isolation_events (key_count, detail) VALUES (?, ?)",
            (len(keys), detail),
        )
        event_id = int(cur.lastrowid)
        for shard_id in solution:
            conn.execute(
                "INSERT INTO isolated_shards (shard_id, event_id) VALUES (?, ?)",
                (shard_id, event_id),
            )

    # 7) 组装响应：逐键给出冲突来源（隔离前的读数分布）
    key_results = []
    for key, per in zip(keys, readings):
        all_values = [v for vals in per.values() for v in vals]
        low, high = min(all_values), max(all_values)
        spread = high - low
        key_results.append(
            {
                "station": key.station,
                "sampled_at": _format_instant(key.sampled_at),
                "indicator_id": key.indicator_id,
                "tolerance": key.tolerance,
                "min_value": low,
                "max_value": high,
                "spread": spread,
                "within_tolerance": spread <= key.tolerance,
                "readings": [
                    {
                        "shard_id": sid,
                        "source_file": source_files[sid],
                        "values": sorted(vals),
                    }
                    for sid, vals in sorted(per.items())
                ],
            }
        )

    if solution:
        message = (
            f"已隔离 {len(solution)} 个分片：{solution}；此后新建快照将排除这些分片，"
            "旧快照不受影响，仍可分页读取原文件"
        )
    else:
        message = "全部质控键的在供读数已在容差内一致，无需隔离任何分片"

    return {
        "event_id": event_id,
        "isolated_shard_ids": solution,
        "isolated_sources": [
            {"shard_id": sid, "source_file": source_files[sid]} for sid in solution
        ],
        "key_results": key_results,
        "message": message,
    }


def list_isolations(conn: sqlite3.Connection) -> dict:
    """当前全部已隔离（停供）分片及其来源文件。"""
    rows = conn.execute(
        "SELECT i.shard_id, i.event_id, i.isolated_at, s.original_name "
        "FROM isolated_shards i JOIN shards s ON s.shard_id = i.shard_id "
        "ORDER BY i.shard_id"
    ).fetchall()
    return {
        "isolated_shard_ids": [int(r["shard_id"]) for r in rows],
        "isolated": [
            {
                "shard_id": int(r["shard_id"]),
                "source_file": r["original_name"],
                "event_id": int(r["event_id"]),
                "isolated_at": r["isolated_at"],
            }
            for r in rows
        ],
    }
