# 水质数仓化验表 API

固定站点、UTC 采样时刻，外加至多 8 个可空浮点指标的化验表服务。
技术栈：**FastAPI + PyArrow + SQLite + Parquet 分片**，可通过 Docker Compose 一键启动。

## 1. 核心模型（为什么改名/删名不会串列）

| 概念 | 说明 |
| --- | --- |
| 指标身份 `indicator_id` | 指标的稳定身份。**改名只改名字，不换身份**；删除只是 `active=0`，身份永不复用。 |
| 字段版 `schema_version` | 每次新增/改名/删除都产生一个不可变新版本，版内固化“字段名 → 指标身份”的完整映射。 |
| 分片 shard | 每次 Parquet 上传产生一个分片，物理列名为身份列 `i{indicator_id}`，与当前叫什么名字完全解耦。 |
| 快照 snapshot | 首次查询时固化“当时的字段名集合 + 当时的全部分片”，此后分页、字段变更、上传、重启都不改变它。 |

典型故事（与需求一致）：

1. 建指标 **浓度**（身份 1，字段版 2），上传历史分片 → 物理列 `i1`。
2. **浓度 改名为 硝酸盐**（身份仍是 1，字段版 3）。
3. 再建指标 **浓度**（同名复用 → 身份 2，字段版 4，语义为磷酸盐，不继承任何旧值）。
4. 用**旧字段版 2** 上传：旧版“浓度”映射到身份 1，数据进 `i1`（硝酸盐），不串列。
5. 用**字段版 4** 上传：当前“浓度”映射到身份 2，数据进 `i2`（磷酸盐）。
6. 查询按当前名合读：旧分片没有 `i2` → 磷酸盐列为 `NULL`；新分片没有 `i1` → 硝酸盐列为 `NULL`。

## 2. 启动

### Docker Compose（推荐）

```bash
docker compose up --build
# 服务：http://localhost:8000
# 交互式文档（Swagger）：http://localhost:8000/docs
# OpenAPI：http://localhost:8000/openapi.json
# 健康检查：http://localhost:8000/health
```

数据持久化在命名卷 `water_quality_data`（容器内 `/data`，含 `warehouse.db` 与 `shards/*.parquet`），
重启容器后字段、分片、快照全部保留。

### 本地直接运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 3. API 一览

| 方法/路径 | 作用 |
| --- | --- |
| `GET /schema` | 当前字段版号、固定字段、存续指标（含身份 ID） |
| `GET /schema/versions/{version_id}` | 查看任意字段版的不可变映射（含已删指标） |
| `POST /schema/indicators` | 新增指标（body `{"name": "..."}`）；同名复用得到**新身份** |
| `POST /schema/indicators/rename` | 改名（body `{"old_name": "...", "new_name": "..."}`），身份不变 |
| `DELETE /schema/indicators/{name}` | 删除指标（历史分片保留） |
| `POST /ingest/parquet` | 表单上传：`schema_version`（必填）+ `file`（Parquet） |
| `GET /rows` | 分页查询；首次不带 `snapshot_id` 即建快照，之后带快照号翻页 |
| `GET /snapshots/{snapshot_id}` | 快照元数据（固化的字段名与分片） |
| `POST /qc/isolate` | 质控隔离：按质控键找出最少应停供的来源分片并打标记 |
| `GET /qc/isolations` | 查看当前已隔离（停供）的分片及来源文件 |
| `GET /docs` | Swagger UI |

### Parquet 上传规则

* 必含固定列：`station`（非空字符串）、`sampled_at`（UTC 时刻；支持带时区 timestamp、
  ISO-8601 字符串含 `Z`/偏移量、date；无时区值按 UTC 处理）。
* 指标列必须可转 `float64`（整数允许、布尔拒绝、全 NULL 允许），至多 8 个。
* 列名按**指定字段版**裁决：
  * 版内不存在的列（未知列）→ **整次拒绝（409）**；
  * 列映射到已删除指标 → **整次拒绝（409）**；
  * 版内未包含的指标可以缺列（指标本身可空）。
* 空文件、0 行分片、无法解析的 Parquet → 409；字段版不存在 → 404。
* 失败只回滚事务并清理临时文件，**不改动任何已有字段、数据与快照**。

### 查询与快照语义

* `GET /rows?page=1&page_size=100`：首次调用返回 `snapshot_id` 与 `snapshot_created=true`。
* 之后 `GET /rows?snapshot_id=...&page=2&page_size=100` 始终读同一批分片、同一组字段名；
  新字段在旧分片为 `NULL`，同名复用的新身份在旧分片同样为 `NULL`。
* 不带快照号的新查询会生成新快照，反映最新字段与在供分片（已隔离分片除外）。
* 分片内按 `(station, sampled_at)` 稳定排序后分页；`page_size` 上限 10000。

### 质控隔离（停供裁决）

同一质控样（站点 + UTC 时刻 + 存续指标 ID）在不同分片读数不一致时，
`POST /qc/isolate` 找出**最少应停供的来源分片**：

* 请求体一次提交 **1~20 个质控键**，每个键为
  `{station, sampled_at, indicator_id, tolerance}`：
  * `sampled_at` 为 ISO-8601，支持 `Z`/偏移量，无时区按 UTC；
  * `indicator_id` 必须是**存续**指标（不存在 → 404，已删除 → 409）；
  * `tolerance` 为**非负绝对容差**（负数、NaN → 422）。
* 只评估**在供分片**（已提交且未被隔离；已隔离分片已停供，不再参与），
  读取同键**非 NULL** 值（NaN 视为无效读数，同样不计）。
* **相关分片**（至少持有一条同键非 NULL 读数）**至多 12 个**，超出整次拒绝（409）；
  **缺测键**（任何在供分片都没有该键的非 NULL 读数）整次拒绝（409）。
* 求解规则：
  * 每个键在**剩余分片**中至少保留一条读数，且剩余读数 **最大值 − 最小值 ≤ 容差**；
  * **同一分片内相互矛盾的读数也计入**最大/最小值（自相矛盾的分片只能被隔离）；
  * 隔离数量最少者优先；**数量相同时取分片 ID 升序序列最小**的方案；
  * 不存在可行方案（如唯一来源自相矛盾）→ 整次拒绝（409）。
* 返回逐键的**冲突来源**（每个分片的原始文件名与全部读数、极差、是否在容差内）
  与**被隔离分片**清单。
* 隔离只打标记、**不删文件**：之后 `GET /rows` 新建的**新快照排除已隔离分片**；
  **旧快照**固化的分片清单不变，**仍可分页读取原文件**。
  `GET /qc/isolations` 可随时查看当前停供清单。
* 评估与落库与上传、建快照共用同一把写锁 + 立即事务：交错时彼此只见完整结果；
  任何失败整体回滚，**不改变供数**。

在 `/docs` 中的典型操作流程：先 `GET /rows` 建旧快照 → `POST /qc/isolate` 提交质控键
→ 再 `GET /rows` 建新快照对比两者分片 → 用旧 `snapshot_id` 继续翻页读取原文件。

### 并发与一致性

所有字段变更、上传、快照创建与质控隔离都在同一把进程写锁 + SQLite `BEGIN IMMEDIATE`
事务内裁决：字段变更、上传、隔离与建快照交错时，每个请求看到的都是一个完整的
已提交状态，不存在半份结果；任何失败整体回滚，不改动已有字段、数据、快照与供数。

## 4. 目录结构

```
app/
  config.py    # 常量与数据目录
  db.py        # SQLite 建表、连接、立即事务
  models.py    # Pydantic 模型与错误类型
  catalog.py   # 指标身份与字段版（新增/改名/删除）
  ingest.py    # Parquet 校验、按字段版映射身份、原子落盘
  query.py     # 快照创建与分页合读
  qc.py        # 质控隔离：同键读数扫描与最小停供方案求解
  main.py      # FastAPI 路由与并发控制
tests/         # pytest 自测 + 独立 HTTP 冒烟脚本
Dockerfile
docker-compose.yml
```

## 5. 自测

```bash
pip install -r requirements.txt
pip install pytest httpx
python -m pytest tests/ -v
# 对已运行的服务做独立 HTTP 冒烟：
python tests/manual_smoke.py
```
