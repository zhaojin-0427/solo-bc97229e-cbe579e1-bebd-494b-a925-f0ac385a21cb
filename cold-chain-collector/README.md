# 冷链温度日志采集系统（Cold-Chain Collector）

网关持续追加 UTF-8 NDJSON 温度日志，本服务监视本地目录，将完整合法的记录
恰好一次入库到 SQLite，供质控系统按运单追溯。支持重命名轮转、半行续写、
坏行隔离、进程重启续读，并提供可演示的 Web 控制台与 HTTP API。

## 目录结构

```
cold-chain-collector/
├── app/                    # Flask 采集服务
│   ├── config.py           # 环境变量配置
│   ├── db.py               # SQLite 连接与表结构（WAL）
│   ├── collector.py        # 核心：轮询读取、续读、轮转、坏行/半行、事务
│   ├── web.py              # HTTP 接口与页面
│   ├── wsgi.py             # 入口
│   └── templates/index.html# 中文 Web 控制台
├── gateway/simulator.py    # 本地网关模拟器（半行 / 重命名轮转 / 坏行注入）
├── tests/                  # 23 个 unittest 用例
├── Dockerfile
├── docker-compose.yml
├── docker-entrypoint.sh
└── run-demo.sh             # 无 Docker 时的本地演示脚本
```

## 一条记录的格式（NDJSON，UTF-8，每行一条，以 `\n` 结束）

```json
{"waybill": "WB-1001", "ts_utc": "2026-11-12T04:42:24.063Z", "temperature": 3.9}
```

* `waybill`：非空字符串，运单号
* `ts_utc`：UTC 时间（必须带 UTC 时区，`Z` 或 `+00:00`），非 UTC/无时区一律拒收
* `temperature`：数值（布尔值、NaN、Infinity 拒收）

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 浏览器打开
#   http://localhost:8000
```

两个服务共享一个命名卷 `gateway-data`：

| 服务 | 作用 |
| --- | --- |
| `web` | 采集 + Web/API，端口 8000，启动即自动采集 |
| `gateway` | 网关模拟器，每秒写 1 条；每 20 行重命名轮转；每 7 条制造一次半行；每 11 条写一条完整坏行 |

数据持久化在命名卷 `gateway-data`（日志）与 `collector-data`（SQLite）中，
`docker compose down` 后数据保留；`docker compose down -v` 清空。

常用操作：

```bash
docker compose logs -f gateway          # 观察 half-line / rotated 日志
docker compose restart web              # 模拟采集进程重启，验证自动续读
docker compose exec web ls -l /data/gateway
```

## 无 Docker 本地运行

```bash
pip install -r requirements.txt
./run-demo.sh                  # 自动启动模拟器 + Web，数据在 ./_demo_data
# 或分别启动：
python3 gateway/simulator.py --watch-dir ./data/gateway
WATCH_DIR=./data/gateway DB_PATH=./data/collector.db python3 -m app.wsgi
```

## 运行测试

```bash
pip install -r requirements.txt
python3 -m unittest discover -s tests -v
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | Web 控制台（进度、坏行、运单查询，2 秒自动刷新） |
| GET | `/healthz` | 健康检查 |
| GET | `/api/status` | 采集线程状态、各文件读取进度、汇总计数 |
| POST | `/api/start` / `/api/stop` | 启动 / 停止采集 |
| GET | `/api/errors?limit=100` | 坏行（含文件、行号、字节偏移、原因、原文） |
| GET | `/api/waybills` | 全部运单汇总 |
| GET | `/api/waybills/<运单号>?limit=500` | 按运单取数，附 min/max/avg 统计 |

示例：

```bash
curl http://localhost:8000/api/waybills/WB-1001
```

## 关键设计与正确性保证

### 1. 文件身份 = `(st_dev, st_ino)`，不依赖文件名

轮转方式为“旧文件改名保留、新文件沿用原名”。采集器以 inode 身份为每个
文件维护独立进度：改名后的旧文件继续按原进度读完，新文件从 0 开始；
旧文件不会被删除，未采完的旧文件状态为 `rotated_pending`，补齐后变
`completed`。

### 2. 只处理以换行结束的记录，半行留待续写

进度只推进到最后一个 `\n` 之后。无换行结尾的内容留在文件里，下一轮（或
重启后）从已提交偏移重新读取拼接，既不丢失也不提前入库。页面上
`pending_bytes` 即当前半行字节数。

### 3. 完整坏行记录位置与原因后跳过

JSON 解析失败、字段缺失、时间非 UTC、温度非数值、UTF-8 非法、超长行等，
写入 `ingest_errors`（含行号与字节偏移），不影响后续行；坏行同样占用源
文件行号。

### 4. 恰好一次（exactly-once）

* 入库行、坏行、读取进度在**同一个 SQLite 事务**提交（WAL 模式）；
* 崩溃只会回滚到上一个提交点，重启后按 `(dev, ino)` + `committed_off`
  重读未提交部分，因此每条完整合法行恰好入库一次；
* 相同内容的不同原始行是独立行，分别保留（无去重逻辑）。

### 5. 内存安全

分块（64 KiB）流式读取；无换行且超过 `MAX_LINE_BYTES` 的行不会常驻内存，
跳过状态持久化在进度表中，跨 tick / 跨进程依然有效，换行到达时记一条坏行。

## 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WATCH_DIR` | `/data/gateway` | 监看目录 |
| `ACTIVE_FILE` | `temperature.log` | 活动文件名 |
| `DB_PATH` | `/data/collector/collector.db` | SQLite 路径 |
| `POLL_INTERVAL` | `1` | 轮询间隔秒 |
| `BATCH_SIZE` | `200` | 每事务最大行数 |
| `MAX_LINE_BYTES` | `1048576` | 单行最大字节，超过记坏行 |
| `AUTOSTART` | `true` | 进程启动即采集 |
| 模拟器 `ROTATE_EVERY` | `20` | 每 N 行重命名轮转，0 关闭 |
| 模拟器 `HALF_LINE_EVERY` | `7` | 每 N 行拆成两次写（无换行半行），0 关闭 |
| 模拟器 `BAD_LINE_EVERY` | `11` | 每 N 行写一条完整坏行，0 关闭 |

## 数据表

* `readings`：入库温度记录，带来源文件、inode、源行号与原文
* `ingest_errors`：坏行（行号、字节偏移、原因、原文截断）
* `ingest_progress`：每个文件身份的已提交偏移、行数、半行字节、跳过状态、
  状态（`active` / `rotated` / `rotated_pending` / `completed`）
