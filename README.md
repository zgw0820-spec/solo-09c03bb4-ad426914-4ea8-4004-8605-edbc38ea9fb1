# 教室 IPTV 换订转发器

按**源 IP** 把上联 UDP 组播流转发到订阅该源的教室端口。换订（整批更换教室
订阅表）时保证教室一直有信号：旧源持续转发到新源实际可用，再统一切换，最后
把上联放行范围收窄到新表。

上联 RTP 偶尔丢包时，转发器用 **RFC 5109** 修复包还原丢失的原包：媒体仍走
现有组播口，修复包走另一个 UDP 口；每组 4 个连续序号配一个奇偶校验包，
100ms 窗口内收齐修复包和其余 3 个媒体包就还原缺失的 RTP 原包，并按发送时
的教室表转发。

- 转发器：FastAPI + asyncio 单收单发套接字，Swagger 在 `/docs`
- 上联模拟器：从多个源 IP 向同一组播组发 RTP 媒体包（固定 12 字节头），
  每组 4 包后发一个 RFC 5109 修复包到 FEC 口；可整组放行或精确放行最多两
  个源，可注入「拒绝改表」「改成后丢回执」「组内丢 1~2 包」故障
- 编排：Docker Compose（uplink / relay / classroom 观察器）

## 快速开始

```bash
docker compose up --build
# Swagger
open http://127.0.0.1:8000/docs
# 观察各教室端口正在收哪个源
docker compose logs -f classroom
```

Compose 使用 `network_mode: host`：组播不会跨默认 Docker bridge 转发，模拟器也
需要 `127.0.0.11/12/13` 这些回环别名来模拟不同源 IP。生产网络里上联口与教室
口是分开的，只有转发器需要组播权限。

不用 Docker 时（本仓库的实测方式）：

```bash
pip install -r requirements.txt
SIM_GROUP=239.255.42.10 SIM_PORT=5004 SIM_FEC_PORT=5006 SIM_INTERFACE=127.0.0.1 \
SIM_SOURCES=127.0.0.11,127.0.0.12,127.0.0.13 SIM_INTERVAL=0.02 \
SIM_INITIAL_MODE=allow SIM_INITIAL_SOURCES=127.0.0.11 \
python3 -m uvicorn simulator.main:app --port 9100 &

RELAY_UPLINK_GROUP=239.255.42.10 RELAY_UPLINK_PORT=5004 \
RELAY_UPLINK_INTERFACE=127.0.0.1 RELAY_MULTICAST_GROUP=239.255.42.10 \
RELAY_MULTICAST_INTERFACE=127.0.0.1 RELAY_FEC_PORT=5006 \
RELAY_UPLINK_URL=http://127.0.0.1:9100 \
RELAY_INITIAL_TABLE='16001=127.0.0.11;16002=127.0.0.11;16003=127.0.0.11' \
python3 -m uvicorn app.main:app --port 8000 &

python3 -m scripts.classroom_watcher       # 观察教室端口
python3 -m scripts.demo_switch             # 用 API 演示全部场景
```

`SIM_INTERVAL=0.02`（20ms 一包）是为了让一组 4 包在约 60~80ms 内到齐，落进
100ms 修复窗口；发包更慢时修复窗口会先于整组到达而关闭，需相应调大
`RELAY_FEC_WINDOW`。

`scripts/demo_switch.py` 依次演示：正常换订、准备失败、收窄失败+暂停换订+
重试成功、丢回执仍判成功。同样的调用也可以直接在 Swagger 里点。

## 丢包修复（RFC 5109）

- **媒体面**：选定源的媒体仍走上联组播口（`RELAY_UPLINK_PORT`）。媒体包是
  固定 12 字节头的 RTP（V=2、无 CSRC/扩展/填充），每源独立序号与 SSRC
  （模拟器取 SSRC = 源 IP 的 32 位值）。
- **修复面**：另开 UDP 口（`RELAY_FEC_PORT`，默认 5006，同一组播组）收
  RFC 5109 修复包。修复包为单层、短掩码（L=0，16 位掩码）：SN base = 4 的
  倍数，掩码 `0xF000`（最高位对应 SN base+0），即每组保护 `4k..4k+3` 四个
  连续序号。修复包 SSRC 与媒体流相同。
- **窗口**：每组从首个媒体包**或**修复包到达起计时 100ms
  （`RELAY_FEC_WINDOW`）。媒体包收到就立即转发，同时留在窗口里参与修复
  （已发的包仍能用来还原同组缺失包）。窗口内收齐**同源 IP、同 SSRC** 的修
  复包和其余 3 个媒体包，就按 RFC 5109 §9 还原缺失的 RTP 原包（P/X/CC/
  M/PT、时间戳、长度分别异或还原，负载异或还原），并**按发送时的教室表**
  转发——补包发出前换订了，就发到新表的端口。
- **到期即停补**：窗口关闭的组不再重开；之后到达的修复包按「已收齐=多余
  （`fec_unneeded`）/ 超时关闭=迟到（`fec_late`）」计数。重复的修复包沿用
  原窗口（不重开、不延长，计 `fec_duplicate`）。
- **最多分发一次**：同一媒体序号原包和补包合起来最多发给教室一次；补出后
  迟到的原包被抑制（`media_duplicates_suppressed`）。去重状态在组关闭后保
  留约 2 秒。
- **单包奇偶的边界**：一组丢 1 包可还原；丢 2 包无法还原，窗口到期后计
  `groups_expired` / `packets_lost`。
- **序号回绕**：组按序号空间对齐（`seq & ~3`），`{65532..65535}` 之后自然
  是 `{0..3}`；下一循环复用组基址时，靠已关闭组的保留期（2 秒）区分新旧。

修复计数在 `GET /stats` 的 `fec` 节：`packets_recovered`（补出并转发的原
包数）、`groups_recovered / groups_expired / groups_complete`、
`packets_lost`、`fec_packets_in / fec_invalid / fec_duplicate / fec_late /
fec_unneeded`、`media_duplicates_suppressed`。

### Swagger 演示丢包修复

1. `POST /demo/uplink/faults`，body `{"drop_media": 1}`：模拟器在下一组
   边界起少发 1 包（`drop_media_sources` 可限定源，缺省全部源）。
2. `GET /stats`：`fec.packets_recovered` +1，教室端口序号无缺口。
3. `POST /demo/uplink/faults`，body `{"drop_media": 2}`：一组少发 2 包，
   单层奇偶无法还原。
4. `GET /stats`：`fec.groups_expired`、`fec.packets_lost` 增加，教室端口
   出现 2 个序号缺口。
5. `POST /migrations` 换订后再注入 `drop_media`：补出的包按新表发到教室
   端口（`GET /stats` 的 `per_classroom_port` 同步增长）。

## 换订协议

1. **准备**：把上联放行范围扩大到「旧表源 ∪ 新表源」。教室表不动。
   - 集合 ≤ 2 个源：精确放行；> 2 个源：模拟器只支持整组或最多两个源，
     因此整组放行（`mode=all`）。
   - 失败（被拒绝 / 实际未放行）→ **按旧表照旧发包**，并尝试把上联恢复到旧表
     所需的源；返回 `failed / prepare_failed`。
2. **切换前复核**：重新读取模拟器**实际生效**的过滤；只有确认覆盖两边源才切换。
3. **统一切换**：一次性替换不可变订阅表快照（数据面原子读取，不阻塞）。
4. **收窄**：把上联收窄到完全等于新表源。
   - 失败 → 新表继续转发（教室不断流），报告上联**实际**放行范围，
     `gate.blocked=true`，**暂停新的换订**，直到重试成功。

关键约束：**每个版本的订阅表最多两个源**；一次换订涉及的两个版本合计也最多
两个源（由 `SubscriptionTable` 与迁移请求校验）。需要三个源时，协议用「整组
放行」的短暂窗口完成过渡，收窄后仍回到每版 ≤2 源。

### 丢回执的判定

每次改表后都重新 `GET /sim/filter` 读取实际状态；**不信任回执本身**：

- 回执丢失 + 实际已生效 → 判成功（`ack=lost, verified_by_actual=true`）
- 回执丢失 + 实际未变 → 判失败
- 回执成功 + 实际不符 → 判失败并复核，不标记完成
- 完全读不到实际状态时，只有明确 ACK 才接受，并标记 `verified_by_actual=false`

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/healthz` | 健康检查 |
| GET | `/state` | 当前表、门禁、事件 |
| GET | `/stats` | 收包/转发/按源/按教室端口/FEC 修复统计 |
| POST | `/migrations` | 整批换订：`{"table": {"16001": "127.0.0.12"}, "note": "..."}` |
| GET | `/migrations` | 换订历史 |
| GET | `/migrations/{id}` | 单次换订详情（含每步实际过滤） |
| POST | `/migrations/{id}/retry-narrow` | 重试收窄；成功才恢复换订 |
| POST | `/demo/reset` | 演示用：重置教室表和模拟器 |
| GET/POST | `/demo/uplink/filter` | 查看/直接设置模拟器过滤 |
| POST | `/demo/uplink/faults` | 注入故障，见下 |

故障注入（模拟器 `POST /sim/faults`，转发器有同名代理）：

```json
{"refuse_changes": 1}
{"drop_receipts": 2}
{"refuse_changes": 1, "refuse_only_sources": ["127.0.0.12"]}
{"drop_receipts": 1, "drop_only_sources": ["127.0.0.12"]}
{"drop_media": 1, "drop_media_sources": ["127.0.0.11"]}
{"drop_media": 2}
{"reset": true}
```

`*_only_sources` 只对精确放行到该源集合的请求生效，用来单独命中「收窄」这一步。
`drop_media`（1 或 2）让指定源在下一组边界起少发对应数量的媒体包，用来演示
丢包修复；模拟器侧 `GET /sim/stats` 的 `dropped`、`fec_sent` 可对照。

## 配置

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `RELAY_UPLINK_GROUP` / `RELAY_UPLINK_PORT` | `239.255.42.10` / `5004` | 上联组播地址 |
| `RELAY_UPLINK_INTERFACE` | `127.0.0.1` | 收流网卡/IP |
| `RELAY_MULTICAST_GROUP` | `239.255.42.10` | 教室侧输出组（默认沿用原组） |
| `RELAY_MULTICAST_INTERFACE` | `127.0.0.1` | 发流网卡/IP |
| `RELAY_CLASSROOM_PORT_MIN/MAX` | `16000` / `16999` | 允许的教室端口范围 |
| `RELAY_FEC_PORT` | `5006` | 收 RFC 5109 修复包的 UDP 口（同组播组）；`0`=关闭修复 |
| `RELAY_FEC_WINDOW` | `0.1` | 每组修复窗口（秒），从首个媒体/修复包到达起计 |
| `RELAY_UPLINK_URL` | `http://127.0.0.1:9100` | 模拟器地址；置空=不管理过滤 |
| `RELAY_UPLINK_REQUIRED` | `false` | 启动时模拟器不可达则失败 |
| `RELAY_INITIAL_TABLE` | 空 | `16001=127.0.0.11;16002=127.0.0.11` |
| `RELAY_ALLOWED_SOURCES` | 空 | 可选的本地源白名单（逗号分隔） |

模拟器侧：`SIM_GROUP`、`SIM_PORT`、`SIM_FEC_PORT`、`SIM_INTERFACE`、
`SIM_SOURCES`、`SIM_INTERVAL`、`SIM_INITIAL_MODE/SOURCES`。

## 测试

```bash
pip install -r requirements-dev.txt
python3 -m pytest -q
```

- `tests/test_table.py`、`tests/test_relay.py`：每版 ≤2 源、按源分发、回环自激抑制
- `tests/test_fec.py`：FEC 编解码往返、逐位置/变长负载还原、窗口内补包、
  超时停补、重复修复包沿用原窗口、双丢不可补、异源/异 SSRC 修复包不匹配、
  序号回绕、补包去重、补包按发送时的教室表从真实 UDP 口发出
- `tests/test_migrations.py`：以 mock 模拟器覆盖成功/准备失败/收窄失败/重试/丢回执
- `tests/test_e2e_udp.py`、`tests/test_e2e_signal_continuity.py`：真实进程 + 真实
  UDP 套接字，验证教室端口实际收到的源、切换无断流、丢回执后仍完成
- `tests/test_e2e_fec.py`：真实进程下注入组内丢包，验证单丢修复计数与教室
  序号连续无重复、双丢窗口到期、换订后对新源丢包仍能补发

## 已知限制

- 订阅表保存在进程内存里，重启回到 `RELAY_INITIAL_TABLE`；多副本之间不同步。
- 「暂停换订」只在单个 relay 实例内生效。
- 上联过滤由模拟器的 HTTP 接口管理；真实上联若没有可读回的过滤接口，只能依
  赖 ACK，此时 `verified_by_actual=false`，丢回执会被当作失败。
- 组播回环可能让转发器收到自己发出的包：仅当教室组与上联组相同且端口范围覆盖
  上联端口时按负载内容做短期去重（`EchoGuard`）；开启修复后，RTP 回环包实际
  先被修复窗口的序号去重拦下（同样被丢弃，只是计入
  `media_duplicates_suppressed`）；这不是通用去重机制。
- 修复只覆盖单层奇偶能承担的范围：每组最多丢 1 包；修复包本身丢失等同于该组
  没有保护。媒体格式假设为固定 12 字节头，非 RTP 流量不参与修复（仍照常转发）。
- 已关闭组的去重状态保留约 2 秒：序号空间回绕一圈（65536 包）若快于该保留
  期，旧记录可能把新一周期的同序号包误判为重复（演示速率下不会发生）。
- Compose 依赖 host 网络与 Linux 回环别名，macOS/Windows 上无法直接演示。
