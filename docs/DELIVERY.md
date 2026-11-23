# 交付说明

## 做了什么

在原有换订转发器上增加 **RFC 5109 丢包修复**：上联 RTP 媒体仍走现有组播口，
另开 UDP 口收修复包；每组 4 个连续序号（4 的倍数起）配一个单层短掩码奇偶
包，100ms 窗口内收齐修复包和其余 3 个媒体包就还原缺失的 RTP 原包，按发送
时的教室表转发。

**转发器（`app/`）**

- `fec.py`（新增）：
  - 线格式：固定 12 字节头媒体包的解析；RFC 5109 修复包（RTP 头 + 10 字节
    FEC 头[E=0,L=0] + 4 字节 level-0 头 + 异或负载）的解析/生成/还原
    （§8/§9：P/X/CC/M/PT、TS、长度异或恢复，SN 取缺失序号，SSRC 同媒体流，
    负载按 protection length 补零异或）。掩码 `0xF000`（MSB 对应 SN base+0）。
  - `FecRepairer`：按（源 IP, SSRC）分流；组 = 序号空间对齐的 4 窗
    （`seq & ~3`）。窗口从首个媒体**或**修复包到达起计 100ms；媒体包先转发
    再入窗（已发的包仍参与修复）；FEC+3 媒体到齐立即还原；到期关闭不再重
    开；重复修复包沿用原窗口（计 `fec_duplicate`）；已关闭组的 FEC 按
    「收齐=多余 / 超时=迟到」分别计 `fec_unneeded` / `fec_late`；同一序号
    原包+补包合计最多分发一次（开窗与已关窗都查重，关窗状态保留 2 秒）。
    序号回绕无需特判：`{65532..65535}` 之后是 `{0..3}`，下一循环靠保留期
    区分。
- `relay.py`：新增 FEC 接收套接字（同组播组、`fec_port`，`0` 关闭修复，
  且不允许与上联口同号）；媒体路径先查重、再照常转发、最后入窗；补出的包
  经 `_emit_recovered` **读取发送时的教室表**发出，同样计入转发统计与回环
  抑制。`/stats` 增加 `fec` 节。
- `config.py`：`RELAY_FEC_PORT`（默认 5006）、`RELAY_FEC_WINDOW`（默认 0.1）。
- `main.py`：`/demo/uplink/faults` 代理模型新增 `drop_media`（1~2）、
  `drop_media_sources`。
- 既有换订状态机（准备→复核→切换→收窄、丢回执按实际过滤判定）未改动。

**模拟器（`simulator/`）**

- 改发 RTP 媒体：固定 12 字节头、每源独立序号与 SSRC（= 源 IP 的 32 位值）、
  PT=96，负载仍是原 JSON（教室观察器/e2e 剥头后解析）。
- 每齐 4 包生成一个 RFC 5109 修复包发到同组播组的 `SIM_FEC_PORT`（默认
  5006），FEC 保护的是**原始** 4 包（含被丢的），源 IP/SSRC 与媒体一致。
- 新故障 `drop_media`（1 或 2，可限定源）：从下一个组边界起少发对应数量的
  媒体包，保证丢包落在同一组内；`/sim/stats` 增加 `dropped`、`fec_sent`、
  `last_dropped`。

**编排与演示**

- `docker-compose.yml`：`SIM_FEC_PORT`/`RELAY_FEC_PORT` 对齐 5006；
  `SIM_INTERVAL` 降为 0.02（一组 4 包约 60~80ms 到齐，落在 100ms 窗口内）。
- `scripts/classroom_watcher.py`、e2e 接收器：剥 12 字节 RTP 头后解析 JSON。
- `README.md`：修复设计、窗口语义、计数器、Swagger 演示步骤、配置表。

## 测出什么

- `python3 -m pytest -q` → **59 passed**（约 44 秒），pyflakes 干净。
  - `tests/test_fec.py`（23）：编解码往返、逐位置与变长负载逐字节还原、
    窗口内补包、FEC 先于媒体到达、超时停补且迟到 FEC 不重开、重复 FEC 沿用
    原窗口（注入 0.05s 后窗口仍按首次到达计）、双丢不可补、异源 IP / 异
    SSRC 的修复包不匹配、序号回绕两组连续修复、补出后迟到原包被抑制、
    关闭组 2 秒保留期后遗忘、非法掩码/未对齐 SN base/截断包计
    `fec_invalid`；引擎级：媒体先转发后入窗、补包发到换订后的新教室端口；
    进程内真实套接字：换表后补包只从新端口出、旧端口无补包。
  - `tests/test_e2e_fec.py`（3，真实进程+真实 UDP）：Swagger 代理注入
    `drop_media=1` → `packets_recovered`+1、教室序号连续且无重复；
    `drop_media=2` → `groups_expired`/`packets_lost` 增加、教室恰缺同组 2
    包；换订到 127.0.0.12 后再丢 1 包 → 仍补出发到原教室端口。
  - 原 33 个换订/转发/迁移测试全部保持通过。
- 手工实测（compose 同款环境变量、真实进程，经 Swagger 代理操作）：
  - `{"drop_media": 1}` → `groups_recovered=1, packets_recovered=1`；
  - `{"drop_media": 2}` → `groups_expired`+1、`packets_lost`+2；
  - 换订 16001→127.0.0.12 后教室端口实测：`.11` 收 49 包、`.12` 收 246 包，
    均无重复无缺口（含对新源注入的 1 包丢失被补回），`packets_recovered`
    随之 +1；
  - 无丢包时 `fec_unneeded` 随组收齐增长（修复包多余），`fec_late=0`；
    换订收窄瞬间被阻断源的半组会过期计入 `packets_lost`（上行的真实表现，
    非误报）。
- Swagger `/docs` 的 `FaultRequest` 已含 `drop_media` / `drop_media_sources`。

## 没完成 / 已知限制

- 本环境没有 Docker，Compose 只做了 YAML 结构与端口对齐校验；容器内启动未
  实测（与上期相同）。手工实测走的是「不用 Docker」路径。
- 单层奇偶只能补每组 1 包；修复包本身丢失等同于该组无保护；长掩码、多层
  （ULP 级别）与 E 扩展收到即计 `fec_invalid`，不支持。
- 媒体假设固定 12 字节头（无 CSRC/扩展/填充）；非 RTP 流量不参与修复（仍照
  常转发）。PT 固定（媒体 96 / 修复 127），未做 SDP 协商。
- 修复窗口 100ms 是硬约束：发包间隔过大（一组跨 >100ms）时永远补不上，需调
  `RELAY_FEC_WINDOW`；演示 compose 用 20ms 间隔。
- 已关闭组的去重状态保留约 2 秒：序号回绕一圈（65536 包）若快于 2 秒
  （>3 万包/秒），新周期同序号包可能被误判重复；演示/IPTV 速率下不会发生。
- 换订/启停瞬间，被阻断或新加入源的半组会按超时计入 `packets_lost`（上行
  真实表现）；模拟器被过滤的源不推进序号，恢复放行后流是连续的。
- 订阅表仍在进程内存，重启回到 `RELAY_INITIAL_TABLE`；多副本不同步，换订门
  禁只在单实例内生效。
- 真实上联若没有可回读的过滤接口，只能信 ACK；丢回执会被判失败。
- 未做鉴权、限流、持久化；`demo/*` 接口只适合本地演示。
