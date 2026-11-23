"""Local up-link simulator.

Sends RTP media to the same multicast group from several different source IPs
(loopback aliases by default) and exposes a filter API with injectable faults:

* ``refuse_changes`` – reject the next N filter changes with HTTP 409 without
  applying anything.
* ``drop_receipts`` – apply the next N filter changes, then drop the
  connection before the acknowledgment is delivered.  The applied filter is
  still visible via ``GET /sim/filter``, so a correct relay can judge by the
  actual state instead of the missing receipt.
* ``drop_media`` – drop the next 1-2 media packets inside one protection
  group (aligned to the group boundary), simulating up-link RTP loss.

Media packets carry a fixed 12-byte RTP header (per-source sequence numbers
and SSRC).  After every group of four media packets, one RFC 5109 repair
packet (single level, short mask) is sent to the same group on the FEC port.
The wire format helpers are shared with the relay in ``app.fec``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from ipaddress import IPv4Address
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator

from app.fec import (
    GROUP_SIZE,
    MEDIA_PAYLOAD_TYPE,
    build_fec_packet,
    build_rtp_header,
)

log = logging.getLogger(__name__)


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value is None or value == "" else value


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


@dataclass
class FaultState:
    refuse_changes: int = 0
    drop_receipts: int = 0
    refuse_only_sources: list[str] | None = None
    drop_only_sources: list[str] | None = None

    def matches(self, wanted: list[str] | None, payload: FilterRequest) -> bool:
        if wanted is None:
            return True
        return payload.mode == "allow" and sorted(set(payload.sources)) == sorted(
            set(wanted)
        )


@dataclass
class FilterState:
    mode: str = "all"
    sources: list[str] = field(default_factory=list)
    revision: int = 0

    def allows(self, source: str) -> bool:
        return self.mode == "all" or source in self.sources

    def view(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "sources": list(self.sources) if self.mode == "allow" else [],
            "revision": self.revision,
        }


@dataclass
class EmitterConfig:
    group: str = "239.255.42.10"
    port: int = 5004
    interface: str = "127.0.0.1"
    sources: tuple[str, ...] = ("127.0.0.11", "127.0.0.12", "127.0.0.13")
    interval: float = 0.2
    fec_port: int = 5006


@dataclass
class RtpSourceState:
    """Per-source RTP emitter state (sequence space is per SSRC)."""

    ssrc: int
    seq: int = 0
    fec_seq: int = 0
    group: list[bytes] = field(default_factory=list)
    drop_pending: int = 0
    drops_running: int = 0


class FilterRequest(BaseModel):
    mode: str = Field(..., pattern="^(all|allow)$")
    sources: list[str] = Field(default_factory=list)

    @field_validator("sources")
    @classmethod
    def _validate(cls, value: list[str]) -> list[str]:
        if len(value) > 2:
            raise ValueError("at most two explicit sources may be allowed")
        import ipaddress

        for item in value:
            ipaddress.IPv4Address(item)
        return value


class FaultsRequest(BaseModel):
    refuse_changes: int = Field(default=0, ge=0)
    drop_receipts: int = Field(default=0, ge=0)
    refuse_only_sources: list[str] | None = Field(
        default=None,
        description=(
            "仅当请求的精确放行源集合与这里给出的完全一致时才消耗一次"
            "拒绝计数（用于只拒绝收窄、不影响准备）"
        ),
    )
    drop_only_sources: list[str] | None = Field(
        default=None,
        description="仅当请求的精确放行源集合与这里一致时才丢回执",
    )
    drop_media: int = Field(
        default=0,
        ge=0,
        le=2,
        description=(
            "让指定源在一组（4 个连续序号）内少发 1 或 2 个媒体包；"
            "丢包从下一个组边界开始"
        ),
    )
    drop_media_sources: list[str] | None = Field(
        default=None,
        description="drop_media 只作用于这些源 IP；缺省为全部源",
    )
    reset: bool = False


class SimulatorState:
    def __init__(self) -> None:
        initial_mode = _env("SIM_INITIAL_MODE", "all")
        initial_sources = [
            item.strip()
            for item in _env("SIM_INITIAL_SOURCES", "").split(",")
            if item.strip()
        ]
        self.filter = FilterState(
            mode=initial_mode if initial_mode in {"all", "allow"} else "all",
            sources=sorted(initial_sources) if initial_mode == "allow" else [],
        )
        self.faults = FaultState()
        self.lock = asyncio.Lock()
        self.sent: dict[str, int] = {}
        self.blocked: dict[str, int] = {}
        self.dropped: dict[str, int] = {}
        self.fec_sent: dict[str, int] = {}
        self.last_sent: dict[str, dict[str, Any]] = {}
        self.last_dropped: dict[str, dict[str, Any]] = {}
        self.started_at = time.time()
        self.emitter_task: asyncio.Task | None = None
        self.config = EmitterConfig(
            group=_env("SIM_GROUP", EmitterConfig.group),
            port=int(_env("SIM_PORT", str(EmitterConfig.port))),
            interface=_env("SIM_INTERFACE", EmitterConfig.interface),
            sources=tuple(
                item.strip()
                for item in _env(
                    "SIM_SOURCES", ",".join(EmitterConfig.sources)
                ).split(",")
                if item.strip()
            ),
            interval=_env_float("SIM_INTERVAL", EmitterConfig.interval),
            fec_port=int(_env("SIM_FEC_PORT", str(EmitterConfig.fec_port))),
        )
        self._sockets: dict[str, socket.socket] = {}
        self._rtp: dict[str, RtpSourceState] = {}

    def rtp_state(self, source: str) -> RtpSourceState:
        state = self._rtp.get(source)
        if state is None:
            state = RtpSourceState(ssrc=int(IPv4Address(source)))
            self._rtp[source] = state
        return state

    def open_sockets(self) -> None:
        for source in self.config.sources:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((source, 0))
            sock.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_MULTICAST_IF,
                socket.inet_aton(self.config.interface),
            )
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
            self._sockets[source] = sock

    def close_sockets(self) -> None:
        for sock in self._sockets.values():
            sock.close()
        self._sockets.clear()

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        self.open_sockets()
        try:
            while True:
                now_ms = int(time.time() * 1000)
                for source in self.config.sources:
                    allowed = self.filter.allows(source)
                    if not allowed:
                        self.blocked[source] = self.blocked.get(source, 0) + 1
                        continue
                    rtp = self.rtp_state(source)
                    seq = rtp.seq
                    timestamp = (rtp.ssrc + seq * 3000) & 0xFFFFFFFF
                    payload = json.dumps(
                        {
                            "src": source,
                            "seq": seq,
                            "sent_at_ms": now_ms,
                            "sim_source": "uplink",
                        },
                        separators=(",", ":"),
                    ).encode()
                    datagram = build_rtp_header(
                        seq=seq,
                        timestamp=timestamp,
                        ssrc=rtp.ssrc,
                        payload_type=MEDIA_PAYLOAD_TYPE,
                    ) + payload
                    rtp.group.append(datagram)
                    rtp.seq = (seq + 1) & 0xFFFF
                    sock = self._sockets[source]

                    # Injected loss: start dropping at a group boundary so
                    # the 1-2 missing packets fall inside one group.
                    if rtp.drop_pending and seq % GROUP_SIZE == 0:
                        rtp.drops_running = rtp.drop_pending
                        rtp.drop_pending = 0
                    if rtp.drops_running:
                        rtp.drops_running -= 1
                        self.dropped[source] = self.dropped.get(source, 0) + 1
                        self.last_dropped[source] = {
                            "seq": seq,
                            "at_ms": now_ms,
                        }
                    else:
                        try:
                            await loop.run_in_executor(
                                None,
                                sock.sendto,
                                datagram,
                                (self.config.group, self.config.port),
                            )
                        except OSError as exc:  # pragma: no cover
                            log.warning("emit from %s failed: %s", source, exc)
                        else:
                            self.sent[source] = self.sent.get(source, 0) + 1
                            self.last_sent[source] = {
                                "seq": seq,
                                "at_ms": now_ms,
                                "bytes": len(datagram),
                            }

                    if len(rtp.group) == GROUP_SIZE:
                        # The repair packet protects the original four media
                        # packets, including any that were dropped on the way.
                        fec = build_fec_packet(
                            rtp.group,
                            fec_seq=rtp.fec_seq,
                            ssrc=rtp.ssrc,
                            timestamp=timestamp,
                        )
                        rtp.group = []
                        rtp.fec_seq = (rtp.fec_seq + 1) & 0xFFFF
                        try:
                            await loop.run_in_executor(
                                None,
                                sock.sendto,
                                fec,
                                (self.config.group, self.config.fec_port),
                            )
                        except OSError as exc:  # pragma: no cover
                            log.warning(
                                "fec emit from %s failed: %s", source, exc
                            )
                        else:
                            self.fec_sent[source] = (
                                self.fec_sent.get(source, 0) + 1
                            )
                await asyncio.sleep(self.config.interval)
        finally:
            self.close_sockets()


state = SimulatorState()


class _InjectedFaultTracebackFilter(logging.Filter):
    """Keep the intentional receipt-drop abort out of uvicorn's error log.

    Dropping a receipt means the connection is cut after the filter change has
    already been applied.  The ASGI stack necessarily sees that as an aborted
    response; the simulator logs a one-line note instead of a traceback.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if record.exc_info and record.exc_info[1] is not None:
            return "injected fault: receipt dropped" not in str(
                record.exc_info[1]
            )
        return True


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("uvicorn.error").addFilter(_InjectedFaultTracebackFilter())
    state.started_at = time.time()
    state.emitter_task = asyncio.create_task(state.run(), name="sim-emitter")
    log.info(
        "up-link simulator emitting %s:%s from %s every %.2fs",
        state.config.group,
        state.config.port,
        ", ".join(state.config.sources),
        state.config.interval,
    )
    try:
        yield
    finally:
        if state.emitter_task is not None:
            state.emitter_task.cancel()
            with suppress(asyncio.CancelledError):
                await state.emitter_task
            state.emitter_task = None


app = FastAPI(
    title="本地 IPTV 上联模拟器",
    version="1.0.0",
    description=(
        "从不同源 IP 向同一组播地址发包；可整组放行或精确放行最多两个源；"
        "可注入“拒绝改表”和“改成后丢回执”故障。"
    ),
    lifespan=lifespan,
)


@app.get("/healthz", summary="健康检查")
async def healthz() -> dict[str, Any]:
    return {
        "status": "ok",
        "emitting": state.emitter_task is not None
        and not state.emitter_task.done(),
        "group": state.config.group,
        "port": state.config.port,
        "fec_port": state.config.fec_port,
        "sources": list(state.config.sources),
        "interval": state.config.interval,
    }


@app.get("/sim/filter", summary="读取实际生效的上联过滤")
async def get_filter() -> dict[str, Any]:
    return state.filter.view()


@app.get("/sim/faults", summary="读取故障注入剩余计数")
async def get_faults() -> dict[str, Any]:
    return {
        "refuse_changes": state.faults.refuse_changes,
        "drop_receipts": state.faults.drop_receipts,
        "refuse_only_sources": state.faults.refuse_only_sources,
        "drop_only_sources": state.faults.drop_only_sources,
        "drop_media_pending": {
            source: rtp.drop_pending + rtp.drops_running
            for source, rtp in state._rtp.items()
            if rtp.drop_pending or rtp.drops_running
        },
    }


@app.post("/sim/faults", summary="设置故障注入计数")
async def set_faults(payload: FaultsRequest) -> dict[str, Any]:
    async with state.lock:
        if payload.reset:
            state.faults = FaultState()
            for rtp in state._rtp.values():
                rtp.drop_pending = 0
                rtp.drops_running = 0
        if payload.refuse_changes:
            state.faults.refuse_changes = payload.refuse_changes
            state.faults.refuse_only_sources = payload.refuse_only_sources
        if payload.drop_receipts:
            state.faults.drop_receipts = payload.drop_receipts
            state.faults.drop_only_sources = payload.drop_only_sources
        if payload.drop_media:
            targets = payload.drop_media_sources or list(state.config.sources)
            for source in targets:
                state.rtp_state(source).drop_pending += payload.drop_media
        return {
            "refuse_changes": state.faults.refuse_changes,
            "drop_receipts": state.faults.drop_receipts,
            "refuse_only_sources": state.faults.refuse_only_sources,
            "drop_only_sources": state.faults.drop_only_sources,
            "drop_media_pending": {
                source: rtp.drop_pending + rtp.drops_running
                for source, rtp in state._rtp.items()
                if rtp.drop_pending or rtp.drops_running
            },
        }


@app.post("/sim/filter", summary="修改上联过滤（可注入故障）")
async def set_filter(payload: FilterRequest):
    async with state.lock:
        if state.faults.refuse_changes > 0 and state.faults.matches(
            state.faults.refuse_only_sources, payload
        ):
            state.faults.refuse_changes -= 1
            log.info("injected refusal: filter left at %s", state.filter.view())
            raise HTTPException(
                status_code=409,
                detail=(
                    "injected fault: simulator refused to change the filter; "
                    "actual filter unchanged"
                ),
            )

        if payload.mode == "allow" and not payload.sources:
            raise HTTPException(
                status_code=422,
                detail="mode=allow requires at least one source",
            )

        state.filter.mode = payload.mode
        state.filter.sources = sorted(set(payload.sources))
        state.filter.revision += 1
        applied = state.filter.view()
        log.info("filter applied: %s", applied)

        if state.faults.drop_receipts > 0 and state.faults.matches(
            state.faults.drop_only_sources, payload
        ):
            state.faults.drop_receipts -= 1
            log.info("injected fault: dropping receipt for %s", applied)

            async def broken_body():
                yield json.dumps(applied).encode()[:12]
                await asyncio.sleep(0.05)
                raise RuntimeError("injected fault: receipt dropped")

            return StreamingResponse(
                broken_body(), media_type="application/json"
            )

        return applied


@app.get("/sim/stats", summary="模拟器发包与拦截统计")
async def get_stats() -> dict[str, Any]:
    return {
        "uptime_s": round(time.time() - state.started_at, 3),
        "filter": state.filter.view(),
        "faults": {
            "refuse_changes": state.faults.refuse_changes,
            "drop_receipts": state.faults.drop_receipts,
        },
        "sent": dict(state.sent),
        "blocked": dict(state.blocked),
        "dropped": dict(state.dropped),
        "fec_sent": dict(state.fec_sent),
        "last_sent": dict(state.last_sent),
        "last_dropped": dict(state.last_dropped),
        "config": {
            "group": state.config.group,
            "port": state.config.port,
            "fec_port": state.config.fec_port,
            "interface": state.config.interface,
            "sources": list(state.config.sources),
            "interval": state.config.interval,
        },
    }
