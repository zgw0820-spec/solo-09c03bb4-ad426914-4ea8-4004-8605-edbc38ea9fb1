"""RFC 5109 generic FEC (XOR parity) loss repair for the up-link RTP stream.

Wire format used by this deployment (single protection level, short mask):

* media packets: fixed 12-byte RTP header (V=2, P=0, X=0, CC=0) + payload;
* repair packets: RTP header (payload type ``FEC_PAYLOAD_TYPE``, same SSRC as
  the protected media stream) + 10-octet FEC header (E=0, L=0) + 4-octet
  level-0 header (protection length + 16-bit mask) + level-0 payload.

Each repair packet protects exactly one group of four consecutive media
sequence numbers starting at a multiple of four: SN base = 4k and mask
``0xF000`` (the mask MSB is offset 0 per RFC 5109 section 7.4, so the four
set bits cover SN base+0 .. SN base+3).

Repair policy implemented by :class:`FecRepairer`:

* media datagrams are forwarded immediately and also kept in their group
  window, so packets that were already sent still take part in repair;
* the window opens when the first media *or* repair packet of the group
  arrives and closes ``window`` seconds later ("到期即停补");
* as soon as the repair packet (same source IP, same SSRC) and the three
  remaining media packets of the group are present, the missing RTP packet
  is reconstructed and emitted; a single parity packet can recover at most
  one loss per group;
* a duplicate repair packet reuses the original window (it neither extends
  nor restarts the timer);
* a media sequence number is emitted at most once, original and
  reconstructed copies counted together, for as long as the group state is
  retained;
* 16-bit sequence wrap-around needs no special case: groups are aligned
  windows of four in the sequence space, so the group {65532..65535} is
  simply followed by the group {0..3}; recently closed groups are forgotten
  after ``closed_ttl`` seconds, which is what lets the next cycle reuse the
  same group base.
"""

from __future__ import annotations

import asyncio
import logging
import struct
import time
from dataclasses import dataclass, field
from typing import Callable

log = logging.getLogger(__name__)

RTP_HEADER_LEN = 12
MEDIA_PAYLOAD_TYPE = 96
FEC_PAYLOAD_TYPE = 127
GROUP_SIZE = 4
GROUP_MASK = 0xF000  # short mask, bits for SN base+0..+3 (MSB = offset 0)
_FEC_HEADER_LEN = 10
_LEVEL_HEADER_LEN = 4


def build_rtp_header(
    *,
    seq: int,
    timestamp: int,
    ssrc: int,
    payload_type: int,
    marker: bool = False,
) -> bytes:
    """Fixed 12-byte RTP header (V=2, P=0, X=0, CC=0)."""

    return bytes([0x80, (0x80 if marker else 0) | (payload_type & 0x7F)]) + struct.pack(
        "!HII", seq & 0xFFFF, timestamp & 0xFFFFFFFF, ssrc & 0xFFFFFFFF
    )


@dataclass(frozen=True)
class MediaPacket:
    """Parsed view of a fixed-header media datagram."""

    seq: int
    timestamp: int
    ssrc: int
    payload_type: int
    marker: bool
    payload: bytes


def parse_media_packet(data: bytes) -> MediaPacket | None:
    """Parse a media datagram; ``None`` when it is not fixed-header RTP."""

    if len(data) < RTP_HEADER_LEN or data[0] != 0x80:
        return None
    (seq,) = struct.unpack_from("!H", data, 2)
    timestamp, ssrc = struct.unpack_from("!II", data, 4)
    return MediaPacket(
        seq=seq,
        timestamp=timestamp,
        ssrc=ssrc,
        payload_type=data[1] & 0x7F,
        marker=bool(data[1] & 0x80),
        payload=data[RTP_HEADER_LEN:],
    )


@dataclass(frozen=True)
class FecPacket:
    """Parsed RFC 5109 repair packet (single level, short mask)."""

    ssrc: int
    fec_seq: int
    sn_base: int
    pxcc_recovery: int  # P/X/CC recovery bits (E/L bits already cleared)
    mpt_recovery: int  # M + PT recovery byte
    ts_recovery: int
    length_recovery: int
    protection_length: int
    mask: int
    payload: bytes  # level-0 payload, exactly ``protection_length`` bytes


def parse_fec_packet(
    data: bytes, *, expected_pt: int = FEC_PAYLOAD_TYPE
) -> FecPacket | None:
    """Parse a repair packet; ``None`` when it is not a supported FEC packet."""

    if len(data) < RTP_HEADER_LEN + _FEC_HEADER_LEN + _LEVEL_HEADER_LEN:
        return None
    if data[0] != 0x80 or (data[1] & 0x7F) != expected_pt:
        return None
    (fec_seq,) = struct.unpack_from("!H", data, 2)
    (ssrc,) = struct.unpack_from("!I", data, 8)
    base = RTP_HEADER_LEN
    if data[base] & 0xC0:  # E (extension) or L (long mask): not supported
        return None
    pxcc_recovery = data[base] & 0x3F
    mpt_recovery = data[base + 1]
    (sn_base,) = struct.unpack_from("!H", data, base + 2)
    (ts_recovery,) = struct.unpack_from("!I", data, base + 4)
    (length_recovery,) = struct.unpack_from("!H", data, base + 8)
    protection_length, mask = struct.unpack_from("!HH", data, base + 10)
    payload = data[base + _FEC_HEADER_LEN + _LEVEL_HEADER_LEN :]
    if len(payload) != protection_length:
        return None
    return FecPacket(
        ssrc=ssrc,
        fec_seq=fec_seq,
        sn_base=sn_base,
        pxcc_recovery=pxcc_recovery,
        mpt_recovery=mpt_recovery,
        ts_recovery=ts_recovery,
        length_recovery=length_recovery,
        protection_length=protection_length,
        mask=mask,
        payload=payload,
    )


def build_fec_packet(
    media: list[bytes],
    *,
    fec_seq: int,
    ssrc: int,
    timestamp: int = 0,
    fec_pt: int = FEC_PAYLOAD_TYPE,
    mask: int = GROUP_MASK,
    sn_base: int | None = None,
) -> bytes:
    """Build the single-level short-mask repair packet for ``media``.

    ``media`` holds the original media datagrams (12-byte header + payload)
    of one protection group.  This is the generator side of RFC 5109
    section 8 and is used by the up-link simulator and by tests.
    """

    if not media:
        raise ValueError("a repair packet needs at least one media packet")
    pxcc = 0
    mpt = 0
    ts_recovery = 0
    length_recovery = 0
    longest = 0
    for packet in media:
        if len(packet) < RTP_HEADER_LEN:
            raise ValueError("media packet shorter than the fixed RTP header")
        pxcc ^= packet[0] & 0x3F
        mpt ^= packet[1]
        (pkt_ts,) = struct.unpack_from("!I", packet, 4)
        ts_recovery ^= pkt_ts
        length_recovery ^= len(packet) - RTP_HEADER_LEN
        longest = max(longest, len(packet) - RTP_HEADER_LEN)
    if sn_base is None:
        (sn_base,) = struct.unpack_from("!H", media[0], 2)
    payload = bytearray(longest)
    for packet in media:
        body = packet[RTP_HEADER_LEN:]
        for index, byte in enumerate(body):
            payload[index] ^= byte
    header = build_rtp_header(
        seq=fec_seq, timestamp=timestamp, ssrc=ssrc, payload_type=fec_pt
    )
    fec_header = bytes([pxcc & 0x3F, mpt & 0xFF]) + struct.pack(
        "!HIH", sn_base & 0xFFFF, ts_recovery & 0xFFFFFFFF, length_recovery & 0xFFFF
    )
    level_header = struct.pack("!HH", longest, mask & 0xFFFF)
    return header + fec_header + level_header + bytes(payload)


def reconstruct_media_packet(
    fec: FecPacket, present: list[bytes], missing_seq: int
) -> bytes | None:
    """Rebuild the missing media datagram (RFC 5109 section 9).

    ``present`` are the received media datagrams of the same group (exactly
    three for a single-loss group).  Returns the recovered datagram, or
    ``None`` when the repair data is inconsistent.
    """

    pxcc = fec.pxcc_recovery
    mpt = fec.mpt_recovery
    timestamp = fec.ts_recovery
    length = fec.length_recovery
    for packet in present:
        pxcc ^= packet[0] & 0x3F
        mpt ^= packet[1]
        (pkt_ts,) = struct.unpack_from("!I", packet, 4)
        timestamp ^= pkt_ts
        length ^= len(packet) - RTP_HEADER_LEN
    if length > fec.protection_length:
        return None
    payload = bytearray(fec.payload)
    for packet in present:
        body = packet[RTP_HEADER_LEN:]
        for index, byte in enumerate(body):
            payload[index] ^= byte
    header = bytes([0x80 | (pxcc & 0x3F), mpt & 0xFF]) + struct.pack(
        "!HII", missing_seq & 0xFFFF, timestamp & 0xFFFFFFFF, fec.ssrc
    )
    return header + bytes(payload[:length])


@dataclass
class _GroupState:
    """One protection group: four aligned sequence numbers, one window."""

    base: int
    created_at: float
    media: dict[int, bytes] = field(default_factory=dict)
    delivered: set[int] = field(default_factory=set)
    fec: FecPacket | None = None
    timer: "asyncio.TimerHandle | None" = None


@dataclass
class _StreamState:
    groups: dict[int, _GroupState] = field(default_factory=dict)
    # base -> (closed_at, delivered seqs, closed_complete); bounded by closed_ttl.
    closed: dict[int, tuple[float, set[int], bool]] = field(default_factory=dict)


class FecRepairer:
    """Tracks protection groups per (source IP, SSRC) and repairs losses.

    ``emit`` is called with the reconstructed datagram and the media source
    IP; the caller forwards it by the classroom table in effect at that
    moment.
    """

    def __init__(
        self,
        emit: Callable[[bytes, str], None],
        *,
        window: float = 0.1,
        closed_ttl: float = 2.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._emit = emit
        self._window = window
        self._closed_ttl = closed_ttl
        self._clock = clock or time.monotonic
        self._streams: dict[tuple[str, int], _StreamState] = {}
        self._counters: dict[str, int] = {
            key: 0
            for key in (
                "fec_packets_in",
                "fec_invalid",
                "fec_duplicate",
                "fec_late",
                "fec_unneeded",
                "groups_opened",
                "groups_recovered",
                "groups_complete",
                "groups_expired",
                "packets_recovered",
                "packets_lost",
                "media_duplicates_suppressed",
            )
        }

    # ------------------------------------------------------------- queries
    def is_duplicate(self, source: str, pkt: MediaPacket) -> bool:
        """True when this media packet was already delivered (orig or repair)."""

        stream = self._streams.get((source, pkt.ssrc))
        if stream is None:
            return False
        base = pkt.seq - pkt.seq % GROUP_SIZE
        group = stream.groups.get(base)
        if group is not None and pkt.seq in group.delivered:
            self._counters["media_duplicates_suppressed"] += 1
            return True
        closed = stream.closed.get(base)
        if closed is not None and pkt.seq in closed[1]:
            self._counters["media_duplicates_suppressed"] += 1
            return True
        return False

    def stats(self) -> dict[str, int]:
        view = dict(self._counters)
        view["window_ms"] = int(round(self._window * 1000))
        view["streams_tracked"] = len(self._streams)
        view["groups_open"] = sum(len(s.groups) for s in self._streams.values())
        return view

    def reset_stats(self) -> None:
        for key in self._counters:
            self._counters[key] = 0

    def close(self) -> None:
        for stream in self._streams.values():
            for group in stream.groups.values():
                if group.timer is not None:
                    group.timer.cancel()
                    group.timer = None

    # ----------------------------------------------------------- media path
    def record_media(
        self, source: str, pkt: MediaPacket, data: bytes, *, emitted: bool
    ) -> None:
        """Remember a received media packet; the engine forwards it first."""

        now = self._clock()
        key = (source, pkt.ssrc)
        stream = self._streams.setdefault(key, _StreamState())
        self._prune(stream, now)
        base = pkt.seq - pkt.seq % GROUP_SIZE
        closed = stream.closed.get(base)
        if closed is not None:
            # 到期即停补：窗口已关闭的组不再重开；只补充送达记录用于去重。
            if emitted:
                closed[1].add(pkt.seq)
            self._gc_stream(key, stream)
            return
        group = stream.groups.get(base)
        if group is None:
            group = self._open_group(stream, key, base, now)
        group.media.setdefault(pkt.seq, data)
        if emitted:
            group.delivered.add(pkt.seq)
        if len(group.media) >= GROUP_SIZE:
            self._close_group(stream, group, complete=True)
        else:
            self._maybe_recover(key, stream, group)
        self._gc_stream(key, stream)

    # -------------------------------------------------------------- fec path
    def record_fec(self, data: bytes, source: str) -> None:
        now = self._clock()
        pkt = parse_fec_packet(data)
        if (
            pkt is None
            or pkt.mask != GROUP_MASK
            or pkt.sn_base % GROUP_SIZE != 0
        ):
            self._counters["fec_invalid"] += 1
            return
        self._counters["fec_packets_in"] += 1
        key = (source, pkt.ssrc)
        stream = self._streams.setdefault(key, _StreamState())
        self._prune(stream, now)
        base = pkt.sn_base
        closed = stream.closed.get(base)
        if closed is not None:
            # 修复包到达时窗口已过：不重开（重复/迟到的修复包沿用原窗口）。
            # 组已收齐（或已补出）时修复包是多余的；组因超时关闭则是迟到。
            if closed[2]:
                self._counters["fec_unneeded"] += 1
            else:
                self._counters["fec_late"] += 1
            self._gc_stream(key, stream)
            return
        group = stream.groups.get(base)
        if group is None:
            group = self._open_group(stream, key, base, now)
        if group.fec is not None:
            # Duplicate repair packet: keep the original window untouched.
            self._counters["fec_duplicate"] += 1
            self._gc_stream(key, stream)
            return
        group.fec = pkt
        self._maybe_recover(key, stream, group)
        self._gc_stream(key, stream)

    # -------------------------------------------------------------- expiry
    def expire_group(self, key: tuple[str, int], base: int) -> None:
        """Close a group whose repair window has elapsed (timer callback)."""

        stream = self._streams.get(key)
        if stream is None:
            return
        self._expire_group(stream, base, self._clock())
        self._gc_stream(key, stream)

    # ------------------------------------------------------------- internal
    def _open_group(
        self, stream: _StreamState, key: tuple[str, int], base: int, now: float
    ) -> _GroupState:
        group = _GroupState(base=base, created_at=now)
        stream.groups[base] = group
        self._counters["groups_opened"] += 1
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # no loop (sync unit tests): lazy expiry only
            return group
        group.timer = loop.call_later(self._window, self.expire_group, key, base)
        return group

    def _close_group(
        self, stream: _StreamState, group: _GroupState, *, complete: bool
    ) -> None:
        stream.groups.pop(group.base, None)
        if group.timer is not None:
            group.timer.cancel()
            group.timer = None
        if complete:
            self._counters["groups_complete"] += 1
        stream.closed[group.base] = (self._clock(), group.delivered, True)

    def _expire_group(self, stream: _StreamState, base: int, now: float) -> None:
        group = stream.groups.pop(base, None)
        if group is None:
            return
        if group.timer is not None:
            group.timer.cancel()
            group.timer = None
        missing = GROUP_SIZE - len(group.media)
        if missing > 0:
            self._counters["groups_expired"] += 1
            self._counters["packets_lost"] += missing
            stream.closed[base] = (now, group.delivered, False)
        else:
            self._counters["groups_complete"] += 1
            stream.closed[base] = (now, group.delivered, True)

    def _maybe_recover(
        self, key: tuple[str, int], stream: _StreamState, group: _GroupState
    ) -> None:
        fec = group.fec
        if fec is None:
            return
        base = group.base
        missing = [
            seq
            for seq in range(base, base + GROUP_SIZE)
            if seq not in group.media
        ]
        if len(missing) != 1:
            return  # nothing missing, or more than one loss: parity cannot help
        present = [
            group.media[seq]
            for seq in range(base, base + GROUP_SIZE)
            if seq != missing[0]
        ]
        recovered = reconstruct_media_packet(fec, present, missing[0])
        if recovered is None:
            self._counters["fec_invalid"] += 1
            return
        group.delivered.add(missing[0])
        self._counters["groups_recovered"] += 1
        self._counters["packets_recovered"] += 1
        self._close_group(stream, group, complete=False)
        self._emit(recovered, key[0])

    def _prune(self, stream: _StreamState, now: float) -> None:
        overdue = [
            base
            for base, group in stream.groups.items()
            if now - group.created_at >= self._window
        ]
        for base in overdue:
            self._expire_group(stream, base, now)
        stale = [
            base
            for base, (closed_at, _, _) in stream.closed.items()
            if now - closed_at >= self._closed_ttl
        ]
        for base in stale:
            del stream.closed[base]

    def _gc_stream(self, key: tuple[str, int], stream: _StreamState) -> None:
        if not stream.groups and not stream.closed:
            self._streams.pop(key, None)
