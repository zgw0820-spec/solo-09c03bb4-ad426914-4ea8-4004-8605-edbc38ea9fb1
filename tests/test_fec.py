"""Unit tests for the RFC 5109 wire format and the repair-window logic."""

from __future__ import annotations

import json

from app.config import Settings
from app.fec import (
    GROUP_MASK,
    GROUP_SIZE,
    MEDIA_PAYLOAD_TYPE,
    FecRepairer,
    build_fec_packet,
    build_rtp_header,
    parse_fec_packet,
    parse_media_packet,
    reconstruct_media_packet,
)
from app.relay import RelayEngine
from app.table import SubscriptionTable

SSRC = 0x7F00000B  # 127.0.0.11
SOURCE = "127.0.0.11"


def make_media(seq: int, ssrc: int = SSRC, payload: bytes | None = None) -> bytes:
    if payload is None:
        payload = json.dumps({"src": SOURCE, "seq": seq}).encode()
    return build_rtp_header(
        seq=seq,
        timestamp=(SSRC + seq * 3000) & 0xFFFFFFFF,
        ssrc=ssrc,
        payload_type=MEDIA_PAYLOAD_TYPE,
    ) + payload


def make_group(base: int, ssrc: int = SSRC) -> list[bytes]:
    return [make_media(base + i, ssrc=ssrc) for i in range(GROUP_SIZE)]


def make_fec(
    media: list[bytes], fec_seq: int = 0, ssrc: int = SSRC, **kwargs
) -> bytes:
    return build_fec_packet(media, fec_seq=fec_seq, ssrc=ssrc, **kwargs)


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, delta: float) -> None:
        self.now += delta


class Recorder:
    def __init__(self) -> None:
        self.emitted: list[tuple[bytes, str]] = []

    def __call__(self, data: bytes, source: str) -> None:
        self.emitted.append((data, source))


def make_repairer(
    clock: FakeClock, emitted: Recorder, window: float = 0.1
) -> FecRepairer:
    return FecRepairer(emitted, window=window, clock=clock)


def feed_media(
    repairer: FecRepairer, datagram: bytes, source: str = SOURCE
) -> None:
    pkt = parse_media_packet(datagram)
    assert pkt is not None
    repairer.record_media(source, pkt, datagram, emitted=True)


# --------------------------------------------------------------- wire format
def test_media_roundtrip():
    data = make_media(7)
    pkt = parse_media_packet(data)
    assert pkt is not None
    assert pkt.seq == 7
    assert pkt.ssrc == SSRC
    assert pkt.payload_type == MEDIA_PAYLOAD_TYPE
    assert pkt.payload == json.dumps({"src": SOURCE, "seq": 7}).encode()


def test_parse_media_rejects_non_fixed_header():
    assert parse_media_packet(b"short") is None
    assert parse_media_packet(b"\x90" + b"\x00" * 20) is None  # CC != 0
    assert parse_media_packet(b"\x70" + b"\x00" * 20) is None  # version != 2


def test_fec_roundtrip():
    media = make_group(8)
    data = make_fec(media, fec_seq=3)
    fec = parse_fec_packet(data)
    assert fec is not None
    assert fec.ssrc == SSRC
    assert fec.fec_seq == 3
    assert fec.sn_base == 8
    assert fec.mask == GROUP_MASK
    assert fec.protection_length == max(len(m) - 12 for m in media)
    ts = 0
    length = 0
    for packet in media:
        ts ^= int.from_bytes(packet[4:8], "big")
        length ^= len(packet) - 12
    assert fec.ts_recovery == ts
    assert fec.length_recovery == length


def test_parse_fec_rejects_unsupported_forms():
    media = make_group(0)
    good = make_fec(media)
    assert parse_fec_packet(good[:20]) is None  # truncated
    long_mask = bytearray(good)
    long_mask[12] |= 0x40  # L bit: long mask unsupported
    assert parse_fec_packet(bytes(long_mask)) is None
    extension = bytearray(good)
    extension[12] |= 0x80  # E bit
    assert parse_fec_packet(bytes(extension)) is None
    wrong_pt = bytearray(good)
    wrong_pt[1] = MEDIA_PAYLOAD_TYPE
    assert parse_fec_packet(bytes(wrong_pt)) is None


def test_reconstruct_each_position():
    media = make_group(4)
    fec = parse_fec_packet(make_fec(media))
    assert fec is not None
    for missing in range(GROUP_SIZE):
        present = [m for i, m in enumerate(media) if i != missing]
        recovered = reconstruct_media_packet(fec, present, 4 + missing)
        assert recovered == media[missing]


def test_reconstruct_with_uneven_payload_lengths():
    media = [
        make_media(0, payload=b"a"),
        make_media(1, payload=b"bbbbbbbbbb"),
        make_media(2, payload=b"ccc"),
        make_media(3, payload=b"ddddd"),
    ]
    fec = parse_fec_packet(make_fec(media))
    assert fec is not None
    for missing in range(GROUP_SIZE):
        present = [m for i, m in enumerate(media) if i != missing]
        recovered = reconstruct_media_packet(fec, present, missing)
        assert recovered == media[missing]


# ------------------------------------------------------------- repair window
def test_recovers_single_loss_immediately():
    clock, emitted = FakeClock(), Recorder()
    repairer = make_repairer(clock, emitted)
    media = make_group(0)
    for index in (0, 1, 3):  # seq 2 lost on the way
        feed_media(repairer, media[index])
    assert emitted.emitted == []
    repairer.record_fec(make_fec(media), SOURCE)
    assert [data for data, _ in emitted.emitted] == [media[2]]
    stats = repairer.stats()
    assert stats["packets_recovered"] == 1
    assert stats["groups_recovered"] == 1
    assert stats["groups_opened"] == 1


def test_fec_can_arrive_before_media():
    clock, emitted = FakeClock(), Recorder()
    repairer = make_repairer(clock, emitted)
    media = make_group(0)
    repairer.record_fec(make_fec(media), SOURCE)  # opens the group window
    for index in (0, 1, 2):
        feed_media(repairer, media[index])
    # FEC + three media present: the fourth is reconstructed at once.
    assert [data for data, _ in emitted.emitted] == [media[3]]
    # The original fourth packet then counts as already delivered.
    pkt = parse_media_packet(media[3])
    assert pkt is not None
    assert repairer.is_duplicate(SOURCE, pkt) is True


def test_complete_group_needs_no_repair():
    clock, emitted = FakeClock(), Recorder()
    repairer = make_repairer(clock, emitted)
    media = make_group(0)
    for index in range(GROUP_SIZE):
        feed_media(repairer, media[index])
    assert emitted.emitted == []
    assert repairer.stats()["groups_complete"] == 1
    # The repair packet for a complete group is superfluous.
    repairer.record_fec(make_fec(media), SOURCE)
    assert repairer.stats()["fec_unneeded"] == 1
    assert emitted.emitted == []


def test_timeout_stops_repair_and_late_fec_is_ignored():
    clock, emitted = FakeClock(), Recorder()
    repairer = make_repairer(clock, emitted)
    media = make_group(0)
    for index in (0, 1, 3):
        feed_media(repairer, media[index])
    clock.advance(0.11)
    repairer.expire_group((SOURCE, SSRC), 0)
    assert repairer.stats()["groups_expired"] == 1
    assert repairer.stats()["packets_lost"] == 1
    # The repair packet arrives after the deadline: no recovery, no new window.
    repairer.record_fec(make_fec(media), SOURCE)
    assert emitted.emitted == []
    stats = repairer.stats()
    assert stats["fec_late"] == 1
    assert stats["packets_recovered"] == 0


def test_lazy_expiry_on_next_packet():
    clock, emitted = FakeClock(), Recorder()
    repairer = make_repairer(clock, emitted)
    media = make_group(0)
    for index in (0, 1, 3):
        feed_media(repairer, media[index])
    clock.advance(0.2)
    # No timer in sync tests: the next packet of the stream prunes the group.
    feed_media(repairer, make_media(4))
    assert repairer.stats()["groups_expired"] == 1


def test_duplicate_fec_reuses_original_window():
    clock, emitted = FakeClock(), Recorder()
    repairer = make_repairer(clock, emitted)
    media = make_group(0)
    fec = make_fec(media)
    repairer.record_fec(fec, SOURCE)
    clock.advance(0.05)
    repairer.record_fec(fec, SOURCE)  # duplicate: must not extend the window
    assert repairer.stats()["fec_duplicate"] == 1
    clock.advance(0.06)  # 0.11s after the first FEC: original window expired
    for index in (0, 1, 3):
        feed_media(repairer, media[index])
    assert emitted.emitted == []
    assert repairer.stats()["groups_expired"] == 1


def test_two_losses_are_not_recoverable():
    clock, emitted = FakeClock(), Recorder()
    repairer = make_repairer(clock, emitted)
    media = make_group(0)
    for index in (0, 1):  # two packets missing from the group
        feed_media(repairer, media[index])
    repairer.record_fec(make_fec(media), SOURCE)
    assert emitted.emitted == []
    repairer.expire_group((SOURCE, SSRC), 0)
    stats = repairer.stats()
    assert stats["groups_expired"] == 1
    assert stats["packets_lost"] == 2
    assert stats["packets_recovered"] == 0


def test_late_original_after_recovery_is_duplicate():
    clock, emitted = FakeClock(), Recorder()
    repairer = make_repairer(clock, emitted)
    media = make_group(0)
    for index in (0, 1, 3):
        feed_media(repairer, media[index])
    repairer.record_fec(make_fec(media), SOURCE)
    assert len(emitted.emitted) == 1
    pkt = parse_media_packet(media[2])
    assert pkt is not None
    assert repairer.is_duplicate(SOURCE, pkt) is True
    assert repairer.stats()["media_duplicates_suppressed"] == 1


def test_fec_from_other_source_ip_does_not_repair():
    clock, emitted = FakeClock(), Recorder()
    repairer = make_repairer(clock, emitted)
    media = make_group(0)
    for index in (0, 1, 3):
        feed_media(repairer, media[index])
    repairer.record_fec(make_fec(media), "127.0.0.12")  # different source IP
    assert emitted.emitted == []
    repairer.record_fec(make_fec(media), SOURCE)  # same IP + SSRC: repairs
    assert [data for data, _ in emitted.emitted] == [media[2]]


def test_fec_with_other_ssrc_does_not_repair():
    clock, emitted = FakeClock(), Recorder()
    repairer = make_repairer(clock, emitted)
    media = make_group(0)
    for index in (0, 1, 3):
        feed_media(repairer, media[index])
    repairer.record_fec(make_fec(media, ssrc=SSRC + 99), SOURCE)
    assert emitted.emitted == []
    repairer.record_fec(make_fec(media), SOURCE)
    assert len(emitted.emitted) == 1


def test_invalid_fec_is_counted():
    clock, emitted = FakeClock(), Recorder()
    repairer = make_repairer(clock, emitted)
    repairer.record_fec(b"\x80" + b"\x00" * 30, SOURCE)  # wrong mask
    media = make_group(0)
    odd_mask = make_fec(media, mask=0xF100)
    repairer.record_fec(odd_mask, SOURCE)
    unaligned = make_fec(media, sn_base=2)
    repairer.record_fec(unaligned, SOURCE)
    stats = repairer.stats()
    assert stats["fec_invalid"] == 3
    assert stats["fec_packets_in"] == 0


def test_sequence_wraparound_groups():
    clock, emitted = FakeClock(), Recorder()
    repairer = make_repairer(clock, emitted)
    media = make_group(65532)  # {65532..65535}, then the space wraps to 0
    for index in (0, 1, 3):  # 65534 lost
        feed_media(repairer, media[index])
    repairer.record_fec(make_fec(media), SOURCE)
    assert [data for data, _ in emitted.emitted] == [media[2]]
    # Next group after the wrap starts at 0 and repairs the same way.
    nxt = make_group(0)
    for index in (0, 2, 3):  # 1 lost
        feed_media(repairer, nxt[index])
    repairer.record_fec(make_fec(nxt, fec_seq=1), SOURCE)
    assert [data for data, _ in emitted.emitted] == [media[2], nxt[1]]
    assert repairer.stats()["packets_recovered"] == 2


def test_closed_group_is_forgotten_after_ttl():
    clock, emitted = FakeClock(), Recorder()
    repairer = make_repairer(clock, emitted, window=0.1)
    media = make_group(0)
    for index in (0, 1, 3):
        feed_media(repairer, media[index])
    repairer.record_fec(make_fec(media), SOURCE)
    assert repairer.is_duplicate(SOURCE, parse_media_packet(media[2])) is True
    clock.advance(3.0)  # closed_ttl default is 2s
    feed_media(repairer, make_media(4))  # triggers pruning
    assert repairer.is_duplicate(SOURCE, parse_media_packet(media[2])) is False


# ------------------------------------------------------- engine integration
class RecordingTransport:
    def __init__(self) -> None:
        self.sent: list[tuple[bytes, tuple[str, int]]] = []

    def sendto(self, data, addr) -> None:
        self.sent.append((data, addr))


def make_engine() -> RelayEngine:
    settings = Settings(
        classroom_port_min=16000,
        classroom_port_max=16999,
        fec_port=5006,
    )
    engine = RelayEngine(settings)
    engine.set_table(
        SubscriptionTable.build(
            {16001: "127.0.0.11"}, port_min=16000, port_max=16999
        )
    )
    return engine


def test_engine_forwards_then_repairs():
    engine = make_engine()
    transport = RecordingTransport()
    engine._send_transport = transport  # type: ignore[attr-defined]
    media = make_group(0)
    for index in (0, 1, 3):
        engine._handle_datagram(media[index], SOURCE)
    # Media is forwarded immediately on receipt.
    assert [data for data, _ in transport.sent] == [media[0], media[1], media[3]]
    engine._handle_fec_datagram(make_fec(media), SOURCE)
    assert transport.sent[-1] == (media[2], ("239.255.42.10", 16001))
    fec_stats = engine.stats_view()["fec"]
    assert fec_stats["packets_recovered"] == 1
    # A late original copy of the repaired packet is not forwarded again.
    engine._handle_datagram(media[2], SOURCE)
    assert len(transport.sent) == 4
    assert engine.stats_view()["fec"]["media_duplicates_suppressed"] == 1


def test_engine_recovered_packet_uses_table_at_send_time():
    engine = make_engine()
    transport = RecordingTransport()
    engine._send_transport = transport  # type: ignore[attr-defined]
    media = make_group(0)
    for index in (0, 1, 3):
        engine._handle_datagram(media[index], SOURCE)
    assert [addr for _, addr in transport.sent] == [
        ("239.255.42.10", 16001)
    ] * 3
    # Re-subscribe before the repair packet arrives: the recovered copy must
    # follow the table in effect when it is emitted.
    engine.set_table(
        SubscriptionTable.build(
            {16002: "127.0.0.11"}, port_min=16000, port_max=16999
        )
    )
    engine._handle_fec_datagram(make_fec(media), SOURCE)
    assert transport.sent[-1] == (media[2], ("239.255.42.10", 16002))


def test_engine_fec_disabled_by_zero_port():
    settings = Settings(
        classroom_port_min=16000, classroom_port_max=16999, fec_port=0
    )
    engine = RelayEngine(settings)
    assert engine._repairer is None  # noqa: SLF001
    assert engine.stats_view()["fec"] == {"enabled": False}


# --------------------------------------- real sockets, in-process (async)
async def test_recovered_packet_sent_on_current_table_ports_over_udp():
    """Real sockets: repair follows the table swapped in before the FEC."""

    import asyncio
    import socket

    from tests.test_e2e_udp import ClassroomReceiver

    settings = Settings(
        uplink_group="239.255.42.10",
        uplink_port=15120,
        uplink_interface="127.0.0.1",
        multicast_group="239.255.42.10",
        multicast_interface="127.0.0.1",
        classroom_port_min=16040,
        classroom_port_max=16049,
        fec_port=15122,
        fec_window=1.0,  # the test paces packets by hand; keep the window open
        uplink_url="",
    )
    engine = RelayEngine(settings)
    engine.set_table(
        SubscriptionTable.build(
            {16046: "127.0.0.1"}, port_min=16040, port_max=16049
        )
    )
    old_receiver = ClassroomReceiver(16046)
    new_receiver = ClassroomReceiver(16047)
    sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sender.setsockopt(
        socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton("127.0.0.1")
    )
    sender.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
    sender.bind(("127.0.0.1", 0))
    media = make_group(8)
    try:
        await engine.start()
        old_receiver.start()
        new_receiver.start()
        # Three of the four media packets of group {8..11} arrive; 10 is lost.
        for index in (0, 1, 3):
            sender.sendto(media[index], ("239.255.42.10", 15120))
        await asyncio.sleep(0.3)
        assert old_receiver.count() == 3
        assert new_receiver.count() == 0
        # Re-subscribe the classroom to a new port before the repair lands.
        engine.set_table(
            SubscriptionTable.build(
                {16047: "127.0.0.1"}, port_min=16040, port_max=16049
            )
        )
        sender.sendto(make_fec(media), ("239.255.42.10", 15122))
        await asyncio.sleep(0.3)
        # The recovered packet (seq 10) went to the new port only.
        assert new_receiver.count() == 1
        assert new_receiver.packets[0][1]["seq"] == 10
        assert old_receiver.count() == 3
        fec_stats = engine.stats_view()["fec"]
        assert fec_stats["packets_recovered"] == 1
    finally:
        await engine.stop()
        old_receiver.stop()
        new_receiver.stop()
        sender.close()
