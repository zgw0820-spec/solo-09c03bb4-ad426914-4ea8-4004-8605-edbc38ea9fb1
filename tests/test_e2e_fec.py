"""End-to-end FEC repair tests with real processes and real UDP sockets.

The up-link simulator emits RTP media (fixed 12-byte header) and one RFC 5109
repair packet per group of four media packets.  Fault injection drops one or
two media packets inside a group; the relay must reconstruct a single loss
within the 100 ms window and must give up (counting the loss) when two
packets of the same group are missing.
"""

from __future__ import annotations

import time
from pathlib import Path

import httpx

from tests.test_e2e_udp import (
    CLASSROOM_GROUP,
    UPLINK_GROUP,
    ClassroomReceiver,
    free_ports,
    run_service,
    wait_http,
)

ROOT = Path(__file__).resolve().parents[1]
UPLINK_PORT = 15130
FEC_PORT = 15132
CLASSROOM_PORT = 16046


def _sim_env() -> dict[str, str]:
    return {
        "SIM_GROUP": UPLINK_GROUP,
        "SIM_PORT": str(UPLINK_PORT),
        "SIM_FEC_PORT": str(FEC_PORT),
        "SIM_INTERFACE": "127.0.0.1",
        "SIM_SOURCES": "127.0.0.11,127.0.0.12",
        # A group of four spans ~60ms at this interval: inside the 100ms
        # repair window with margin.
        "SIM_INTERVAL": "0.02",
        "SIM_INITIAL_MODE": "allow",
        "SIM_INITIAL_SOURCES": "127.0.0.11",
    }


def _relay_env(sim_port: int) -> dict[str, str]:
    return {
        "RELAY_UPLINK_GROUP": UPLINK_GROUP,
        "RELAY_UPLINK_PORT": str(UPLINK_PORT),
        "RELAY_UPLINK_INTERFACE": "127.0.0.1",
        "RELAY_MULTICAST_GROUP": CLASSROOM_GROUP,
        "RELAY_MULTICAST_INTERFACE": "127.0.0.1",
        "RELAY_CLASSROOM_PORT_MIN": "16040",
        "RELAY_CLASSROOM_PORT_MAX": "16049",
        "RELAY_FEC_PORT": str(FEC_PORT),
        "RELAY_FEC_WINDOW": "0.1",
        "RELAY_UPLINK_URL": f"http://127.0.0.1:{sim_port}",
        "RELAY_UPLINK_REQUIRED": "true",
        "RELAY_INITIAL_TABLE": f"{CLASSROOM_PORT}=127.0.0.11",
    }


def _fec_stats(base: str) -> dict:
    return httpx.get(f"{base}/stats", timeout=5).json()["fec"]


def _delta(now: dict, baseline: dict, key: str) -> int:
    return now[key] - baseline[key]


def _seqs(receiver: ClassroomReceiver, source: str) -> list[int]:
    return [
        packet["seq"]
        for _, packet in receiver.packets
        if packet.get("src") == source and isinstance(packet.get("seq"), int)
    ]


def _assert_contiguous_and_unique(seqs: list[int]) -> None:
    assert seqs, "no packets received"
    assert len(seqs) == len(set(seqs)), "duplicate media packets delivered"
    assert sorted(seqs) == list(range(min(seqs), max(seqs) + 1)), (
        "gaps in the delivered sequence"
    )


def test_fec_repairs_single_packet_loss(tmp_path):
    sim_port, relay_port = free_ports(2)
    receiver = ClassroomReceiver(CLASSROOM_PORT)
    receiver.start()
    try:
        with run_service(
            "simulator.main:app", sim_port, _sim_env(), tmp_path / "sim.log"
        ) as sim_process:
            wait_http(
                f"http://127.0.0.1:{sim_port}/healthz",
                process=sim_process,
                log_path=tmp_path / "sim.log",
            )
            with run_service(
                "app.main:app", relay_port, _relay_env(sim_port),
                tmp_path / "relay.log",
            ) as relay_process:
                base = f"http://127.0.0.1:{relay_port}"
                wait_http(
                    f"{base}/healthz",
                    process=relay_process,
                    log_path=tmp_path / "relay.log",
                )
                time.sleep(0.6)
                baseline = _fec_stats(base)
                assert baseline["enabled"] is True
                assert baseline["fec_packets_in"] > 0

                # Drop one media packet inside one group (via the relay's
                # Swagger proxy, as in the manual demo).
                faults = httpx.post(
                    f"{base}/demo/uplink/faults",
                    json={
                        "drop_media": 1,
                        "drop_media_sources": ["127.0.0.11"],
                    },
                    timeout=5,
                )
                assert faults.status_code == 200, faults.text
                time.sleep(1.0)

                sim_stats = httpx.get(
                    f"http://127.0.0.1:{sim_port}/sim/stats", timeout=5
                ).json()
                assert sim_stats["dropped"].get("127.0.0.11", 0) == 1

                stats = _fec_stats(base)
                assert _delta(stats, baseline, "packets_recovered") == 1
                assert _delta(stats, baseline, "groups_recovered") == 1
                assert _delta(stats, baseline, "packets_lost") == 0
                # Every media packet reached the classroom exactly once.
                _assert_contiguous_and_unique(_seqs(receiver, "127.0.0.11"))
    finally:
        receiver.stop()


def test_fec_window_expires_when_two_packets_are_lost(tmp_path):
    sim_port, relay_port = free_ports(2)
    receiver = ClassroomReceiver(CLASSROOM_PORT)
    receiver.start()
    try:
        with run_service(
            "simulator.main:app", sim_port, _sim_env(), tmp_path / "sim.log"
        ) as sim_process:
            wait_http(
                f"http://127.0.0.1:{sim_port}/healthz",
                process=sim_process,
                log_path=tmp_path / "sim.log",
            )
            with run_service(
                "app.main:app", relay_port, _relay_env(sim_port),
                tmp_path / "relay.log",
            ) as relay_process:
                base = f"http://127.0.0.1:{relay_port}"
                wait_http(
                    f"{base}/healthz",
                    process=relay_process,
                    log_path=tmp_path / "relay.log",
                )
                time.sleep(0.6)
                baseline = _fec_stats(base)

                # Two losses inside one group: a single parity packet cannot
                # repair them; the group expires after the 100ms window.
                httpx.post(
                    f"{base}/demo/uplink/faults",
                    json={
                        "drop_media": 2,
                        "drop_media_sources": ["127.0.0.11"],
                    },
                    timeout=5,
                ).raise_for_status()
                time.sleep(1.0)

                sim_stats = httpx.get(
                    f"http://127.0.0.1:{sim_port}/sim/stats", timeout=5
                ).json()
                assert sim_stats["dropped"].get("127.0.0.11", 0) == 2

                stats = _fec_stats(base)
                assert _delta(stats, baseline, "packets_recovered") == 0
                assert _delta(stats, baseline, "groups_expired") >= 1
                assert _delta(stats, baseline, "packets_lost") >= 2

                seqs = _seqs(receiver, "127.0.0.11")
                assert seqs
                missing = sorted(set(range(min(seqs), max(seqs) + 1)) - set(seqs))
                assert len(missing) == 2
                # The two missing packets belong to the same group of four.
                assert missing[0] // 4 == missing[1] // 4
                # No duplicates even though repair was attempted.
                assert len(seqs) == len(set(seqs))
    finally:
        receiver.stop()


def test_fec_repair_after_resubscription(tmp_path):
    sim_port, relay_port = free_ports(2)
    receiver = ClassroomReceiver(CLASSROOM_PORT)
    receiver.start()
    try:
        with run_service(
            "simulator.main:app", sim_port, _sim_env(), tmp_path / "sim.log"
        ) as sim_process:
            wait_http(
                f"http://127.0.0.1:{sim_port}/healthz",
                process=sim_process,
                log_path=tmp_path / "sim.log",
            )
            with run_service(
                "app.main:app", relay_port, _relay_env(sim_port),
                tmp_path / "relay.log",
            ) as relay_process:
                base = f"http://127.0.0.1:{relay_port}"
                wait_http(
                    f"{base}/healthz",
                    process=relay_process,
                    log_path=tmp_path / "relay.log",
                )
                time.sleep(0.6)

                # Re-subscribe the classroom port to the other source.
                migration = httpx.post(
                    f"{base}/migrations",
                    json={"table": {str(CLASSROOM_PORT): "127.0.0.12"}},
                    timeout=10,
                )
                assert migration.status_code == 200, migration.text
                assert migration.json()["state"] == "completed"
                time.sleep(0.6)
                baseline = _fec_stats(base)
                before = len(_seqs(receiver, "127.0.0.12"))
                assert before > 0

                # Lose one packet of the *new* source: the recovered copy
                # must be emitted to the classroom by the current table.
                httpx.post(
                    f"{base}/demo/uplink/faults",
                    json={
                        "drop_media": 1,
                        "drop_media_sources": ["127.0.0.12"],
                    },
                    timeout=5,
                ).raise_for_status()
                time.sleep(1.0)

                stats = _fec_stats(base)
                assert _delta(stats, baseline, "packets_recovered") == 1
                _assert_contiguous_and_unique(_seqs(receiver, "127.0.0.12"))
    finally:
        receiver.stop()
