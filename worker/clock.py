"""A corrected clock: this machine's time plus its offset from NTP time servers.

The pods take their time from the Docker VM, which takes it from Windows. Windows can drift: on 2026-09-24 it
was 245 ms behind NTP time, so every latency was 245 ms too low. This module measures the offset in the
program, so our times are correct even when the machine's clock is not.
"""

import socket
import statistics
import struct
import threading
import time

SERVERS = ("time.cloudflare.com", "time.google.com", "pool.ntp.org")
NTP_EPOCH = 2208988800  # seconds from 1900 (NTP) to 1970 (Unix)
SAMPLES = 4  # queries to each server; the one with the shortest round trip has the smallest error


def sntp(server: str, timeout: float = 2.0) -> tuple[float, float]:
    """One SNTP query. Returns (offset, round trip) in seconds. offset = server time - our time."""
    request = b"\x23" + 47 * b"\0"  # version 4, client mode
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.settimeout(timeout)
        sent = time.time()
        s.sendto(request, (server, 123))
        reply, _ = s.recvfrom(48)
        received = time.time()
    server_in = _ntp_seconds(reply[32:40])
    server_out = _ntp_seconds(reply[40:48])
    offset = ((server_in - sent) + (server_out - received)) / 2
    round_trip = (received - sent) - (server_out - server_in)
    return offset, round_trip


def _ntp_seconds(field: bytes) -> float:
    seconds, fraction = struct.unpack("!II", field)
    return seconds - NTP_EPOCH + fraction / 2**32


def measure(servers: tuple[str, ...] = SERVERS) -> tuple[float, float]:
    """(offset, error) in seconds: the median over servers of each server's best sample.

    The error is half the round trip of the chosen samples: the true offset is within it.
    Raises OSError if no server answers.
    """
    best = []
    for server in servers:
        samples = []
        for _ in range(SAMPLES):
            try:
                samples.append(sntp(server))
            except OSError:
                continue
        if samples:
            best.append(min(samples, key=lambda s: s[1]))
    if not best:
        raise OSError("no NTP server answered")
    offset = statistics.median(o for o, _ in best)
    error = statistics.median(rtt for _, rtt in best) / 2
    return offset, error


class Clock:
    """time.time() plus the latest NTP offset. A background thread measures the offset again every minute."""

    def __init__(self, interval: float = 60.0):
        self.offset = 0.0  # seconds to add to time.time()
        self.error = None  # seconds; None until the first successful measurement
        self._interval = interval
        self._refresh()
        threading.Thread(target=self._loop, daemon=True).start()

    def now(self) -> float:
        return time.time() + self.offset

    def _refresh(self) -> None:
        try:
            self.offset, self.error = measure()
        except OSError as e:  # keep the last offset: a short NTP outage must not stop the worker
            print(f"clock: {e!r}; keeping offset {self.offset * 1000:+.1f} ms", flush=True)

    def _loop(self) -> None:
        while True:
            time.sleep(self._interval)
            self._refresh()
