from prometheus_client import start_http_server, Counter, Gauge, Summary
from okx_hft.utils.logging import get_logger
import time, asyncio

log = get_logger(__name__)

events_total = Counter("events_total", "Total WS events", ["channel", "instId"])
reconnects_total = Counter("reconnects_total", "Total reconnects", [])
cs_fail_total = Counter("cs_fail_total", "Checksum failures", ["instId"])
gaps_total = Counter("gaps_total", "Sequence gaps", ["instId", "channel"])
staleness_ms_gauge = Gauge(
    "staleness_ms_gauge", "Event staleness in ms", ["channel", "instId"]
)
event_loop_lag_ms = Gauge("event_loop_lag_ms", "Event loop lag in ms", [])
ws_roundtrip_ms = Summary(
    "ws_roundtrip_ms", "WebSocket ping-pong round-trip time ms", []
)


async def monitor_loop_lag() -> None:
    while True:
        t0 = time.perf_counter()
        await asyncio.sleep(0)
        lag = (time.perf_counter() - t0) * 1000.0
        event_loop_lag_ms.set(lag)
        await asyncio.sleep(1.0)


async def run_metrics_server(port: int = 9108) -> None:
    start_http_server(port)
    log.info(f"metrics_started on port {port}")
    await monitor_loop_lag()
