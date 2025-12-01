import asyncio
import json
import time
from datetime import datetime, timezone

import websockets  # pip install websockets

WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
INST_ID = "BTC-USDT-SWAP"
CHANNEL = "trades"


async def count_trades(duration_sec: int = 60):
    start_wall = time.time()
    count = 0

    first_ts_ms = None
    last_ts_ms = None

    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10) as ws:
        sub_msg = {
            "op": "subscribe",
            "args": [
                {
                    "channel": CHANNEL,
                    "instId": INST_ID,
                }
            ],
        }
        await ws.send(json.dumps(sub_msg))
        print(f"Subscribed to {CHANNEL}:{INST_ID}, counting for {duration_sec} seconds...")

        while time.time() - start_wall < duration_sec:
            msg = await ws.recv()
            data = json.loads(msg)

            if (
                isinstance(data, dict)
                and data.get("arg", {}).get("channel") == CHANNEL
                and "data" in data
            ):
                trades = data["data"]
                count += len(trades)

                for t in trades:
                    ts_ms = int(t["ts"])

                    if first_ts_ms is None or ts_ms < first_ts_ms:
                        first_ts_ms = ts_ms
                    if last_ts_ms is None or ts_ms > last_ts_ms:
                        last_ts_ms = ts_ms

    print("=" * 60)
    print(f"Trades received: {count}")

    if first_ts_ms is not None and last_ts_ms is not None:
        first_dt = datetime.fromtimestamp(first_ts_ms / 1000, tz=timezone.utc)
        last_dt = datetime.fromtimestamp(last_ts_ms / 1000, tz=timezone.utc)

        print(f"First trade ts (ms): {first_ts_ms}")
        print(f"First trade UTC   : {first_dt.isoformat()}")
        print(f"Last  trade ts (ms): {last_ts_ms}")
        print(f"Last  trade UTC   : {last_dt.isoformat()}")

        print("\n-- SQL for Timescale (ms stored in BIGINT):")
        print(f"""
SELECT count(*)
FROM okx_raw.trades
WHERE instid = '{INST_ID}'
  AND ts_event_ms >= {first_ts_ms}
  AND ts_event_ms <= {last_ts_ms};
""")
    else:
        print("No trades received in this interval.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(count_trades(duration_sec=300))

