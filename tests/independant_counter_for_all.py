import asyncio
import json
import time
from datetime import datetime, timezone

import websockets  # pip install websockets

WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
INST_ID = "BTC-USDT-SWAP"
DURATION_SEC = 300

# Каналы, которые слушаем, и к каким таблицам их проверяем.
# Для books5 указываем основную таблицу + extra_tables для снапшотов.
CHANNEL_CONFIG = [
    {
        "channel": "trades",
        "instId": INST_ID,
        "db_table": "okx_raw.trades",
        "ts_column": "ts_event_ms",
        "instid_column": "instid",
    },
    {
        "channel": "books5",
        "instId": INST_ID,
        "db_table": "okx_raw.orderbook_updates",          # апдейты
        "extra_tables": ["okx_raw.orderbook_snapshots"],  # + снапшоты
        "ts_column": "ts_event_ms",
        "instid_column": "instid",
    },
    {
        "channel": "funding-rate",
        "instId": INST_ID,
        "db_table": "okx_raw.funding_rates",
        "ts_column": "ts_event_ms",
        "instid_column": "instid",
    },
    {
        "channel": "mark-price",
        "instId": INST_ID,
        "db_table": "okx_raw.mark_prices",
        "ts_column": "ts_event_ms",
        "instid_column": "instid",
    },
    {
        "channel": "open-interest",
        "instId": INST_ID,
        "db_table": "okx_raw.open_interest",
        "ts_column": "ts_event_ms",
        "instid_column": "instid",
    },
    {
        "channel": "tickers",
        "instId": INST_ID,
        "db_table": "okx_raw.tickers",
        "ts_column": "ts_event_ms",
        "instid_column": "instid",
    },
]


async def count_all_channels(duration_sec: int = DURATION_SEC) -> None:
    # Статистика по каждому каналу (одна запись на channel)
    stats = {
        cfg["channel"]: {
            "count": 0,
            "first_ts_ms": None,
            "last_ts_ms": None,
        }
        for cfg in CHANNEL_CONFIG
    }

    start_wall = time.time()

    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10) as ws:
        # Подписка сразу на все каналы
        sub_msg = {
            "op": "subscribe",
            "args": [
                {"channel": cfg["channel"], "instId": cfg["instId"]}
                for cfg in CHANNEL_CONFIG
            ],
        }
        await ws.send(json.dumps(sub_msg))
        print(f"Subscribed for {duration_sec} seconds to channels:")
        for cfg in CHANNEL_CONFIG:
            print(f"  - {cfg['channel']} : {cfg['instId']}")
        print()

        # Счётчик до тех пор, пока не истечёт duration_sec
        while time.time() - start_wall < duration_sec:
            msg = await ws.recv()
            data = json.loads(msg)

            if not isinstance(data, dict):
                continue

            arg = data.get("arg") or {}
            channel_name = arg.get("channel")
            if channel_name not in stats:
                continue
            if "data" not in data:
                continue

            records = data["data"]
            chan_stats = stats[channel_name]
            chan_stats["count"] += len(records)

            for rec in records:
                ts_ms = int(rec["ts"])
                if chan_stats["first_ts_ms"] is None or ts_ms < chan_stats["first_ts_ms"]:
                    chan_stats["first_ts_ms"] = ts_ms
                if chan_stats["last_ts_ms"] is None or ts_ms > chan_stats["last_ts_ms"]:
                    chan_stats["last_ts_ms"] = ts_ms

    # ----------------------------------------
    # Вывод результата и SQL по всем таблицам
    # ----------------------------------------
    print("=" * 80)
    print(f"Wall-clock duration: {duration_sec} seconds\n")

    for cfg in CHANNEL_CONFIG:
        ch = cfg["channel"]
        st = stats[ch]

        print(f"=== Channel: {ch}  |  InstId: {cfg['instId']} ===")
        print(f"Records received: {st['count']}")

        first_ts_ms = st["first_ts_ms"]
        last_ts_ms = st["last_ts_ms"]

        if first_ts_ms is None or last_ts_ms is None:
            print("No data received for this channel in the interval.\n")
            continue

        first_dt = datetime.fromtimestamp(first_ts_ms / 1000, tz=timezone.utc)
        last_dt = datetime.fromtimestamp(last_ts_ms / 1000, tz=timezone.utc)

        print(f"First ts (ms) : {first_ts_ms}")
        print(f"First UTC     : {first_dt.isoformat()}")
        print(f"Last  ts (ms) : {last_ts_ms}")
        print(f"Last  UTC     : {last_dt.isoformat()}")

        ts_col = cfg["ts_column"]
        inst_col = cfg["instid_column"]
        inst_id = cfg["instId"]

        # Основная таблица
        print("\n-- SQL for main table:")
        print(
            f"""
SELECT count(*)
FROM {cfg['db_table']}
WHERE {inst_col} = '{inst_id}'
  AND {ts_col} >= {first_ts_ms}
  AND {ts_col} <= {last_ts_ms};
""".strip()
        )

        # Дополнительные таблицы (например, orderbook_snapshots)
        extra_tables = cfg.get("extra_tables", [])
        for tbl in extra_tables:
            print("\n-- SQL for extra table:")
            print(
                f"""
SELECT count(*)
FROM {tbl}
WHERE {inst_col} = '{inst_id}'
  AND {ts_col} >= {first_ts_ms}
  AND {ts_col} <= {last_ts_ms};
""".strip()
            )

        print("\n" + "-" * 80 + "\n")


if __name__ == "__main__":
    asyncio.run(count_all_channels())
