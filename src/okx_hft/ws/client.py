
import asyncio
import aiohttp
import orjson
import random
from typing import Dict, Any
from okx_hft.config.settings import Settings
from okx_hft.utils.logging import get_logger
from okx_hft.metrics.server import reconnects_total, events_total
from okx_hft.storage.clickhouse import ClickHouseStorage
from okx_hft.handlers.trades import TradesHandler
from okx_hft.handlers.orderbook import OrderBookHandler
from okx_hft.handlers.funding_rate import FundingRateHandler
from okx_hft.handlers.mark_price import MarkPriceHandler
from okx_hft.handlers.tickers import TickersHandler
from okx_hft.handlers.open_interest import OpenInterestHandler

log = get_logger(__name__)


def full_jitter_delay(base: float, cap: float, attempt: int) -> float:
    exp = min(cap, base * (2 ** attempt))
    return random.uniform(0, exp)


class OKXWebSocketClient:
    def __init__(self, settings: Settings) -> None:
        self.s = settings
        self._attempt = 0
        
        # Пытаемся создать ClickHouse storage, если не получается - работаем без него
        try:
            self.storage = ClickHouseStorage(
                dsn=self.s.CLICKHOUSE_DSN,
                user=self.s.CLICKHOUSE_USER,
                password=self.s.CLICKHOUSE_PASSWORD,
                db=self.s.CLICKHOUSE_DB,
            )
            log.info("ClickHouse storage initialized successfully")
        except Exception as e:
            log.warning(
                f"Failed to initialize ClickHouse storage: {e}. "
                f"Working without storage."
            )
            self.storage = None
        
        # Инициализируем обработчики
        self.trades_handler = TradesHandler(self.storage)
        self.orderbook_handler = OrderBookHandler(self.storage)
        self.funding_rate_handler = FundingRateHandler(self.storage)
        self.mark_price_handler = MarkPriceHandler(self.storage)
        self.tickers_handler = TickersHandler(self.storage)
        self.open_interest_handler = OpenInterestHandler(self.storage)

    def _sub_payload(self) -> Dict[str, Any]:
        args = [
            {"channel": ch, "instId": inst}
            for ch in self.s.CHANNELS
            for inst in self.s.INSTRUMENTS
        ]
        return {"op": "subscribe", "args": args}

    async def run_forever(self) -> None:
        while True:
            try:
                await self._run_once()
                self._attempt = 0
            except Exception as e:  # transport/protocol fallback; classify further in future
                reconnects_total.inc()
                self._attempt += 1
                delay = full_jitter_delay(
                    self.s.BACKOFF_BASE, self.s.BACKOFF_CAP, self._attempt
                )
                log.error(
                    f"ws_error_reconnect: {str(e)}, attempt={self._attempt}, "
                    f"sleep_s={delay}"
                )
                await asyncio.sleep(delay)

    async def _run_once(self) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.s.OKX_WS_URL, heartbeat=20) as ws:
                await ws.send_json(self._sub_payload())
                log.info(f"subscribed: {self._sub_payload()}")
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = orjson.loads(msg.data)
                        await self._on_message(data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        raise RuntimeError("WS error")

    async def _on_message(self, data: Dict[str, Any]) -> None:
        arg = data.get("arg", {})
        channel = arg.get("channel", "unknown")
        inst = arg.get("instId", "unknown")
        
        if channel and inst:
            events_total.labels(channel=channel, instId=inst).inc()
            
            # Логируем входящие сообщения для отладки
            log.info(
                f"Received message: channel={channel}, inst={inst}, "
                f"data_count={len(data.get('data', []))}"
            )
            
            # Маршрутизация сообщений по каналам
            if channel == "trades":
                await self.trades_handler.on_trade(data)
            elif channel == "books-l2-tbt":
                # Проверяем тип сообщения: snapshot или increment
                if data.get("action") == "snapshot":
                    await self.orderbook_handler.on_snapshot(data)
                elif data.get("action") == "update":
                    await self.orderbook_handler.on_increment(data)
                else:
                    # Если action не указан, считаем snapshot
                    await self.orderbook_handler.on_snapshot(data)
            elif channel == "funding-rate":
                await self.funding_rate_handler.on_funding_rate(data)
            elif channel == "mark-price":
                await self.mark_price_handler.on_mark_price(data)
            elif channel == "tickers":
                await self.tickers_handler.on_ticker(data)
            elif channel == "open-interest":
                await self.open_interest_handler.on_open_interest(data)
            else:
                log.warning(f"Unknown channel: {channel}")

    async def periodic_flush(self) -> None:
        """Периодическая отправка батчей каждые 5 секунд"""
        while True:
            try:
                await asyncio.sleep(5.0)
                await self.trades_handler.flush()
                await self.orderbook_handler.flush()
                await self.funding_rate_handler.flush()
                await self.mark_price_handler.flush()
                await self.tickers_handler.flush()
                await self.open_interest_handler.flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Error in periodic flush: {str(e)}")
