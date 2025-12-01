
import asyncio
import aiohttp
import orjson
import random
from typing import Dict, Any
from okx_hft.config.settings import Settings
from okx_hft.utils.logging import get_logger
from okx_hft.metrics.server import reconnects_total, events_total
from okx_hft.storage.postgres import PostgreSQLStorage
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
        
        # PostgreSQL storage will be initialized in run_forever (async)
        self.storage = None
        
        # Инициализируем обработчики
        self.trades_handler = TradesHandler(self.storage)
        self.orderbook_handler = OrderBookHandler(
            self.storage,
            batch_size=self.s.BATCH_MAX_SIZE,
            flush_ms=self.s.FLUSH_INTERVAL_MS,
            snapshot_interval_sec=self.s.SNAPSHOT_INTERVAL_SEC,
            max_depth=self.s.ORDERBOOK_MAX_DEPTH
        )
        # Set resubscribe callback for orderbook
        self.orderbook_handler.set_resubscribe_callback(self._resubscribe_orderbook)
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
        # Initialize PostgreSQL storage
        try:
            self.storage = PostgreSQLStorage(
                host=self.s.POSTGRES_HOST,
                port=self.s.POSTGRES_PORT,
                user=self.s.POSTGRES_USER,
                password=self.s.POSTGRES_PASSWORD,
                database=self.s.POSTGRES_DB,
                schema=self.s.POSTGRES_SCHEMA,
            )
            await self.storage.connect()
            log.info("PostgreSQL storage initialized successfully")
            
            # Update handlers with storage
            self.trades_handler.storage = self.storage
            self.orderbook_handler.storage = self.storage
            self.funding_rate_handler.storage = self.storage
            self.mark_price_handler.storage = self.storage
            self.tickers_handler.storage = self.storage
            self.open_interest_handler.storage = self.storage
        except Exception as e:
            log.warning(
                f"Failed to initialize PostgreSQL storage: {e}. "
                f"Working without storage."
            )
            self.storage = None
        
        # Start periodic snapshot generation once event loop is running
        try:
            self.orderbook_handler.start_periodic_snapshots()
        except Exception as e:
            log.error(
                f"Failed to start periodic snapshots in run_forever: {e}",
                exc_info=True
            )
        
        while True:
            try:
                await self._run_once()
                self._attempt = 0
            except Exception as e:  # transport/protocol fallback; classify further in future
                reconnects_total.inc()
                # Generate snapshots before reconnect
                try:
                    await self.orderbook_handler.on_reconnect()
                except Exception as reconnect_err:
                    log.error(
                        f"Error generating snapshots on reconnect: {reconnect_err}"
                    )
                
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
                sub_payload = self._sub_payload()
                await ws.send_json(sub_payload)
                log.info(f"Sent subscription: {sub_payload}")
                
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = orjson.loads(msg.data)
                        
                        # Логируем ответы от сервера на подписку и ошибки
                        if data.get("event") or data.get("code") or "arg" not in data:
                            log.info(f"Server response: {data}")
                        
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
            elif channel in ("books", "books-l2-tbt", "books50-l2-tbt", "books5"):
                # Логируем входящие данные orderbook для отладки
                action = data.get("action", "snapshot")
                log.info(
                    f"Received orderbook message: channel={channel}, "
                    f"inst={inst}, action={action}, "
                    f"data_count={len(data.get('data', []))}"
                )
                # Проверяем тип сообщения: snapshot или update
                if action == "snapshot":
                    await self.orderbook_handler.on_snapshot(data)
                elif action == "update":
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

    async def _resubscribe_orderbook(self, inst_id: str) -> None:
        """Resubscribe to orderbook for specific instrument (called on checksum mismatch)"""
        log.warning(
            f"Resubscribe requested for {inst_id} (checksum mismatch)"
        )
        # TODO: Implement actual resubscribe logic
        # For now, just log - full implementation would require WS connection access
        # The book will be reset and wait for new snapshot

    async def flush_all_handlers(self) -> None:
        """Сбросить батчи всех обработчиков в хранилище.
        
        Метод идемпотентен - повторные вызовы безопасны.
        Каждый handler.flush() ничего не делает, если батч пуст.
        """
        handlers = [
            ("trades", self.trades_handler),
            ("orderbook", self.orderbook_handler),
            ("funding_rate", self.funding_rate_handler),
            ("mark_price", self.mark_price_handler),
            ("tickers", self.tickers_handler),
            ("open_interest", self.open_interest_handler),
        ]
        
        for name, handler in handlers:
            try:
                await handler.flush()
            except Exception as e:
                log.error(f"Ошибка при сбросе {name} при остановке: {e}")

    async def periodic_flush(self) -> None:
        """Периодическая отправка батчей каждые 5 секунд"""
        while True:
            try:
                await asyncio.sleep(5.0)
                await self.flush_all_handlers()
            except asyncio.CancelledError:
                log.info("Задача periodic_flush отменена, выполняем финальный сброс...")
                try:
                    await self.flush_all_handlers()
                    log.info("Финальный сброс выполнен успешно")
                except Exception as e:
                    log.error(f"Ошибка при финальном сбросе: {e}")
                break
            except Exception as e:
                log.error(f"Ошибка в periodic flush: {str(e)}")
