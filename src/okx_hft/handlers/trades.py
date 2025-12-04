import time
from typing import Dict, Any, List
from okx_hft.handlers.interfaces import ITradeHandler
from okx_hft.storage.interfaces import IStorage
from okx_hft.utils.logging import get_logger

log = get_logger(__name__)


class TradesHandler(ITradeHandler):
    def __init__(self, storage: IStorage = None) -> None:
        self.storage = storage
        self.batch: List[Dict[str, Any]] = []
        self.batch_max_size = 100  # Увеличен для производительности

    async def on_trade(self, msg: Dict[str, Any]) -> None:
        """Обработка торговых данных"""
        try:
            data = msg.get("data", [])
            if not data:
                return

            for trade in data:
                if processed_trade := self._process_trade(trade):
                    self.batch.append(processed_trade)

            # Отправляем батч если достигли максимального размера
            if len(self.batch) >= self.batch_max_size:
                await self._flush_batch()

        except Exception as e:
            log.error(f"Error processing trade: {str(e)}")

    def _process_trade(self, trade: Dict[str, Any]) -> Dict[str, Any] | None:
        """Обработка одного трейда"""
        try:
            return {
                "instId": trade.get("instId", ""),
                "ts_event_ms": int(trade.get("ts", 0)),
                "tradeId": trade.get("tradeId", ""),
                "px": float(trade.get("px", 0.0)),
                "sz": float(trade.get("sz", 0.0)),
                "side": trade.get("side", ""),
                "ts_ingest_ms": int(time.time() * 1000)
            }
        except (ValueError, TypeError) as e:
            log.error(f"Error processing trade data: {str(e)}")
            return None

    async def _flush_batch(self) -> None:
        """Отправка батча в хранилище"""
        if self.batch:
            if self.storage:
                try:
                    await self.storage.write_trades(self.batch)
                    self.batch = []
                except Exception as e:
                    log.error(f"Error flushing trades: {str(e)}, batch_size={len(self.batch)}")
            else:
                self.batch = []

    async def flush(self) -> None:
        """Принудительная отправка оставшихся данных"""
        if self.batch:
            await self._flush_batch()
