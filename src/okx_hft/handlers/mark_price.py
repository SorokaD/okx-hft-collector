import time
from typing import Dict, Any, List
from okx_hft.storage.interfaces import IStorage
from okx_hft.utils.logging import get_logger

log = get_logger(__name__)


class MarkPriceHandler:
    def __init__(self, storage: IStorage = None) -> None:
        self.storage = storage
        self.batch: List[Dict[str, Any]] = []
        self.batch_max_size = 200  # Увеличен для производительности

    async def on_mark_price(self, msg: Dict[str, Any]) -> None:
        """Обработка данных mark price"""
        try:
            data = msg.get("data", [])
            if not data:
                return

            for price_data in data:
                if processed_price := self._process_mark_price(price_data):
                    self.batch.append(processed_price)

            # Отправляем батч если достигли максимального размера
            if len(self.batch) >= self.batch_max_size:
                await self._flush_batch()

        except Exception as e:
            log.error(f"Error processing mark price: {str(e)}")

    def _process_mark_price(
        self, price_data: Dict[str, Any]
    ) -> Dict[str, Any] | None:
        """Обработка одного mark price"""
        try:
            return {
                "instId": price_data.get("instId", ""),
                "markPx": float(price_data.get("markPx", 0.0)),
                "idxPx": float(price_data.get("idxPx", 0.0)),
                "idxTs": int(price_data.get("idxTs", 0)),
                "ts_event_ms": int(price_data.get("ts", 0)),
                "ts_ingest_ms": int(time.time() * 1000)
            }
        except (ValueError, TypeError) as e:
            log.error(f"Error processing mark price data: {str(e)}")
            return None

    async def _flush_batch(self) -> None:
        """Отправка батча в хранилище"""
        if self.batch:
            if self.storage:
                try:
                    await self.storage.write_mark_prices(self.batch)
                    self.batch = []
                except Exception as e:
                    log.error(f"Error flushing mark prices: {str(e)}, batch_size={len(self.batch)}")
            else:
                self.batch = []

    async def flush(self) -> None:
        """Принудительная отправка оставшихся данных"""
        if self.batch:
            await self._flush_batch()
