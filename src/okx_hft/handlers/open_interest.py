import time
from typing import Dict, Any, List
from okx_hft.storage.interfaces import IStorage
from okx_hft.utils.logging import get_logger

log = get_logger(__name__)


class OpenInterestHandler:
    def __init__(self, storage: IStorage = None) -> None:
        self.storage = storage
        self.batch: List[Dict[str, Any]] = []
        self.batch_max_size = 1000

    async def on_open_interest(self, msg: Dict[str, Any]) -> None:
        """Обработка данных open interest"""
        try:
            data = msg.get("data", [])
            if not data:
                return

            for oi_data in data:
                if processed_oi := self._process_open_interest(oi_data):
                    self.batch.append(processed_oi)

            # Отправляем батч если достигли максимального размера
            if len(self.batch) >= self.batch_max_size:
                await self._flush_batch()

        except Exception as e:
            log.error(f"Error processing open interest: {str(e)}")

    def _process_open_interest(
        self, oi_data: Dict[str, Any]
    ) -> Dict[str, Any] | None:
        """Обработка одного open interest"""
        try:
            return {
                "instId": oi_data.get("instId", ""),
                "oi": float(oi_data.get("oi", 0.0)),
                "oiCcy": float(oi_data.get("oiCcy", 0.0)),
                "ts_event_ms": int(oi_data.get("ts", 0)),
                "ts_ingest_ms": int(time.time() * 1000)
            }
        except (ValueError, TypeError) as e:
            log.error(f"Error processing open interest data: {str(e)}")
            return None

    async def _flush_batch(self) -> None:
        """Отправка батча в хранилище"""
        if not self.batch:
            return
        
        # Атомарно забираем батч — новые данные пойдут в новый список
        batch_to_write = self.batch
        self.batch = []
        
        if self.storage:
            try:
                await self.storage.write_open_interest(batch_to_write)
            except Exception as e:
                log.error(f"Error flushing open interest: {str(e)}, batch_size={len(batch_to_write)}")

    async def flush(self) -> None:
        """Принудительная отправка оставшихся данных"""
        if self.batch:
            await self._flush_batch()

