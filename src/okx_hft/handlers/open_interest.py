import time
from typing import Dict, Any, List
from okx_hft.storage.interfaces import IStorage
from okx_hft.utils.logging import get_logger

log = get_logger(__name__)


class OpenInterestHandler:
    def __init__(self, storage: IStorage = None) -> None:
        self.storage = storage
        self.batch: List[Dict[str, Any]] = []
        self.batch_max_size = 50

    async def on_open_interest(self, msg: Dict[str, Any]) -> None:
        """Обработка данных open interest"""
        try:
            data = msg.get("data", [])
            if not data:
                return

            for oi_data in data:
                if processed_oi := self._process_open_interest(oi_data):
                    self.batch.append(processed_oi)
                    log.info(f"Added open interest to batch: {processed_oi}")

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
        if self.batch:
            if self.storage:
                try:
                    await self.storage.write_open_interest(self.batch)
                    log.info(
                        f"Successfully flushed {len(self.batch)} "
                        f"open interest records to storage"
                    )
                    self.batch = []
                except Exception as e:
                    # Проверяем, не является ли это "успешным" результатом
                    if "ClickHouse error writing open_interest: 0" in str(e):
                        log.info(
                            f"Successfully flushed {len(self.batch)} "
                            f"open interest records to storage "
                            f"(ClickHouse returned 0)"
                        )
                        self.batch = []
                    else:
                        log.error(
                            f"Error flushing open interest batch: {str(e)}, "
                            f"batch_size={len(self.batch)}"
                        )
                        log.error(
                            f"Batch sample: "
                            f"{self.batch[:2] if self.batch else 'empty'}"
                        )
            else:
                log.info(
                    f"No storage available, skipping flush of "
                    f"{len(self.batch)} open interest records"
                )
                self.batch = []

    async def flush(self) -> None:
        """Принудительная отправка оставшихся данных"""
        if self.batch:
            await self._flush_batch()

