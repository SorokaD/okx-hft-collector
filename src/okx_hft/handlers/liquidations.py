import time
from typing import Dict, Any, List
from okx_hft.storage.interfaces import IStorage
from okx_hft.utils.logging import get_logger

log = get_logger(__name__)


class LiquidationsHandler:
    def __init__(self, storage: IStorage = None) -> None:
        self.storage = storage
        self.batch: List[Dict[str, Any]] = []
        self.batch_max_size = 50

    async def on_liquidation(self, msg: Dict[str, Any]) -> None:
        """Обработка данных ликвидаций"""
        try:
            data = msg.get("data", [])
            if not data:
                return

            for liq_data in data:
                if processed_liq := self._process_liquidation(liq_data):
                    self.batch.append(processed_liq)
                    log.info(f"Added liquidation to batch: {processed_liq}")

            # Отправляем батч если достигли максимального размера
            if len(self.batch) >= self.batch_max_size:
                await self._flush_batch()

        except Exception as e:
            log.error(f"Error processing liquidation: {str(e)}")

    def _process_liquidation(
        self, liq_data: Dict[str, Any]
    ) -> Dict[str, Any] | None:
        """Обработка одной ликвидации"""
        try:
            return {
                "instId": liq_data.get("instId", ""),
                "posSide": liq_data.get("posSide", ""),
                "side": liq_data.get("side", ""),
                "sz": float(liq_data.get("sz", 0.0)),
                "bkPx": float(liq_data.get("bkPx", 0.0)),
                "bkLoss": float(liq_data.get("bkLoss", 0.0)),
                "ccy": liq_data.get("ccy", ""),
                "ts_event_ms": int(liq_data.get("ts", 0)),
                "ts_ingest_ms": int(time.time() * 1000)
            }
        except (ValueError, TypeError) as e:
            log.error(f"Error processing liquidation data: {str(e)}")
            return None

    async def _flush_batch(self) -> None:
        """Отправка батча в хранилище"""
        if self.batch:
            if self.storage:
                try:
                    await self.storage.write_liquidations(self.batch)
                    log.info(
                        f"Successfully flushed {len(self.batch)} "
                        f"liquidations to storage"
                    )
                    self.batch = []
                except Exception as e:
                    # Проверяем, не является ли это "успешным" результатом
                    if "ClickHouse error writing liquidations: 0" in str(e):
                        log.info(
                            f"Successfully flushed {len(self.batch)} "
                            f"liquidations to storage (ClickHouse returned 0)"
                        )
                        self.batch = []
                    else:
                        log.error(
                            f"Error flushing liquidation batch: {str(e)}, "
                            f"batch_size={len(self.batch)}"
                        )
                        log.error(
                            f"Batch sample: "
                            f"{self.batch[:2] if self.batch else 'empty'}"
                        )
            else:
                log.info(
                    f"No storage available, skipping flush of "
                    f"{len(self.batch)} liquidations"
                )
                self.batch = []

    async def flush(self) -> None:
        """Принудительная отправка оставшихся данных"""
        if self.batch:
            await self._flush_batch()

