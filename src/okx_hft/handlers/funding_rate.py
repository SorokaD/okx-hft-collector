import time
from typing import Dict, Any, List
from okx_hft.storage.interfaces import IStorage
from okx_hft.utils.logging import get_logger

log = get_logger(__name__)


class FundingRateHandler:
    def __init__(self, storage: IStorage = None) -> None:
        self.storage = storage
        self.batch: List[Dict[str, Any]] = []
        self.batch_max_size = 1000

    async def on_funding_rate(self, msg: Dict[str, Any]) -> None:
        """Обработка данных funding rate"""
        try:
            data = msg.get("data", [])
            if not data:
                return

            for rate_data in data:
                if processed_rate := self._process_funding_rate(rate_data):
                    self.batch.append(processed_rate)

            # Отправляем батч если достигли максимального размера
            if len(self.batch) >= self.batch_max_size:
                await self._flush_batch()

        except Exception as e:
            log.error(f"Error processing funding rate: {str(e)}")

    def _process_funding_rate(
        self, rate_data: Dict[str, Any]
    ) -> Dict[str, Any] | None:
        """Обработка одного funding rate"""
        try:
            return {
                "instId": rate_data.get("instId", ""),
                "fundingRate": float(rate_data.get("fundingRate", 0.0)),
                "fundingTime": int(rate_data.get("fundingTime", 0)),
                "nextFundingTime": int(
                    rate_data.get("nextFundingTime", 0)
                ),
                "ts_event_ms": int(rate_data.get("ts", 0)),
                "ts_ingest_ms": int(time.time() * 1000)
            }
        except (ValueError, TypeError) as e:
            log.error(f"Error processing funding rate data: {str(e)}")
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
                await self.storage.write_funding_rates(batch_to_write)
            except Exception as e:
                log.error(f"Error flushing funding rates: {str(e)}, batch_size={len(batch_to_write)}")

    async def flush(self) -> None:
        """Принудительная отправка оставшихся данных"""
        if self.batch:
            await self._flush_batch()
