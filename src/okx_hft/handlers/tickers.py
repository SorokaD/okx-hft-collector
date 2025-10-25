import time
from typing import Dict, Any, List
from okx_hft.storage.interfaces import IStorage
from okx_hft.utils.logging import get_logger

log = get_logger(__name__)


class TickersHandler:
    def __init__(self, storage: IStorage = None) -> None:
        self.storage = storage
        self.batch: List[Dict[str, Any]] = []
        self.batch_max_size = 50

    async def on_ticker(self, msg: Dict[str, Any]) -> None:
        """Обработка данных ticker"""
        try:
            data = msg.get("data", [])
            if not data:
                return

            for ticker_data in data:
                if processed_ticker := self._process_ticker(ticker_data):
                    self.batch.append(processed_ticker)
                    log.info(f"Added ticker to batch: {processed_ticker}")

            # Отправляем батч если достигли максимального размера
            if len(self.batch) >= self.batch_max_size:
                await self._flush_batch()

        except Exception as e:
            log.error(f"Error processing ticker: {str(e)}")

    def _process_ticker(
        self, ticker_data: Dict[str, Any]
    ) -> Dict[str, Any] | None:
        """Обработка одного ticker"""
        try:
            return {
                "instId": ticker_data.get("instId", ""),
                "last": float(ticker_data.get("last", 0.0)),
                "lastSz": float(ticker_data.get("lastSz", 0.0)),
                "bidPx": float(ticker_data.get("bidPx", 0.0)),
                "bidSz": float(ticker_data.get("bidSz", 0.0)),
                "askPx": float(ticker_data.get("askPx", 0.0)),
                "askSz": float(ticker_data.get("askSz", 0.0)),
                "open24h": float(ticker_data.get("open24h", 0.0)),
                "high24h": float(ticker_data.get("high24h", 0.0)),
                "low24h": float(ticker_data.get("low24h", 0.0)),
                "vol24h": float(ticker_data.get("vol24h", 0.0)),
                "volCcy24h": float(ticker_data.get("volCcy24h", 0.0)),
                "ts_event_ms": int(ticker_data.get("ts", 0)),
                "ts_ingest_ms": int(time.time() * 1000)
            }
        except (ValueError, TypeError) as e:
            log.error(f"Error processing ticker data: {str(e)}")
            return None

    async def _flush_batch(self) -> None:
        """Отправка батча в хранилище"""
        if self.batch:
            if self.storage:
                try:
                    await self.storage.write_tickers(self.batch)
                    log.info(
                        f"Successfully flushed {len(self.batch)} "
                        f"tickers to storage"
                    )
                    self.batch = []
                except Exception as e:
                    # Проверяем, не является ли это "успешным" результатом
                    if "ClickHouse error writing tickers: 0" in str(e):
                        log.info(
                            f"Successfully flushed {len(self.batch)} "
                            f"tickers to storage (ClickHouse returned 0)"
                        )
                        self.batch = []
                    else:
                        log.error(
                            f"Error flushing ticker batch: {str(e)}, "
                            f"batch_size={len(self.batch)}"
                        )
                        log.error(
                            f"Batch sample: "
                            f"{self.batch[:2] if self.batch else 'empty'}"
                        )
            else:
                log.info(
                    f"No storage available, skipping flush of "
                    f"{len(self.batch)} tickers"
                )
                self.batch = []

    async def flush(self) -> None:
        """Принудительная отправка оставшихся данных"""
        if self.batch:
            await self._flush_batch()

