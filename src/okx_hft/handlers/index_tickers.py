import time
from typing import Dict, Any, List
from okx_hft.storage.interfaces import IStorage
from okx_hft.utils.logging import get_logger

log = get_logger(__name__)


class IndexTickersHandler:
    """Handler for OKX index-tickers channel.
    
    Stores index price data in a separate table.
    Index tickers provide the underlying index price for derivatives.
    """
    
    def __init__(self, storage: IStorage = None) -> None:
        self.storage = storage
        self.batch: List[Dict[str, Any]] = []
        self.batch_max_size = 1000

    async def on_index_ticker(self, msg: Dict[str, Any]) -> None:
        """Process index ticker message from OKX WebSocket."""
        try:
            data = msg.get("data", [])
            if not data:
                return

            for ticker_data in data:
                if processed := self._process_index_ticker(ticker_data):
                    self.batch.append(processed)

            if len(self.batch) >= self.batch_max_size:
                await self._flush_batch()

        except Exception as e:
            log.error(f"Error processing index ticker: {str(e)}")

    def _process_index_ticker(
        self, ticker_data: Dict[str, Any]
    ) -> Dict[str, Any] | None:
        """Process single index ticker record.
        
        OKX index-tickers response format:
        {
            "instId": "BTC-USDT",
            "idxPx": "45000.12",
            "high24h": "46000.00",
            "low24h": "44000.00",
            "open24h": "44500.00",
            "sodUtc0": "44500.00",
            "sodUtc8": "44550.00",
            "ts": "1627382915000"
        }
        """
        try:
            return {
                "instId": ticker_data.get("instId", ""),
                "idxPx": float(ticker_data.get("idxPx", 0.0)),
                "open24h": float(ticker_data.get("open24h", 0.0)),
                "high24h": float(ticker_data.get("high24h", 0.0)),
                "low24h": float(ticker_data.get("low24h", 0.0)),
                "sodUtc0": float(ticker_data.get("sodUtc0", 0.0)),
                "sodUtc8": float(ticker_data.get("sodUtc8", 0.0)),
                "ts_event_ms": int(ticker_data.get("ts", 0)),
                "ts_ingest_ms": int(time.time() * 1000)
            }
        except (ValueError, TypeError) as e:
            log.error(f"Error processing index ticker data: {str(e)}")
            return None

    async def _flush_batch(self) -> None:
        """Send batch to storage."""
        if not self.batch:
            return
        
        batch_to_write = self.batch
        self.batch = []
        
        if self.storage:
            try:
                await self.storage.write_index_tickers(batch_to_write)
            except Exception as e:
                log.error(f"Error flushing index tickers: {str(e)}, batch_size={len(batch_to_write)}")

    async def flush(self) -> None:
        """Force flush remaining data."""
        if self.batch:
            await self._flush_batch()


