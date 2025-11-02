import time
from typing import Dict, Any, List, Optional
from okx_hft.handlers.interfaces import IOrderBookHandler
from okx_hft.storage.interfaces import IStorage
from okx_hft.utils.logging import get_logger

log = get_logger(__name__)


class OrderBookHandler(IOrderBookHandler):
    def __init__(self, storage: IStorage = None, batch_size: int = 50, flush_ms: int = 100) -> None:
        self.storage = storage
        self.batch_snapshots: List[Dict[str, Any]] = []
        self.batch_updates: List[Dict[str, Any]] = []
        self.batch_max_size = batch_size  # Уменьшил до 50 как у mark_prices

    async def on_snapshot(self, msg: Dict[str, Any]) -> None:
        """Process orderbook snapshot (action: snapshot)"""
        try:
            arg = msg.get("arg", {})
            inst_id = arg.get("instId", "")
            data = msg.get("data", [])

            log.info(
                f"on_snapshot called: inst_id={inst_id}, "
                f"data_len={len(data)}, msg_keys={list(msg.keys())}"
            )

            if not data:
                log.warning(f"No data in snapshot message for {inst_id}, msg: {msg}")
                return

            ts_ingest_ms = time.time_ns() // 1_000_000

            for snapshot_data in data:
                processed = self._process_snapshot(snapshot_data, inst_id, ts_ingest_ms)
                if processed:
                    self.batch_snapshots.append(processed)
                    log.info(
                        f"Added orderbook snapshot to batch: {processed['instId']}, "
                        f"bids_count={len(processed.get('bids', []))}, "
                        f"asks_count={len(processed.get('asks', []))}"
                    )
                else:
                    log.warning(f"Failed to process snapshot data: {snapshot_data}")

            # Flush if batch is full
            if len(self.batch_snapshots) >= self.batch_max_size:
                await self._flush_snapshots()

        except Exception as e:
            log.error(f"Error processing orderbook snapshot: {str(e)}", exc_info=True)

    async def on_increment(self, msg: Dict[str, Any]) -> None:
        """Process orderbook update (action: update)"""
        try:
            arg = msg.get("arg", {})
            inst_id = arg.get("instId", "")
            data = msg.get("data", [])

            log.info(
                f"on_increment called: inst_id={inst_id}, "
                f"data_len={len(data)}, msg_keys={list(msg.keys())}"
            )

            if not data:
                log.warning(f"No data in update message for {inst_id}, msg: {msg}")
                return

            ts_ingest_ms = time.time_ns() // 1_000_000

            for update_data in data:
                processed = self._process_update(update_data, inst_id, ts_ingest_ms)
                if processed:
                    self.batch_updates.append(processed)
                    log.info(
                        f"Added orderbook update to batch: {processed['instId']}, "
                        f"bids_delta_count={len(processed.get('bids_delta', []))}, "
                        f"asks_delta_count={len(processed.get('asks_delta', []))}"
                    )
                else:
                    log.warning(f"Failed to process update data: {update_data}")

            # Flush if batch is full
            if len(self.batch_updates) >= self.batch_max_size:
                await self._flush_updates()

        except Exception as e:
            log.error(f"Error processing orderbook update: {str(e)}", exc_info=True)

    def _process_snapshot(
        self, snapshot: Dict[str, Any], inst_id: str, ts_ingest_ms: int
    ) -> Optional[Dict[str, Any]]:
        """Process snapshot message into storage format"""
        try:
            # OKX sends bids/asks as arrays: [["price", "size", ...], ...]
            bids_raw = snapshot.get("bids", [])
            asks_raw = snapshot.get("asks", [])
            
            # Parse and validate (limit to 50 levels)
            bids = self._parse_levels(bids_raw, max_levels=50)
            asks = self._parse_levels(asks_raw, max_levels=50)
            
            ts_event_ms = int(snapshot.get("ts", 0))
            checksum = int(snapshot.get("checksum", 0))
            
            return {
                "instId": inst_id,
                "ts_event_ms": ts_event_ms,
                "ts_ingest_ms": ts_ingest_ms,
                "bids": bids,
                "asks": asks,
                "checksum": checksum,
            }
        except (ValueError, TypeError, KeyError) as e:
            log.error(f"Error processing snapshot: {str(e)}, data={snapshot}")
            return None

    def _process_update(
        self, update: Dict[str, Any], inst_id: str, ts_ingest_ms: int
    ) -> Optional[Dict[str, Any]]:
        """Process update message into storage format"""
        try:
            # OKX sends bids/asks as arrays: [["price", "size", ...], ...]
            # Size=0 means remove the level
            bids_delta_raw = update.get("bids", [])
            asks_delta_raw = update.get("asks", [])
            
            # Parse delta (all levels, not limited)
            bids_delta = self._parse_levels(bids_delta_raw, max_levels=None)
            asks_delta = self._parse_levels(asks_delta_raw, max_levels=None)
            
            ts_event_ms = int(update.get("ts", 0))
            checksum = int(update.get("checksum", 0))
            
            return {
                "instId": inst_id,
                "ts_event_ms": ts_event_ms,
                "ts_ingest_ms": ts_ingest_ms,
                "bids_delta": bids_delta,
                "asks_delta": asks_delta,
                "checksum": checksum,
            }
        except (ValueError, TypeError, KeyError) as e:
            log.error(f"Error processing update: {str(e)}, data={update}")
            return None

    def _parse_levels(
        self, levels: List[List[str]], max_levels: Optional[int] = None
    ) -> List[Dict[str, str]]:
        """Parse price levels from OKX format [["price", "size"], ...] to [{"price": "...", "size": "..."}, ...]"""
        result = []
        count = 0
        
        for level in levels:
            if not isinstance(level, list) or len(level) < 2:
                continue
            if max_levels and count >= max_levels:
                break
            
            price_str = str(level[0])
            size_str = str(level[1])
            
            # Validate numeric
            try:
                float(price_str)
                float(size_str)
            except (ValueError, TypeError):
                continue
            
            result.append({"price": price_str, "size": size_str})
            count += 1
        
        return result

    async def _flush_snapshots(self) -> None:
        """Flush snapshots batch to storage"""
        if self.batch_snapshots:
            if self.storage:
                try:
                    await self.storage.write_orderbook_snapshots(self.batch_snapshots)
                    log.info(
                        f"Successfully flushed {len(self.batch_snapshots)} "
                        f"orderbook snapshots to storage"
                    )
                    self.batch_snapshots = []
                except Exception as e:
                    log.error(
                        f"Error flushing orderbook snapshots batch: {str(e)}, "
                        f"batch_size={len(self.batch_snapshots)}"
                    )
                    log.error(
                        f"Batch sample: "
                        f"{self.batch_snapshots[:1] if self.batch_snapshots else 'empty'}"
                    )
            else:
                log.info(
                    f"No storage available, skipping flush of "
                    f"{len(self.batch_snapshots)} orderbook snapshots"
                )
                self.batch_snapshots = []

    async def _flush_updates(self) -> None:
        """Flush updates batch to storage"""
        if self.batch_updates:
            if self.storage:
                try:
                    await self.storage.write_orderbook_updates(self.batch_updates)
                    log.info(
                        f"Successfully flushed {len(self.batch_updates)} "
                        f"orderbook updates to storage"
                    )
                    self.batch_updates = []
                except Exception as e:
                    log.error(
                        f"Error flushing orderbook updates batch: {str(e)}, "
                        f"batch_size={len(self.batch_updates)}"
                    )
                    log.error(
                        f"Batch sample: "
                        f"{self.batch_updates[:1] if self.batch_updates else 'empty'}"
                    )
            else:
                log.info(
                    f"No storage available, skipping flush of "
                    f"{len(self.batch_updates)} orderbook updates"
                )
                self.batch_updates = []

    async def flush(self) -> None:
        """Force flush all remaining data (called periodically)"""
        await self._flush_snapshots()
        await self._flush_updates()
