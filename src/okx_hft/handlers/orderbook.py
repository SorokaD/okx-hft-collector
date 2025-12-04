"""
OrderBookHandler with in-memory book management and periodic snapshots
"""
import time
import asyncio
import uuid
from typing import Dict, Any, List, Optional, Callable
from okx_hft.handlers.interfaces import IOrderBookHandler
from okx_hft.handlers.orderbook_l2 import OrderBookL2
from okx_hft.storage.interfaces import IStorage
from okx_hft.utils.logging import get_logger

log = get_logger(__name__)


class OrderBookHandler(IOrderBookHandler):
    """
    Handles orderbook snapshots and updates.
    Maintains in-memory books and generates periodic snapshots.
    """
    
    def __init__(
        self, 
        storage: IStorage = None, 
        batch_size: int = 50, 
        flush_ms: int = 100,
        snapshot_interval_sec: float = 30.0,
        max_depth: int = 50
    ) -> None:
        self.storage = storage
        self.batch_snapshots: List[Dict[str, Any]] = []
        self.batch_updates: List[Dict[str, Any]] = []
        self.batch_max_size = batch_size
        
        # In-memory order books per instrument
        self.books: Dict[str, OrderBookL2] = {}
        self.snapshot_interval_sec = snapshot_interval_sec
        self.max_depth = max_depth
        self._snapshot_task: Optional[asyncio.Task] = None
        self._resubscribe_callback: Optional[Callable[[str], None]] = None

    async def on_snapshot(self, msg: Dict[str, Any]) -> None:
        """Process orderbook snapshot (action: snapshot) - apply to in-memory book"""
        try:
            arg = msg.get("arg", {})
            inst_id = arg.get("instId", "")
            data = msg.get("data", [])

            if not data:
                log.warning(f"No data in snapshot message for {inst_id}")
                return

            # Get or create book for this instrument
            if inst_id not in self.books:
                self.books[inst_id] = OrderBookL2(inst_id, max_depth=self.max_depth)
                log.info(
                    f"Created OrderBookL2 for {inst_id} "
                    f"(total books: {len(self.books)})"
                )

            book = self.books[inst_id]
            ts_ingest_ms = time.time_ns() // 1_000_000

            for snapshot_data in data:
                # Apply snapshot to in-memory book
                success = book.apply_snapshot(snapshot_data)
                if success:
                    # Generate snapshot rows immediately after receiving OKX snapshot
                    snapshot_id = str(uuid.uuid4())  # UUID for ClickHouse
                    ts_event_ms = int(snapshot_data.get("ts", ts_ingest_ms))
                    rows = book.to_snapshot_rows(
                        snapshot_id=snapshot_id,
                        ts_event_ms=ts_event_ms,
                        max_levels=self.max_depth
                    )
                    if rows:
                        self.batch_snapshots.extend(rows)
                else:
                    log.warning(
                        f"Failed to apply snapshot to OrderBookL2: {inst_id}"
                    )

            # Flush if batch is full
            if len(self.batch_snapshots) >= self.batch_max_size:
                await self._flush_snapshots()

        except Exception as e:
            log.error(
                f"Error processing orderbook snapshot: {str(e)}", exc_info=True
            )

    async def on_increment(self, msg: Dict[str, Any]) -> None:
        """Process orderbook update (action: update) - apply to in-memory book"""
        try:
            arg = msg.get("arg", {})
            inst_id = arg.get("instId", "")
            data = msg.get("data", [])

            if not data:
                log.warning(f"No data in update message for {inst_id}")
                return

            # Get book for this instrument (create if doesn't exist)
            if inst_id not in self.books:
                log.warning(
                    f"Received update for {inst_id} but no book exists. "
                    f"Creating book - will wait for snapshot."
                )
                self.books[inst_id] = OrderBookL2(inst_id, max_depth=self.max_depth)
            
            book = self.books[inst_id]
            ts_ingest_ms = time.time_ns() // 1_000_000

            for update_data in data:
                # Apply update to in-memory book (if valid)
                if book.is_valid():
                    success, checksum_ok = book.apply_updates(update_data)
                    
                    if not success:
                        log.error(
                            f"Failed to apply update to OrderBookL2[{inst_id}]"
                        )
                    
                    if not checksum_ok:
                        log.warning(
                            f"Checksum/sequence mismatch for {inst_id}. "
                            f"Generating snapshot before resubscribe..."
                        )
                        # Generate snapshot before resubscribe
                        await self._generate_snapshot_for_instrument(inst_id)
                        # Reset book and trigger resubscribe
                        book.reset()
                        if self._resubscribe_callback:
                            await self._resubscribe_callback(inst_id)
                
                # Still save the update
                processed = self._process_update(update_data, inst_id, ts_ingest_ms)
                if processed:
                    self.batch_updates.append(processed)

            # Flush if batch is full
            if len(self.batch_updates) >= self.batch_max_size:
                await self._flush_updates()

        except Exception as e:
            log.error(
                f"Error processing orderbook update: {str(e)}", exc_info=True
            )

    def _process_update(
        self, update: Dict[str, Any], inst_id: str, ts_ingest_ms: int
    ) -> Optional[Dict[str, Any]]:
        """Process update message into storage format"""
        try:
            bids_delta_raw = update.get("bids", [])
            asks_delta_raw = update.get("asks", [])
            
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
        """Parse price levels from OKX format"""
        result = []
        count = 0
        
        for level in levels:
            if not isinstance(level, list) or len(level) < 2:
                continue
            if max_levels and count >= max_levels:
                break
            
            price_str = str(level[0])
            size_str = str(level[1])
            
            try:
                float(price_str)
                float(size_str)
            except (ValueError, TypeError):
                continue
            
            result.append({"price": price_str, "size": size_str})
            count += 1
        
        return result

    async def _generate_snapshot_for_instrument(self, inst_id: str) -> None:
        """Generate snapshot for specific instrument and add to batch"""
        if inst_id not in self.books:
            return
        
        book = self.books[inst_id]
        if not book.is_valid():
            return
        
        snapshot_id = str(uuid.uuid4())  # UUID for ClickHouse
        ts_ingest_ms = time.time_ns() // 1_000_000
        ts_event_ms = book.last_ts_event_ms or ts_ingest_ms
        
        rows = book.to_snapshot_rows(
            snapshot_id=snapshot_id,
            ts_event_ms=ts_event_ms,
            max_levels=self.max_depth
        )
        
        if rows:
            self.batch_snapshots.extend(rows)
            
            # Flush if batch is full
            if len(self.batch_snapshots) >= self.batch_max_size:
                await self._flush_snapshots()

    async def _flush_snapshots(self) -> None:
        """Flush snapshots batch to storage"""
        if not self.batch_snapshots:
            return
        
        # Атомарно забираем батч — новые данные пойдут в новый список
        batch_to_write = self.batch_snapshots
        self.batch_snapshots = []
        
        if self.storage:
            try:
                await self.storage.write_orderbook_snapshots(batch_to_write)
            except Exception as e:
                log.error(
                    f"Error flushing orderbook snapshots: {str(e)}, "
                    f"batch_size={len(batch_to_write)}"
                )

    async def _flush_updates(self) -> None:
        """Flush updates batch to storage"""
        if not self.batch_updates:
            return
        
        # Атомарно забираем батч — новые данные пойдут в новый список
        batch_to_write = self.batch_updates
        self.batch_updates = []
        
        if self.storage:
            try:
                await self.storage.write_orderbook_updates(batch_to_write)
            except Exception as e:
                log.error(
                    f"Error flushing orderbook updates: {str(e)}, "
                    f"batch_size={len(batch_to_write)}"
                )

    async def flush(self) -> None:
        """Force flush all remaining data"""
        await self._flush_snapshots()
        await self._flush_updates()
    
    def set_resubscribe_callback(
        self, callback: Callable[[str], None]
    ) -> None:
        """Set callback for resubscribe on checksum mismatch"""
        self._resubscribe_callback = callback
    
    async def periodic_snapshots(self) -> None:
        """Periodically generate snapshots from in-memory books"""
        log.info(
            f"Starting periodic snapshot generation task "
            f"(interval={self.snapshot_interval_sec}s, max_depth={self.max_depth})"
        )
        iteration = 0
        while True:
            try:
                await asyncio.sleep(self.snapshot_interval_sec)
                iteration += 1
                
                if not self.books:
                    log.debug("No books in memory, skipping snapshot generation")
                    continue
                
                ts_ingest_ms = time.time_ns() // 1_000_000
                generated_count = 0
                total_rows = 0
                
                for inst_id, book in self.books.items():
                    if not book.is_valid():
                        log.debug(
                            f"Book {inst_id} is not valid, skipping snapshot"
                        )
                        continue
                    
                    snapshot_id = str(uuid.uuid4())  # UUID for ClickHouse
                    ts_event_ms = book.last_ts_event_ms or ts_ingest_ms
                    
                    rows = book.to_snapshot_rows(
                        snapshot_id=snapshot_id,
                        ts_event_ms=ts_event_ms,
                        max_levels=self.max_depth
                    )
                    
                    if rows:
                        self.batch_snapshots.extend(rows)
                        generated_count += 1
                        total_rows += len(rows)
                
                # Flush if batch is full or we generated new snapshots
                if len(self.batch_snapshots) >= self.batch_max_size:
                    await self._flush_snapshots()
                elif len(self.batch_snapshots) > 0 and generated_count > 0:
                    await self._flush_snapshots()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(
                    f"Error in periodic snapshots: {str(e)}", exc_info=True
                )
    
    def start_periodic_snapshots(self) -> None:
        """Start periodic snapshot generation task"""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                if self._snapshot_task is None or self._snapshot_task.done():
                    self._snapshot_task = asyncio.create_task(
                        self.periodic_snapshots()
                    )
                    log.info(
                        f"✓ Started periodic snapshot generation task "
                        f"(interval={self.snapshot_interval_sec}s, "
                        f"max_depth={self.max_depth})"
                    )
                else:
                    log.warning(
                        "Periodic snapshot task already running"
                    )
            else:
                log.error(
                    "Cannot start periodic snapshots: event loop is not running"
                )
        except RuntimeError as e:
            log.error(
                f"Failed to start periodic snapshot task (no event loop): {e}",
                exc_info=True
            )
        except Exception as e:
            log.error(
                f"Failed to start periodic snapshot task: {e}",
                exc_info=True
            )
    
    def stop_periodic_snapshots(self) -> None:
        """Stop periodic snapshot generation task"""
        if self._snapshot_task and not self._snapshot_task.done():
            self._snapshot_task.cancel()
            log.info("Stopped periodic snapshot generation")
    
    async def on_reconnect(self) -> None:
        """Called on WebSocket reconnect - generate snapshots for all valid books"""
        log.info(
            f"on_reconnect: generating snapshots for {len(self.books)} books"
        )
        for inst_id in list(self.books.keys()):
            await self._generate_snapshot_for_instrument(inst_id)
        # Flush after generating all snapshots
        await self._flush_snapshots()
