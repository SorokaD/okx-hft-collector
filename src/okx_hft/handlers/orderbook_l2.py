"""
OrderBookL2 - in-memory order book state management
"""
import uuid
from typing import Dict, List, Optional, Tuple, Any
from collections import OrderedDict
from okx_hft.utils.logging import get_logger

log = get_logger(__name__)


class OrderBookL2:
    """
    In-memory L2 order book that applies snapshots and updates.
    Maintains full book state for periodic snapshot generation.
    """
    
    def __init__(self, inst_id: str, max_depth: int = 50):
        self.inst_id = inst_id
        self.max_depth = max_depth
        # Bids: price -> size (descending order, highest bid first)
        self.bids: OrderedDict[str, str] = OrderedDict()
        # Asks: price -> size (ascending order, lowest ask first)
        self.asks: OrderedDict[str, str] = OrderedDict()
        self.last_ts_event_ms: int = 0
        self.last_checksum: int = 0
        self.seq_id: Optional[int] = None
        self.prev_seq_id: Optional[int] = None
        self._is_valid = False
        
    def apply_snapshot(self, snapshot_data: Dict) -> bool:
        """Apply snapshot - replaces current book state"""
        try:
            bids_raw = snapshot_data.get("bids", [])
            asks_raw = snapshot_data.get("asks", [])
            
            # Clear current state
            self.bids.clear()
            self.asks.clear()
            
            # Apply bids (sorted descending by price)
            for level in bids_raw:
                if not isinstance(level, list) or len(level) < 2:
                    continue
                price_str = str(level[0])
                size_str = str(level[1])
                
                try:
                    float(price_str)  # Validate price is numeric
                    size_float = float(size_str)
                    
                    if size_float > 0:
                        self.bids[price_str] = size_str
                    # size=0 means remove, ignore in snapshot
                except (ValueError, TypeError):
                    continue
            
            # Apply asks (sorted ascending by price)
            for level in asks_raw:
                if not isinstance(level, list) or len(level) < 2:
                    continue
                price_str = str(level[0])
                size_str = str(level[1])
                
                try:
                    float(price_str)  # Validate price is numeric
                    size_float = float(size_str)
                    
                    if size_float > 0:
                        self.asks[price_str] = size_str
                    # size=0 means remove, ignore in snapshot
                except (ValueError, TypeError):
                    continue
            
            # Sort: bids descending, asks ascending
            self.bids = OrderedDict(
                sorted(self.bids.items(), key=lambda x: float(x[0]), reverse=True)
            )
            self.asks = OrderedDict(
                sorted(self.asks.items(), key=lambda x: float(x[0]))
            )
            
            self.last_ts_event_ms = int(snapshot_data.get("ts", 0))
            self.last_checksum = int(snapshot_data.get("checksum", 0))
            self.seq_id = snapshot_data.get("seqId")
            self.prev_seq_id = snapshot_data.get("prevSeqId")
            self._is_valid = True
            
            log.info(
                f"OrderBookL2[{self.inst_id}] applied snapshot: "
                f"bids={len(self.bids)}, asks={len(self.asks)}, "
                f"checksum={self.last_checksum}"
            )
            return True
            
        except Exception as e:
            log.error(
                f"Error applying snapshot to OrderBookL2[{self.inst_id}]: {e}"
            )
            self._is_valid = False
            return False
    
    def apply_updates(self, update_data: Dict) -> Tuple[bool, bool]:
        """
        Apply update - modifies current book state
        Returns: (success, checksum_ok)
        """
        try:
            # Validate sequence if available
            seq_id = update_data.get("seqId")
            prev_seq_id = update_data.get("prevSeqId")
            checksum = int(update_data.get("checksum", 0))
            
            checksum_ok = True
            if self.seq_id is not None and prev_seq_id is not None:
                if prev_seq_id != self.seq_id:
                    log.warning(
                        f"Sequence mismatch for {self.inst_id}: "
                        f"expected prevSeqId={self.seq_id}, got {prev_seq_id}"
                    )
                    checksum_ok = False
            
            bids_raw = update_data.get("bids", [])
            asks_raw = update_data.get("asks", [])
            
            # Apply bid updates
            for level in bids_raw:
                if not isinstance(level, list) or len(level) < 2:
                    continue
                price_str = str(level[0])
                size_str = str(level[1])
                
                try:
                    size_float = float(size_str)
                    if size_float > 0:
                        self.bids[price_str] = size_str
                    else:
                        # size=0 means remove the level
                        self.bids.pop(price_str, None)
                except (ValueError, TypeError):
                    continue
            
            # Apply ask updates
            for level in asks_raw:
                if not isinstance(level, list) or len(level) < 2:
                    continue
                price_str = str(level[0])
                size_str = str(level[1])
                
                try:
                    size_float = float(size_str)
                    if size_float > 0:
                        self.asks[price_str] = size_str
                    else:
                        # size=0 means remove the level
                        self.asks.pop(price_str, None)
                except (ValueError, TypeError):
                    continue
            
            # Re-sort after updates
            self.bids = OrderedDict(
                sorted(self.bids.items(), key=lambda x: float(x[0]), reverse=True)
            )
            self.asks = OrderedDict(
                sorted(self.asks.items(), key=lambda x: float(x[0]))
            )
            
            self.last_ts_event_ms = int(update_data.get("ts", 0))
            self.last_checksum = checksum
            if seq_id is not None:
                self.seq_id = seq_id
            if prev_seq_id is not None:
                self.prev_seq_id = prev_seq_id
            
            return True, checksum_ok
            
        except Exception as e:
            log.error(
                f"Error applying update to OrderBookL2[{self.inst_id}]: {e}"
            )
            return False, False
    
    def to_snapshot_rows(
        self,
        snapshot_id: str,
        ts_event_ms: int,
        max_levels: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Convert current book state to normalized rows for ClickHouse.
        Each row represents one level (bid or ask) of the snapshot.
        Returns list of dicts with: snapshot_id (UUID), instId, ts_event_ms, 
        side (Enum8: 1=bid, 2=ask), price, size, level (UInt16)
        """
        if not self._is_valid:
            return []
        
        rows = []
        limit = max_levels or self.max_depth
        
        # Add bid levels (top N, highest price first)
        for idx, (price_str, size_str) in enumerate(list(self.bids.items())[:limit]):
            try:
                # Convert to float for ClickHouse Float64 compatibility
                price = float(price_str)
                size = float(size_str)
                rows.append({
                    "snapshot_id": snapshot_id,
                    "instId": self.inst_id,
                    "ts_event_ms": ts_event_ms,
                    "side": 1,  # Enum8: 'bid' = 1
                    "price": price,
                    "size": size,
                    "level": idx + 1,  # UInt16: level number starting from 1
                })
            except (ValueError, TypeError) as e:
                log.warning(
                    f"Invalid price/size for bid level {idx} in {self.inst_id}: "
                    f"price={price_str}, size={size_str}, error={e}"
                )
                continue
        
        # Add ask levels (top N, lowest price first)
        for idx, (price_str, size_str) in enumerate(list(self.asks.items())[:limit]):
            try:
                # Convert to float for ClickHouse Float64 compatibility
                price = float(price_str)
                size = float(size_str)
                rows.append({
                    "snapshot_id": snapshot_id,
                    "instId": self.inst_id,
                    "ts_event_ms": ts_event_ms,
                    "side": 2,  # Enum8: 'ask' = 2
                    "price": price,
                    "size": size,
                    "level": idx + 1,  # UInt16: level number starting from 1
                })
            except (ValueError, TypeError) as e:
                log.warning(
                    f"Invalid price/size for ask level {idx} in {self.inst_id}: "
                    f"price={price_str}, size={size_str}, error={e}"
                )
                continue
        
        return rows
    
    def is_valid(self) -> bool:
        """Check if book state is valid (has received at least one snapshot)"""
        return self._is_valid
    
    def reset(self) -> None:
        """Reset book state (call on resubscribe/reconnect)"""
        self.bids.clear()
        self.asks.clear()
        self._is_valid = False
        self.last_ts_event_ms = 0
        self.last_checksum = 0
        self.seq_id = None
        self.prev_seq_id = None
        log.info(f"OrderBookL2[{self.inst_id}] reset")

