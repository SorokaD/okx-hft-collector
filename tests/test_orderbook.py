"""Unit tests for orderbook handler"""
import pytest
import time
from src.okx_hft.handlers.orderbook import OrderBookHandler


def test_process_update():
    """Test update processing with ts_ingest_ms"""
    handler = OrderBookHandler(storage=None)
    
    update_data = {
        "ts": "1704067200500",
        "checksum": 123457,
        "bids": [
            ["50000.6", "1.5"],  # Price update
            ["50000.0", "0"],    # Remove level (size=0)
        ],
        "asks": [
            ["50001.1", "2.0"],  # Price update
        ]
    }
    
    ts_ingest_ms = 1704067200600
    result = handler._process_update(
        update_data,
        "BTC-USDT-SWAP",
        ts_ingest_ms
    )
    
    assert result is not None
    assert result["instId"] == "BTC-USDT-SWAP"
    assert result["ts_event_ms"] == 1704067200500
    assert result["ts_ingest_ms"] == ts_ingest_ms
    assert result["checksum"] == 123457
    assert len(result["bids_delta"]) == 2
    assert len(result["asks_delta"]) == 1
    # Size=0 should still be included in delta
    assert result["bids_delta"][1]["size"] == "0"


def test_process_update_ts_ingest_ms_is_present():
    """Test that ts_ingest_ms is always present in processed update"""
    handler = OrderBookHandler(storage=None)
    
    update_data = {
        "ts": "1704067200000",
        "checksum": 12345,
        "bids": [["50000.0", "1.0"]],
        "asks": [["50001.0", "1.0"]],
    }
    
    ts_ingest_ms = int(time.time() * 1000)
    result = handler._process_update(update_data, "BTC-USDT-SWAP", ts_ingest_ms)
    
    # ts_ingest_ms must be present
    assert "ts_ingest_ms" in result
    assert isinstance(result["ts_ingest_ms"], int)
    assert result["ts_ingest_ms"] == ts_ingest_ms


def test_process_update_ts_ingest_ms_is_recent():
    """Test that ts_ingest_ms is close to current time"""
    handler = OrderBookHandler(storage=None)
    
    update_data = {
        "ts": "1704067200000",
        "checksum": 12345,
        "bids": [["50000.0", "1.0"]],
        "asks": [],
    }
    
    now_ms = int(time.time() * 1000)
    result = handler._process_update(update_data, "BTC-USDT-SWAP", now_ms)
    
    # ts_ingest_ms should be within 1 second of now
    delta_ms = abs(result["ts_ingest_ms"] - now_ms)
    assert delta_ms < 1000, f"ts_ingest_ms too far from now: delta={delta_ms}ms"


def test_process_update_ts_ingest_ms_gte_ts_event_ms():
    """Test that ts_ingest_ms >= ts_event_ms (with small tolerance for clock drift)"""
    handler = OrderBookHandler(storage=None)
    
    ts_event_ms = 1704067200000
    ts_ingest_ms = 1704067200100  # 100ms later
    
    update_data = {
        "ts": str(ts_event_ms),
        "checksum": 12345,
        "bids": [["50000.0", "1.0"]],
        "asks": [],
    }
    
    result = handler._process_update(update_data, "BTC-USDT-SWAP", ts_ingest_ms)
    
    # ts_ingest_ms should be >= ts_event_ms
    # Allow small negative values for clock drift (up to 1 second)
    latency = result["ts_ingest_ms"] - result["ts_event_ms"]
    assert latency >= -1000, f"ts_ingest_ms too far before ts_event_ms: latency={latency}ms"


def test_parse_levels_max_50():
    """Test that levels are limited to 50 for snapshots"""
    handler = OrderBookHandler(storage=None)
    
    # Create 60 levels
    many_levels = [[f"50000.{i}", "1.0"] for i in range(60)]
    
    result = handler._parse_levels(many_levels, max_levels=50)
    
    assert len(result) == 50


def test_parse_levels_no_limit():
    """Test that updates are not limited"""
    handler = OrderBookHandler(storage=None)
    
    many_levels = [[f"50000.{i}", "1.0"] for i in range(60)]
    
    result = handler._parse_levels(many_levels, max_levels=None)
    
    assert len(result) == 60


def test_parse_levels_invalid_data():
    """Test that invalid levels are skipped"""
    handler = OrderBookHandler(storage=None)
    
    invalid_levels = [
        ["50000.0", "1.0"],      # Valid
        ["invalid", "1.0"],      # Invalid price
        ["50001.0", "invalid"],  # Invalid size
        [],                      # Empty
        ["50002.0"],             # Missing size
        ["50003.0", "2.0"],      # Valid
    ]
    
    result = handler._parse_levels(invalid_levels, max_levels=None)
    
    # Only 2 valid levels
    assert len(result) == 2
    assert result[0]["price"] == "50000.0"
    assert result[1]["price"] == "50003.0"


def test_batch_updates_accumulation():
    """Test that updates accumulate in batch"""
    handler = OrderBookHandler(storage=None, batch_size=100)
    
    assert len(handler.batch_updates) == 0
    
    # Manually add update
    handler.batch_updates.append({
        "instId": "BTC-USDT-SWAP",
        "ts_event_ms": 1704067200000,
        "ts_ingest_ms": 1704067200100,
        "bids_delta": [],
        "asks_delta": [],
        "checksum": 0,
    })
    
    assert len(handler.batch_updates) == 1
    assert handler.batch_updates[0]["ts_ingest_ms"] == 1704067200100
