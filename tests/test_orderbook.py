"""Unit tests for orderbook handler"""
import pytest
from src.okx_hft.handlers.orderbook import OrderBookHandler


def test_process_snapshot():
    """Test snapshot processing"""
    handler = OrderBookHandler(storage=None)
    
    snapshot_data = {
        "ts": "1704067200000",
        "checksum": 123456,
        "bids": [
            ["50000.5", "1.0", "0", "1"],
            ["50000.0", "2.0", "0", "2"],
        ],
        "asks": [
            ["50001.0", "1.5", "0", "3"],
            ["50001.5", "2.5", "0", "4"],
        ]
    }
    
    result = handler._process_snapshot(
        snapshot_data,
        "BTC-USDT-SWAP",
        1704067200100
    )
    
    assert result is not None
    assert result["instId"] == "BTC-USDT-SWAP"
    assert result["ts_event_ms"] == 1704067200000
    assert result["ts_ingest_ms"] == 1704067200100
    assert result["checksum"] == 123456
    assert len(result["bids"]) == 2
    assert len(result["asks"]) == 2
    assert result["bids"][0]["price"] == "50000.5"
    assert result["bids"][0]["size"] == "1.0"


def test_process_update():
    """Test update processing"""
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
    
    result = handler._process_update(
        update_data,
        "BTC-USDT-SWAP",
        1704067200600
    )
    
    assert result is not None
    assert result["instId"] == "BTC-USDT-SWAP"
    assert result["ts_event_ms"] == 1704067200500
    assert result["ts_ingest_ms"] == 1704067200600
    assert result["checksum"] == 123457
    assert len(result["bids_delta"]) == 2
    assert len(result["asks_delta"]) == 1
    # Size=0 should still be included in delta
    assert result["bids_delta"][1]["size"] == "0"


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


def test_backpressure():
    """Test backpressure handling"""
    handler = OrderBookHandler(storage=None, batch_size=10)
    
    # Fill buffer beyond threshold
    for i in range(11):
        handler.batch_snapshots.append({
            "instId": "BTC-USDT-SWAP",
            "ts_event_ms": i,
            "ts_ingest_ms": i,
            "bids": [],
            "asks": [],
            "checksum": 0
        })
    
    # Should have max_buffer_size items after backpressure
    assert len(handler.batch_snapshots) == handler._max_buffer_size


