#!/usr/bin/env python3
"""Local test script for OKX orderbook ingestion.

Run with:
    python scripts/okx_orderbook_local.py

Prints first 3 parsed messages (snapshot and updates) for a single instrument.
"""
import asyncio
import json
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import aiohttp
import orjson
from okx_hft.handlers.orderbook import OrderBookHandler


async def test_orderbook():
    """Test orderbook handler with real OKX WebSocket"""
    ws_url = "wss://ws.okx.com:8443/ws/v5/public"
    inst_id = "BTC-USDT-SWAP"  # Single instrument for testing
    
    handler = OrderBookHandler(storage=None, batch_size=200, flush_ms=100)
    
    # Counter for messages
    snapshot_count = 0
    update_count = 0
    max_messages = 3
    
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(ws_url, heartbeat=20) as ws:
            # Subscribe to books50-l2-tbt
            sub_payload = {
                "op": "subscribe",
                "args": [{"channel": "books50-l2-tbt", "instId": inst_id}]
            }
            await ws.send_json(sub_payload)
            print(f"Subscribed to {inst_id}")
            
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = orjson.loads(msg.data)
                    
                    # Check for subscription confirmation
                    if data.get("event") == "subscribe":
                        print(f"Subscription confirmed: {data}")
                        continue
                    
                    arg = data.get("arg", {})
                    channel = arg.get("channel", "")
                    
                    if channel == "books50-l2-tbt":
                        action = data.get("action", "snapshot")
                        
                        if action == "snapshot" and snapshot_count < max_messages:
                            snapshot_count += 1
                            print(f"\n=== Snapshot #{snapshot_count} ===")
                            print(f"Raw message: {json.dumps(data, indent=2)}")
                            
                            # Process through handler
                            processed = handler._process_snapshot(
                                data.get("data", [{}])[0] if data.get("data") else {},
                                inst_id,
                                int(asyncio.get_event_loop().time() * 1000)
                            )
                            print(f"Processed: {json.dumps(processed, indent=2)}")
                            
                        elif action == "update" and update_count < max_messages:
                            update_count += 1
                            print(f"\n=== Update #{update_count} ===")
                            print(f"Raw message: {json.dumps(data, indent=2)}")
                            
                            # Process through handler
                            processed = handler._process_update(
                                data.get("data", [{}])[0] if data.get("data") else {},
                                inst_id,
                                int(asyncio.get_event_loop().time() * 1000)
                            )
                            print(f"Processed: {json.dumps(processed, indent=2)}")
                    
                    # Stop after getting enough messages
                    if snapshot_count >= max_messages and update_count >= max_messages:
                        print("\n✓ Got required messages, exiting...")
                        break
                        
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    print(f"WebSocket error: {msg}")
                    break


if __name__ == "__main__":
    try:
        asyncio.run(test_orderbook())
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


