from okx_hft.storage.interfaces import IStorage
from typing import Dict, Any, Sequence
import clickhouse_connect


class ClickHouseStorage(IStorage):
    def __init__(self, dsn: str, user: str, password: str, db: str) -> None:
        # Parse DSN to extract host and port
        from urllib.parse import urlparse
        parsed = urlparse(dsn)
        host = parsed.hostname or "localhost"
        port = parsed.port or 8123
        
        # Store DSN and credentials for HTTP requests
        self.dsn = dsn
        self.user = user
        self.password = password
        self.db = db  # Store db name early for use in write methods
        
        print(
            f"Connecting to ClickHouse: host={host}, "
            f"port={port}, user={user}, db={db}"
        )
        # First connect without database to create it
        temp_client = clickhouse_connect.get_client(
            host=host, port=port, username=user, password=password
        )
        print("Connected to ClickHouse successfully")
        temp_client.command(f"CREATE DATABASE IF NOT EXISTS {db}")
        print(f"Created database {db}")
        # Now connect with the database
        self.client = clickhouse_connect.get_client(
            host=host, port=port, username=user, password=password, database=db
        )
        print(f"Connected to database {db}")
        
        # Create schema with error handling
        try:
            self._ensure_schema(db)
            print("Schema ensured successfully")
        except Exception as e:
            print(f"ERROR: Failed to ensure schema: {str(e)}")
            raise

    def _ensure_schema(self, db: str) -> None:
        # Create tables in database with error handling
        tables = [
            ("trades", 
             f"CREATE TABLE IF NOT EXISTS {db}.trades("
             "instId String, ts_event_ms UInt64, tradeId String, "
             "px Float64, sz Float64, side String, ts_ingest_ms UInt64"
             ") ENGINE=MergeTree() "
             "ORDER BY (instId, ts_event_ms, tradeId)"),
            ("funding_rates",
             f"CREATE TABLE IF NOT EXISTS {db}.funding_rates("
             "instId String, fundingRate Float64, "
             "fundingTime UInt64, nextFundingTime UInt64, "
             "ts_event_ms UInt64, ts_ingest_ms UInt64"
             ") ENGINE=MergeTree() "
             "ORDER BY (instId, ts_event_ms)"),
            ("mark_prices",
             f"CREATE TABLE IF NOT EXISTS {db}.mark_prices("
             "instId String, markPx Float64, idxPx Float64, idxTs UInt64, "
             "ts_event_ms UInt64, ts_ingest_ms UInt64"
             ") ENGINE=MergeTree() "
             "ORDER BY (instId, ts_event_ms)"),
            ("tickers",
             f"CREATE TABLE IF NOT EXISTS {db}.tickers("
             "instId String, last Float64, lastSz Float64, "
             "bidPx Float64, bidSz Float64, askPx Float64, askSz Float64, "
             "open24h Float64, high24h Float64, low24h Float64, "
             "vol24h Float64, volCcy24h Float64, "
             "ts_event_ms UInt64, ts_ingest_ms UInt64"
             ") ENGINE=MergeTree() "
             "ORDER BY (instId, ts_event_ms)"),
            ("open_interest",
             f"CREATE TABLE IF NOT EXISTS {db}.open_interest("
             "instId String, oi Float64, oiCcy Float64, "
             "ts_event_ms UInt64, ts_ingest_ms UInt64"
             ") ENGINE=MergeTree() "
             "ORDER BY (instId, ts_event_ms)"),
        ]
        
        for table_name, create_sql in tables:
            try:
                self.client.command(create_sql)
                print(f"Created/verified table {db}.{table_name}")
            except Exception as e:
                print(f"ERROR creating table {db}.{table_name}: {str(e)}")
                raise
        
        # Create orderbook_snapshots table
        try:
            create_snapshots_sql = (
                f"CREATE TABLE IF NOT EXISTS {db}.orderbook_snapshots ("
                "instId String, "
                "ts_event_ms UInt64, "
                "ts_event DateTime64(3) ALIAS toDateTime64(ts_event_ms/1000, 3), "
                "ts_ingest_ms UInt64, "
                "ts_ingest DateTime64(3) ALIAS toDateTime64(ts_ingest_ms/1000, 3), "
                "bids Nested (price Decimal(20,8), size Decimal(20,8)), "
                "asks Nested (price Decimal(20,8), size Decimal(20,8)), "
                "checksum Int64"
                ") ENGINE = MergeTree() "
                "ORDER BY (instId, ts_event_ms) "
                "TTL toDateTime64(ts_event_ms/1000, 3) + toIntervalDay(7) "
                "SETTINGS index_granularity = 8192"
            )
            self.client.command(create_snapshots_sql)
            print(f"Created/verified table {db}.orderbook_snapshots")
            
            # Add comments to orderbook_snapshots columns
            # (these may fail if table already exists, so we ignore errors)
            try:
                self.client.command(
                    f"ALTER TABLE {db}.orderbook_snapshots "
                    f"MODIFY COLUMN instId COMMENT "
                    "'Instrument ID (e.g., BTC-USDT-SWAP)'"
                )
            except Exception:
                pass  # Column comment may already exist
            try:
                self.client.command(
                    f"ALTER TABLE {db}.orderbook_snapshots "
                    f"MODIFY COLUMN ts_event_ms COMMENT "
                    "'Event timestamp from OKX (milliseconds)'"
                )
            except Exception:
                pass
            try:
                self.client.command(
                    f"ALTER TABLE {db}.orderbook_snapshots "
                    f"MODIFY COLUMN ts_ingest_ms COMMENT "
                    "'Ingestion timestamp (local, milliseconds)'"
                )
            except Exception:
                pass
            try:
                self.client.command(
                    f"ALTER TABLE {db}.orderbook_snapshots "
                    f"MODIFY COLUMN checksum COMMENT "
                    "'OKX checksum for validation'"
                )
            except Exception:
                pass
        except Exception as e:
            print(f"ERROR creating table {db}.orderbook_snapshots: {str(e)}")
            raise
        
        # Create orderbook_updates table
        try:
            create_updates_sql = (
                f"CREATE TABLE IF NOT EXISTS {db}.orderbook_updates ("
                "instId String, "
                "ts_event_ms UInt64, "
                "ts_event DateTime64(3) ALIAS toDateTime64(ts_event_ms/1000, 3), "
                "ts_ingest_ms UInt64, "
                "ts_ingest DateTime64(3) ALIAS toDateTime64(ts_ingest_ms/1000, 3), "
                "bids_delta Nested (price Decimal(20,8), size Decimal(20,8)), "
                "asks_delta Nested (price Decimal(20,8), size Decimal(20,8)), "
                "checksum Int64"
                ") ENGINE = MergeTree() "
                "ORDER BY (instId, ts_event_ms) "
                "TTL toDateTime64(ts_event_ms/1000, 3) + toIntervalDay(7) "
                "SETTINGS index_granularity = 8192"
            )
            self.client.command(create_updates_sql)
            print(f"Created/verified table {db}.orderbook_updates")
            
            # Add comments to orderbook_updates columns
            # (these may fail if table already exists, so we ignore errors)
            try:
                self.client.command(
                    f"ALTER TABLE {db}.orderbook_updates "
                    f"MODIFY COLUMN instId COMMENT "
                    "'Instrument ID (e.g., BTC-USDT-SWAP)'"
                )
            except Exception:
                pass
            try:
                self.client.command(
                    f"ALTER TABLE {db}.orderbook_updates "
                    f"MODIFY COLUMN ts_event_ms COMMENT "
                    "'Event timestamp from OKX (milliseconds)'"
                )
            except Exception:
                pass
            try:
                self.client.command(
                    f"ALTER TABLE {db}.orderbook_updates "
                    f"MODIFY COLUMN ts_ingest_ms COMMENT "
                    "'Ingestion timestamp (local, milliseconds)'"
                )
            except Exception:
                pass
            try:
                self.client.command(
                    f"ALTER TABLE {db}.orderbook_updates "
                    f"MODIFY COLUMN bids_delta COMMENT "
                    "'Changed bid levels (size=0 means remove level)'"
                )
            except Exception:
                pass
            try:
                self.client.command(
                    f"ALTER TABLE {db}.orderbook_updates "
                    f"MODIFY COLUMN asks_delta COMMENT "
                    "'Changed ask levels (size=0 means remove level)'"
                )
            except Exception:
                pass
            try:
                self.client.command(
                    f"ALTER TABLE {db}.orderbook_updates "
                    f"MODIFY COLUMN checksum COMMENT "
                    "'OKX checksum for validation'"
                )
            except Exception:
                pass
        except Exception as e:
            print(f"ERROR creating table {db}.orderbook_updates: {str(e)}")
            raise


    async def write_trades(self, batch: Sequence[Dict[str, Any]]) -> None:
        if not batch:
            return
        try:
            print(f"Inserting {len(batch)} trades to ClickHouse")
            print(f"Sample data: {batch[0] if batch else 'empty'}")

            # Попробуем использовать HTTP API напрямую
            import aiohttp

            # Формируем INSERT запрос
            values = []
            for trade in batch:
                values.append(
                    f"('{trade['instId']}', {trade['ts_event_ms']}, "
                    f"'{trade['tradeId']}', {trade['px']}, {trade['sz']}, "
                    f"'{trade['side']}', {trade['ts_ingest_ms']})"
                )

            query = f"INSERT INTO {self.db}.trades VALUES {', '.join(values)}"
            print(f"Query: {query}")

            # Parse DSN to get host and port for auth
            from urllib.parse import urlparse
            parsed = urlparse(self.dsn)
            host = parsed.hostname or "localhost"
            port = parsed.port or 8123
            
            # Create auth URL with credentials
            auth_url = f"http://{host}:{port}/"
            
            # Use stored credentials
            user = self.user
            password = self.password
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    auth_url, 
                    data=query,
                    auth=aiohttp.BasicAuth(user, password)
                ) as response:
                    result = await response.text()
                    print(f"HTTP insert result: {result}, status: {response.status}")

        except Exception as e:
            print(f"ClickHouse insert error: {str(e)}")
            raise Exception(f"ClickHouse error writing trades: {str(e)}")

    async def write_funding_rates(self, batch: Sequence[Dict[str, Any]]) -> None:
        if not batch:
            return
        try:
            print(f"Inserting {len(batch)} funding rates to ClickHouse")
            print(f"Sample data: {batch[0] if batch else 'empty'}")

            # Попробуем использовать HTTP API напрямую
            import aiohttp

            # Формируем INSERT запрос
            values = []
            for rate in batch:
                values.append(
                    f"('{rate['instId']}', {rate['fundingRate']}, "
                    f"{rate['fundingTime']}, {rate['nextFundingTime']}, "
                    f"{rate['ts_event_ms']}, {rate['ts_ingest_ms']})"
                )

            query = f"INSERT INTO {self.db}.funding_rates VALUES {', '.join(values)}"
            print(f"Query: {query}")

            # Parse DSN to get host and port for auth
            from urllib.parse import urlparse
            parsed = urlparse(self.dsn)
            host = parsed.hostname or "localhost"
            port = parsed.port or 8123
            
            # Create auth URL with credentials
            auth_url = f"http://{host}:{port}/"
            
            # Use stored credentials
            user = self.user
            password = self.password
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    auth_url, 
                    data=query,
                    auth=aiohttp.BasicAuth(user, password)
                ) as response:
                    result = await response.text()
                    print(f"HTTP insert result: {result}, status: {response.status}")

        except Exception as e:
            print(f"ClickHouse insert error: {str(e)}")
            raise Exception(f"ClickHouse error writing funding_rates: {str(e)}")

    async def write_mark_prices(self, batch: Sequence[Dict[str, Any]]) -> None:
        if not batch:
            return
        try:
            print(f"Inserting {len(batch)} mark prices to ClickHouse")
            print(f"Sample data: {batch[0] if batch else 'empty'}")

            # Попробуем использовать HTTP API напрямую
            import asyncio
            import aiohttp

            # Формируем INSERT запрос
            values = []
            for price in batch:
                values.append(
                    f"('{price['instId']}', {price['markPx']}, "
                    f"{price['idxPx']}, {price['idxTs']}, "
                    f"{price['ts_event_ms']}, {price['ts_ingest_ms']})"
                )

            query = f"INSERT INTO {self.db}.mark_prices VALUES {', '.join(values)}"
            print(f"Query: {query}")

            # Parse DSN to get host and port for auth
            from urllib.parse import urlparse
            parsed = urlparse(self.dsn)
            host = parsed.hostname or "localhost"
            port = parsed.port or 8123
            
            # Create auth URL with credentials
            auth_url = f"http://{host}:{port}/"
            
            # Use stored credentials
            user = self.user
            password = self.password
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    auth_url, 
                    data=query,
                    auth=aiohttp.BasicAuth(user, password)
                ) as response:
                    result = await response.text()
                    print(f"HTTP insert result: {result}, status: {response.status}")

        except Exception as e:
            print(f"ClickHouse insert error: {str(e)}")
            raise Exception(f"ClickHouse error writing mark_prices: {str(e)}")

    async def write_tickers(self, batch: Sequence[Dict[str, Any]]) -> None:
        if not batch:
            return
        try:
            print(f"Inserting {len(batch)} tickers to ClickHouse")
            print(f"Sample data: {batch[0] if batch else 'empty'}")

            import asyncio
            import aiohttp

            values = []
            for ticker in batch:
                values.append(
                    f"('{ticker['instId']}', {ticker['last']}, "
                    f"{ticker['lastSz']}, {ticker['bidPx']}, {ticker['bidSz']}, "
                    f"{ticker['askPx']}, {ticker['askSz']}, {ticker['open24h']}, "
                    f"{ticker['high24h']}, {ticker['low24h']}, {ticker['vol24h']}, "
                    f"{ticker['volCcy24h']}, {ticker['ts_event_ms']}, "
                    f"{ticker['ts_ingest_ms']})"
                )

            query = f"INSERT INTO {self.db}.tickers VALUES {', '.join(values)}"
            print(f"Query: {query}")

            from urllib.parse import urlparse
            parsed = urlparse(self.dsn)
            host = parsed.hostname or "localhost"
            port = parsed.port or 8123
            
            auth_url = f"http://{host}:{port}/"
            user = self.user
            password = self.password
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    auth_url, 
                    data=query,
                    auth=aiohttp.BasicAuth(user, password)
                ) as response:
                    result = await response.text()
                    print(f"HTTP insert result: {result}, status: {response.status}")

        except Exception as e:
            print(f"ClickHouse insert error: {str(e)}")
            raise Exception(f"ClickHouse error writing tickers: {str(e)}")

    async def write_open_interest(self, batch: Sequence[Dict[str, Any]]) -> None:
        if not batch:
            return
        try:
            print(f"Inserting {len(batch)} open interest records to ClickHouse")
            print(f"Sample data: {batch[0] if batch else 'empty'}")

            import asyncio
            import aiohttp

            values = []
            for oi in batch:
                values.append(
                    f"('{oi['instId']}', {oi['oi']}, {oi['oiCcy']}, "
                    f"{oi['ts_event_ms']}, {oi['ts_ingest_ms']})"
                )

            query = f"INSERT INTO {self.db}.open_interest VALUES {', '.join(values)}"
            print(f"Query: {query}")

            from urllib.parse import urlparse
            parsed = urlparse(self.dsn)
            host = parsed.hostname or "localhost"
            port = parsed.port or 8123
            
            auth_url = f"http://{host}:{port}/"
            user = self.user
            password = self.password
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    auth_url, 
                    data=query,
                    auth=aiohttp.BasicAuth(user, password)
                ) as response:
                    result = await response.text()
                    print(f"HTTP insert result: {result}, status: {response.status}")

        except Exception as e:
            print(f"ClickHouse insert error: {str(e)}")
            raise Exception(f"ClickHouse error writing open_interest: {str(e)}")

    async def write_orderbook_snapshots(self, batch: Sequence[Dict[str, Any]]) -> None:
        """Write orderbook snapshots using JSONEachRow format for Nested types"""
        if not batch:
            return
        try:
            import aiohttp
            import orjson
            from okx_hft.utils.logging import get_logger
            
            log = get_logger(__name__)
            log.info(f"Inserting {len(batch)} orderbook snapshots to ClickHouse")
            
            from urllib.parse import urlparse
            parsed = urlparse(self.dsn)
            host = parsed.hostname or "localhost"
            port = parsed.port or 8123
            
            # Prepare JSON rows for JSONEachRow format
            rows = []
            for snapshot in batch:
                # Format Nested types: bids.price and bids.size as arrays
                row = {
                    "instId": snapshot["instId"],
                    "ts_event_ms": snapshot["ts_event_ms"],
                    "ts_ingest_ms": snapshot["ts_ingest_ms"],
                    "checksum": snapshot.get("checksum", 0),
                    "bids.price": [float(level["price"]) for level in snapshot.get("bids", [])],
                    "bids.size": [float(level["size"]) for level in snapshot.get("bids", [])],
                    "asks.price": [float(level["price"]) for level in snapshot.get("asks", [])],
                    "asks.size": [float(level["size"]) for level in snapshot.get("asks", [])],
                }
                rows.append(orjson.dumps(row).decode('utf-8'))
            
            # Join rows with newlines for JSONEachRow format
            data = "\n".join(rows)
            
            # Use JSONEachRow format
            url = f"http://{host}:{port}/?query=INSERT INTO {self.db}.orderbook_snapshots FORMAT JSONEachRow"
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    data=data.encode('utf-8'),
                    auth=aiohttp.BasicAuth(self.user, self.password),
                    headers={"Content-Type": "application/x-ndjson"}
                ) as response:
                    result = await response.text()
                    if response.status != 200:
                        log.error(f"ClickHouse HTTP error: {response.status}, {result}")
                        raise Exception(f"ClickHouse HTTP error: {response.status}, {result}")
                    log.info(f"Inserted {len(batch)} orderbook snapshots, status: {response.status}")
                    
        except Exception as e:
            log.error(f"ClickHouse insert error for orderbook_snapshots: {str(e)}")
            raise Exception(f"ClickHouse error writing orderbook_snapshots: {str(e)}")

    async def write_orderbook_updates(self, batch: Sequence[Dict[str, Any]]) -> None:
        """Write orderbook updates using JSONEachRow format for Nested types"""
        if not batch:
            return
        try:
            import aiohttp
            import orjson
            from okx_hft.utils.logging import get_logger
            
            log = get_logger(__name__)
            log.info(f"Inserting {len(batch)} orderbook updates to ClickHouse")
            
            from urllib.parse import urlparse
            parsed = urlparse(self.dsn)
            host = parsed.hostname or "localhost"
            port = parsed.port or 8123
            
            # Prepare JSON rows for JSONEachRow format
            rows = []
            for update in batch:
                # Format Nested types: bids_delta.price and bids_delta.size as arrays
                row = {
                    "instId": update["instId"],
                    "ts_event_ms": update["ts_event_ms"],
                    "ts_ingest_ms": update["ts_ingest_ms"],
                    "checksum": update.get("checksum", 0),
                    "bids_delta.price": [float(level["price"]) for level in update.get("bids_delta", [])],
                    "bids_delta.size": [float(level["size"]) for level in update.get("bids_delta", [])],
                    "asks_delta.price": [float(level["price"]) for level in update.get("asks_delta", [])],
                    "asks_delta.size": [float(level["size"]) for level in update.get("asks_delta", [])],
                }
                rows.append(orjson.dumps(row).decode('utf-8'))
            
            # Join rows with newlines for JSONEachRow format
            data = "\n".join(rows)
            
            # Use JSONEachRow format
            url = f"http://{host}:{port}/?query=INSERT INTO {self.db}.orderbook_updates FORMAT JSONEachRow"
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    data=data.encode('utf-8'),
                    auth=aiohttp.BasicAuth(self.user, self.password),
                    headers={"Content-Type": "application/x-ndjson"}
                ) as response:
                    result = await response.text()
                    if response.status != 200:
                        log.error(f"ClickHouse HTTP error: {response.status}, {result}")
                        raise Exception(f"ClickHouse HTTP error: {response.status}, {result}")
                    log.info(f"Inserted {len(batch)} orderbook updates, status: {response.status}")
                    
        except Exception as e:
            log.error(f"ClickHouse insert error for orderbook_updates: {str(e)}")
            raise Exception(f"ClickHouse error writing orderbook_updates: {str(e)}")

    async def flush(self) -> None:
        pass
