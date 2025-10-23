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
        
        print(
            f"Connecting to ClickHouse: host={host}, port={port}, user={user}, db={db}"
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
        self._ensure_schema(db)
        print("Schema ensured")

    def _ensure_schema(self, db: str) -> None:
        self.client.command(
            "CREATE TABLE IF NOT EXISTS lob_updates("
            "instId String, ts_event_ms UInt64, seqId UInt64, "
            "bids Array(Tuple(Float64, Float64)), asks Array(Tuple(Float64, Float64)), "
            "spread Float64, mid Float64, cs_ok UInt8, ts_ingest_ms UInt64"
            ") ENGINE=ReplacingMergeTree(ts_ingest_ms) "
            "ORDER BY (instId, ts_event_ms, seqId)"
        )
        self.client.command(
            "CREATE TABLE IF NOT EXISTS trades("
            "instId String, ts_event_ms UInt64, tradeId String, px Float64, sz Float64, "
            "side String, ts_ingest_ms UInt64"
            ") ENGINE=MergeTree() "
            "ORDER BY (instId, ts_event_ms, tradeId)"
        )

    async def write_lob_updates(self, batch: Sequence[Dict[str, Any]]) -> None:
        if not batch:
            return
        try:
            # Используем asyncio.to_thread для синхронного вызова в асинхронном контексте
            import asyncio

            result = await asyncio.to_thread(
                self.client.insert,
                "lob_updates",
                batch,
                column_names=list(batch[0].keys()),
            )
            print(
                f"ClickHouse insert result (lob_updates): {result}, type: {type(result)}"
            )
        except Exception as e:
            raise Exception(f"ClickHouse error writing lob_updates: {str(e)}")

    async def write_trades(self, batch: Sequence[Dict[str, Any]]) -> None:
        if not batch:
            return
        try:
            print(f"Inserting {len(batch)} trades to ClickHouse")
            print(f"Sample data: {batch[0] if batch else 'empty'}")

            # Попробуем использовать HTTP API напрямую
            import asyncio
            import aiohttp

            # Формируем INSERT запрос
            values = []
            for trade in batch:
                values.append(
                    f"('{trade['instId']}', {trade['ts_event_ms']}, '{trade['tradeId']}', {trade['px']}, {trade['sz']}, '{trade['side']}', {trade['ts_ingest_ms']})"
                )

            query = f"INSERT INTO default.trades VALUES {', '.join(values)}"
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

    async def flush(self) -> None:
        pass
