from okx_hft.storage.interfaces import IStorage
from typing import Dict, Any, Sequence
import asyncpg
from okx_hft.utils.logging import get_logger

log = get_logger(__name__)


class PostgreSQLStorage(IStorage):
    def __init__(
        self, 
        host: str = "localhost",
        port: int = 5432,
        user: str = "",
        password: str = "",
        database: str = "okx_hft",
        schema: str = "okx_raw"
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.schema = schema
        self.pool: asyncpg.Pool | None = None
        
        log.info(
            f"Initializing PostgreSQLStorage: "
            f"host={host}, port={port}, database={database}, schema={schema}"
        )

    async def connect(self) -> None:
        """Create connection pool and ensure schema exists"""
        try:
            # First connect to postgres database to create our database if needed
            admin_conn = await asyncpg.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database="postgres"
            )
            
            # Create database if not exists
            db_exists = await admin_conn.fetchval(
                "SELECT 1 FROM pg_database WHERE datname = $1",
                self.database
            )
            if not db_exists:
                await admin_conn.execute(
                    f'CREATE DATABASE "{self.database}"'
                )
                log.info(f"Created database {self.database}")
            await admin_conn.close()
            
            # Create connection pool to our database
            self.pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                min_size=2,
                max_size=10
            )
            log.info(f"Connected to PostgreSQL database {self.database}")
            
            # Ensure schema exists
            async with self.pool.acquire() as conn:
                await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}"')
                log.info(f"Created/verified schema {self.schema}")
            
            # Create tables
            await self._ensure_schema()
            log.info("Schema ensured successfully")
            
        except Exception as e:
            log.error(f"ERROR: Failed to connect to PostgreSQL: {str(e)}")
            raise

    async def _ensure_schema(self) -> None:
        """Create all tables in the schema"""
        async with self.pool.acquire() as conn:
            # Set search path to our schema
            await conn.execute(f'SET search_path TO "{self.schema}"')
            
            # Create trades table
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS "{self.schema}".trades (
                    instid VARCHAR(50) NOT NULL,
                    ts_event_ms BIGINT NOT NULL,
                    tradeid VARCHAR(100) NOT NULL,
                    px DOUBLE PRECISION NOT NULL,
                    sz DOUBLE PRECISION NOT NULL,
                    side VARCHAR(10) NOT NULL,
                    ts_ingest_ms BIGINT NOT NULL,
                    PRIMARY KEY (instid, ts_event_ms, tradeid)
                )
            """)
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_trades_instid_ts 
                ON "{self.schema}".trades(instid, ts_event_ms)
            """)
            log.info(f"Created/verified table {self.schema}.trades")
            
            # Create funding_rates table
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS "{self.schema}".funding_rates (
                    instid VARCHAR(50) NOT NULL,
                    fundingrate DOUBLE PRECISION NOT NULL,
                    fundingtime BIGINT NOT NULL,
                    nextfundingtime BIGINT NOT NULL,
                    ts_event_ms BIGINT NOT NULL,
                    ts_ingest_ms BIGINT NOT NULL,
                    PRIMARY KEY (instid, ts_event_ms)
                )
            """)
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_funding_rates_instid_ts 
                ON "{self.schema}".funding_rates(instid, ts_event_ms)
            """)
            log.info(f"Created/verified table {self.schema}.funding_rates")
            
            # Create mark_prices table
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS "{self.schema}".mark_prices (
                    instid VARCHAR(50) NOT NULL,
                    markpx DOUBLE PRECISION NOT NULL,
                    idxpx DOUBLE PRECISION NOT NULL,
                    idxts BIGINT NOT NULL,
                    ts_event_ms BIGINT NOT NULL,
                    ts_ingest_ms BIGINT NOT NULL,
                    PRIMARY KEY (instid, ts_event_ms)
                )
            """)
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_mark_prices_instid_ts 
                ON "{self.schema}".mark_prices(instid, ts_event_ms)
            """)
            log.info(f"Created/verified table {self.schema}.mark_prices")
            
            # Create tickers table
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS "{self.schema}".tickers (
                    instid VARCHAR(50) NOT NULL,
                    last DOUBLE PRECISION,
                    lastsz DOUBLE PRECISION,
                    bidpx DOUBLE PRECISION,
                    bidsz DOUBLE PRECISION,
                    askpx DOUBLE PRECISION,
                    asksz DOUBLE PRECISION,
                    open24h DOUBLE PRECISION,
                    high24h DOUBLE PRECISION,
                    low24h DOUBLE PRECISION,
                    vol24h DOUBLE PRECISION,
                    volccy24h DOUBLE PRECISION,
                    ts_event_ms BIGINT NOT NULL,
                    ts_ingest_ms BIGINT NOT NULL,
                    PRIMARY KEY (instid, ts_event_ms)
                )
            """)
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_tickers_instid_ts 
                ON "{self.schema}".tickers(instid, ts_event_ms)
            """)
            log.info(f"Created/verified table {self.schema}.tickers")
            
            # Create open_interest table
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS "{self.schema}".open_interest (
                    instid VARCHAR(50) NOT NULL,
                    oi DOUBLE PRECISION NOT NULL,
                    oiccy DOUBLE PRECISION NOT NULL,
                    ts_event_ms BIGINT NOT NULL,
                    ts_ingest_ms BIGINT NOT NULL,
                    PRIMARY KEY (instid, ts_event_ms)
                )
            """)
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_open_interest_instid_ts 
                ON "{self.schema}".open_interest(instid, ts_event_ms)
            """)
            log.info(f"Created/verified table {self.schema}.open_interest")
            
            # Migrate oiccy column type if needed (from VARCHAR to DOUBLE PRECISION)
            try:
                await conn.execute(f"""
                    ALTER TABLE "{self.schema}".open_interest 
                    ALTER COLUMN oiccy TYPE DOUBLE PRECISION 
                    USING oiccy::double precision
                """)
                log.info(f"Migrated oiccy column to DOUBLE PRECISION")
            except Exception as e:
                # Column might already be correct type or table doesn't exist yet
                if "does not exist" not in str(e) and "already" not in str(e).lower():
                    log.debug(f"Could not migrate oiccy column (might already be correct): {e}")
            
            # Create orderbook_snapshots table
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS "{self.schema}".orderbook_snapshots (
                    snapshot_id UUID NOT NULL,
                    instid VARCHAR(50) NOT NULL,
                    ts_event_ms BIGINT NOT NULL,
                    side SMALLINT NOT NULL,
                    price DOUBLE PRECISION NOT NULL,
                    size DOUBLE PRECISION NOT NULL,
                    level SMALLINT NOT NULL,
                    PRIMARY KEY (instid, ts_event_ms, snapshot_id, side, price)
                )
            """)
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_orderbook_snapshots_instid_ts 
                ON "{self.schema}".orderbook_snapshots(instid, ts_event_ms)
            """)
            log.info(f"Created/verified table {self.schema}.orderbook_snapshots")
            
            # Create orderbook_updates table (using JSONB for nested data)
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS "{self.schema}".orderbook_updates (
                    instid VARCHAR(50) NOT NULL,
                    ts_event_ms BIGINT NOT NULL,
                    bids_delta JSONB,
                    asks_delta JSONB,
                    checksum BIGINT,
                    PRIMARY KEY (instid, ts_event_ms)
                )
            """)
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_orderbook_updates_instid_ts 
                ON "{self.schema}".orderbook_updates(instid, ts_event_ms)
            """)
            log.info(f"Created/verified table {self.schema}.orderbook_updates")
            
            # Create index_tickers table
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS "{self.schema}".index_tickers (
                    instid VARCHAR(50) NOT NULL,
                    idxpx DOUBLE PRECISION NOT NULL,
                    open24h DOUBLE PRECISION,
                    high24h DOUBLE PRECISION,
                    low24h DOUBLE PRECISION,
                    sodutc0 DOUBLE PRECISION,
                    sodutc8 DOUBLE PRECISION,
                    ts_event_ms BIGINT NOT NULL,
                    ts_ingest_ms BIGINT NOT NULL,
                    PRIMARY KEY (instid, ts_event_ms)
                )
            """)
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_index_tickers_instid_ts 
                ON "{self.schema}".index_tickers(instid, ts_event_ms)
            """)
            log.info(f"Created/verified table {self.schema}.index_tickers")

    async def write_trades(self, batch: Sequence[Dict[str, Any]]) -> None:
        if not batch:
            return
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(f'SET search_path TO "{self.schema}"')
                await conn.executemany(
                    f"""
                    INSERT INTO "{self.schema}".trades 
                    (instid, ts_event_ms, tradeid, px, sz, side, ts_ingest_ms)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (instid, ts_event_ms, tradeid) DO NOTHING
                    """,
                    [
                        (
                            trade["instId"],
                            trade["ts_event_ms"],
                            trade["tradeId"],
                            trade["px"],
                            trade["sz"],
                            trade["side"],
                            trade["ts_ingest_ms"],
                        )
                        for trade in batch
                    ],
                )
        except Exception as e:
            log.error(f"PostgreSQL insert error: {str(e)}")
            raise Exception(f"PostgreSQL error writing trades: {str(e)}")

    async def write_funding_rates(self, batch: Sequence[Dict[str, Any]]) -> None:
        if not batch:
            return
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(f'SET search_path TO "{self.schema}"')
                await conn.executemany(
                    f"""
                    INSERT INTO "{self.schema}".funding_rates 
                    (instid, fundingrate, fundingtime, nextfundingtime, ts_event_ms, ts_ingest_ms)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (instid, ts_event_ms) DO NOTHING
                    """,
                    [
                        (
                            rate["instId"],
                            rate["fundingRate"],
                            rate["fundingTime"],
                            rate["nextFundingTime"],
                            rate["ts_event_ms"],
                            rate["ts_ingest_ms"],
                        )
                        for rate in batch
                    ],
                )
        except Exception as e:
            log.error(f"PostgreSQL insert error: {str(e)}")
            raise Exception(f"PostgreSQL error writing funding_rates: {str(e)}")

    async def write_mark_prices(self, batch: Sequence[Dict[str, Any]]) -> None:
        if not batch:
            return
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(f'SET search_path TO "{self.schema}"')
                await conn.executemany(
                    f"""
                    INSERT INTO "{self.schema}".mark_prices 
                    (instid, markpx, idxpx, idxts, ts_event_ms, ts_ingest_ms)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (instid, ts_event_ms) DO NOTHING
                    """,
                    [
                        (
                            price["instId"],
                            price["markPx"],
                            price["idxPx"],
                            price["idxTs"],
                            price["ts_event_ms"],
                            price["ts_ingest_ms"],
                        )
                        for price in batch
                    ],
                )
        except Exception as e:
            log.error(f"PostgreSQL insert error: {str(e)}")
            raise Exception(f"PostgreSQL error writing mark_prices: {str(e)}")

    async def write_tickers(self, batch: Sequence[Dict[str, Any]]) -> None:
        if not batch:
            return
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(f'SET search_path TO "{self.schema}"')
                await conn.executemany(
                    f"""
                    INSERT INTO "{self.schema}".tickers 
                    (instid, last, lastsz, bidpx, bidsz, askpx, asksz, 
                     open24h, high24h, low24h, vol24h, volccy24h, ts_event_ms, ts_ingest_ms)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                    ON CONFLICT (instid, ts_event_ms) DO NOTHING
                    """,
                    [
                        (
                            ticker["instId"],
                            ticker["last"],
                            ticker["lastSz"],
                            ticker["bidPx"],
                            ticker["bidSz"],
                            ticker["askPx"],
                            ticker["askSz"],
                            ticker["open24h"],
                            ticker["high24h"],
                            ticker["low24h"],
                            ticker["vol24h"],
                            ticker["volCcy24h"],
                            ticker["ts_event_ms"],
                            ticker["ts_ingest_ms"],
                        )
                        for ticker in batch
                    ],
                )
        except Exception as e:
            log.error(f"PostgreSQL insert error: {str(e)}")
            raise Exception(f"PostgreSQL error writing tickers: {str(e)}")

    async def write_open_interest(self, batch: Sequence[Dict[str, Any]]) -> None:
        if not batch:
            return
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(f'SET search_path TO "{self.schema}"')
                await conn.executemany(
                    f"""
                    INSERT INTO "{self.schema}".open_interest 
                    (instid, oi, oiccy, ts_event_ms, ts_ingest_ms)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (instid, ts_event_ms) DO NOTHING
                    """,
                    [
                        (
                            oi["instId"],
                            oi["oi"],
                            oi["oiCcy"],
                            oi["ts_event_ms"],
                            oi["ts_ingest_ms"],
                        )
                        for oi in batch
                    ],
                )
        except Exception as e:
            log.error(f"PostgreSQL insert error: {str(e)}")
            raise Exception(f"PostgreSQL error writing open_interest: {str(e)}")

    async def write_orderbook_snapshots(self, batch: Sequence[Dict[str, Any]]) -> None:
        """
        Write orderbook snapshots to PostgreSQL.
        Format: {snapshot_id (UUID), instId, ts_event_ms, side (1=bid, 2=ask), 
        price (Float64), size (Float64), level (UInt16)}
        """
        from okx_hft.utils.logging import get_logger
        log = get_logger(__name__)
        
        if not batch:
            return
        
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(f'SET search_path TO "{self.schema}"')
                await conn.executemany(
                    f"""
                    INSERT INTO "{self.schema}".orderbook_snapshots 
                    (snapshot_id, instid, ts_event_ms, side, price, size, level)
                    VALUES ($1::uuid, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (instid, ts_event_ms, snapshot_id, side, price) DO NOTHING
                    """,
                    [
                        (
                            row["snapshot_id"],
                            row["instId"],
                            row["ts_event_ms"],
                            row["side"],
                            row["price"],
                            row["size"],
                            row["level"],
                        )
                        for row in batch
                    ],
                )
        except Exception as e:
            log.error(
                f"PostgreSQL insert error for orderbook_snapshots: {str(e)}"
            )
            raise Exception(
                f"PostgreSQL error writing orderbook_snapshots: {str(e)}"
            )

    async def write_index_tickers(self, batch: Sequence[Dict[str, Any]]) -> None:
        if not batch:
            return
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(f'SET search_path TO "{self.schema}"')
                await conn.executemany(
                    f"""
                    INSERT INTO "{self.schema}".index_tickers 
                    (instid, idxpx, open24h, high24h, low24h, sodutc0, sodutc8, ts_event_ms, ts_ingest_ms)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (instid, ts_event_ms) DO NOTHING
                    """,
                    [
                        (
                            ticker["instId"],
                            ticker["idxPx"],
                            ticker["open24h"],
                            ticker["high24h"],
                            ticker["low24h"],
                            ticker["sodUtc0"],
                            ticker["sodUtc8"],
                            ticker["ts_event_ms"],
                            ticker["ts_ingest_ms"],
                        )
                        for ticker in batch
                    ],
                )
        except Exception as e:
            log.error(f"PostgreSQL insert error: {str(e)}")
            raise Exception(f"PostgreSQL error writing index_tickers: {str(e)}")

    async def write_orderbook_updates(self, batch: Sequence[Dict[str, Any]]) -> None:
        """Write orderbook updates using JSONB for nested types"""
        if not batch:
            return
        try:
            import orjson
            
            async with self.pool.acquire() as conn:
                await conn.execute(f'SET search_path TO "{self.schema}"')
                await conn.executemany(
                    f"""
                    INSERT INTO "{self.schema}".orderbook_updates 
                    (instid, ts_event_ms, bids_delta, asks_delta, checksum)
                    VALUES ($1, $2, $3::jsonb, $4::jsonb, $5)
                    ON CONFLICT (instid, ts_event_ms) DO NOTHING
                    """,
                    [
                        (
                            update["instId"],
                            update["ts_event_ms"],
                            orjson.dumps(update.get("bids_delta", [])).decode('utf-8'),
                            orjson.dumps(update.get("asks_delta", [])).decode('utf-8'),
                            update.get("checksum", 0),
                        )
                        for update in batch
                    ],
                )
        except Exception as e:
            log.error(f"PostgreSQL insert error for orderbook_updates: {str(e)}")
            raise Exception(f"PostgreSQL error writing orderbook_updates: {str(e)}")

    async def flush(self) -> None:
        """No-op for PostgreSQL (data is written immediately)"""
        pass

    async def close(self) -> None:
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
            log.info("PostgreSQL connection pool closed")

