
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List


class Settings(BaseSettings):
    INSTRUMENTS: List[str] = Field(
        default_factory=lambda: [
 
            "BTC-USDT-SWAP",
            "ETH-USDT-SWAP",
        ]
    )
    CHANNELS: List[str] = Field(
        default_factory=lambda: [
            "trades",
            "funding-rate",
            "mark-price",
            "tickers",
            "open-interest",
            "books",
        ]
    )
    OKX_WS_URL: str = "wss://ws.okx.com:8443/ws/v5/public"

    CLICKHOUSE_DSN: str = "http://localhost:8123"
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = ""
    CLICKHOUSE_DB: str = "market_raw"
    LOCAL_WAL_DIR: str = "./wal"
    PARQUET_DIR: str = "./parquet"

    BATCH_MAX_SIZE: int = 5000
    FLUSH_INTERVAL_MS: int = 150

    # Orderbook snapshot settings
    SNAPSHOT_INTERVAL_SEC: float = 30.0  # Default interval for periodic snapshots
    ORDERBOOK_MAX_DEPTH: int = 50  # Max depth levels to store

    BACKOFF_BASE: float = 0.5
    BACKOFF_CAP: float = 30.0

    METRICS_PORT: int = 9108
    LOG_LEVEL: str = "INFO"

    model_config = {"env_file": ".env", "case_sensitive": False}
