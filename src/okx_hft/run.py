
import asyncio
from okx_hft.config.settings import Settings
from okx_hft.ws.client import OKXWebSocketClient
from okx_hft.metrics.server import run_metrics_server
from okx_hft.utils.logging import get_logger

log = get_logger(__name__)

async def main() -> None:
    settings = Settings()
    log.info(f"settings_loaded: {settings.model_dump()}")
    metrics_task = asyncio.create_task(run_metrics_server(port=settings.METRICS_PORT))
    client = OKXWebSocketClient(settings=settings)
    
    # Запускаем задачу для периодической отправки батчей
    flush_task = asyncio.create_task(client.periodic_flush())
    
    try:
        await client.run_forever()
    finally:
        flush_task.cancel()
        metrics_task.cancel()
        # Close PostgreSQL connection
        if client.storage:
            await client.storage.close()

if __name__ == "__main__":
    asyncio.run(main())
