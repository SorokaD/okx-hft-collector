
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
        log.info("Остановка коллектора...")
        
        # Отменяем и ждём завершения задачи периодического сброса
        # Задача выполнит финальный сброс при отмене
        flush_task.cancel()
        try:
            await flush_task
        except asyncio.CancelledError:
            pass
        
        # Страховочный сброс: гарантируем сохранение всех оставшихся данных
        # Обрабатывает случай, когда flush_task был отменён во время sleep()
        # или если данные пришли после последнего сброса
        log.info("Выполняем страховочный сброс всех обработчиков...")
        try:
            await client.flush_all_handlers()
            log.info("Страховочный сброс завершён")
        except Exception as e:
            log.error(f"Ошибка при страховочном сбросе: {e}")
        
        # Отменяем сервер метрик
        metrics_task.cancel()
        try:
            await metrics_task
        except asyncio.CancelledError:
            pass
        
        # Закрываем соединение с PostgreSQL
        if client.storage:
            log.info("Закрытие соединения с PostgreSQL...")
            await client.storage.close()
            log.info("Соединение с PostgreSQL закрыто")


if __name__ == "__main__":
    asyncio.run(main())
