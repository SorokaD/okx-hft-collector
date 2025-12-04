"""Тесты для поведения финального сброса при остановке.

Проверяем, что последняя порция данных корректно сбрасывается
в хранилище при остановке коллектора (например, Ctrl+C).
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock
import pytest

from src.okx_hft.handlers.trades import TradesHandler
from src.okx_hft.handlers.funding_rate import FundingRateHandler
from src.okx_hft.handlers.orderbook import OrderBookHandler


class TestTradesHandlerFlush:
    """Тесты для поведения TradesHandler.flush()."""

    @pytest.mark.asyncio
    async def test_flush_empty_batch_does_nothing(self):
        """Проверяем, что flush() с пустым батчем ничего не делает."""
        mock_storage = AsyncMock()
        handler = TradesHandler(storage=mock_storage)
        
        # Батч пуст
        assert len(handler.batch) == 0
        
        await handler.flush()
        
        # Storage не должен быть вызван
        mock_storage.write_trades.assert_not_called()

    @pytest.mark.asyncio
    async def test_flush_with_data_writes_to_storage(self):
        """Проверяем, что flush() записывает батч в хранилище и очищает его."""
        mock_storage = AsyncMock()
        handler = TradesHandler(storage=mock_storage)
        
        # Добавляем трейды в батч
        trades = [
            {
                "instId": "BTC-USDT-SWAP",
                "ts_event_ms": 1704067200000,
                "tradeId": "123",
                "px": 50000.0,
                "sz": 1.0,
                "side": "buy",
                "ts_ingest_ms": 1704067200100
            },
            {
                "instId": "BTC-USDT-SWAP",
                "ts_event_ms": 1704067200100,
                "tradeId": "124",
                "px": 50001.0,
                "sz": 0.5,
                "side": "sell",
                "ts_ingest_ms": 1704067200200
            }
        ]
        handler.batch = trades.copy()
        
        await handler.flush()
        
        # Storage должен быть вызван с батчем
        mock_storage.write_trades.assert_called_once_with(trades)
        # Батч должен быть очищен
        assert len(handler.batch) == 0

    @pytest.mark.asyncio
    async def test_flush_no_storage_clears_batch(self):
        """Проверяем, что flush() без хранилища очищает батч."""
        handler = TradesHandler(storage=None)
        
        handler.batch = [{"test": "data"}]
        
        await handler.flush()
        
        # Батч должен быть очищен даже без хранилища
        assert len(handler.batch) == 0

    @pytest.mark.asyncio
    async def test_flush_is_idempotent(self):
        """Проверяем, что многократный вызов flush() безопасен."""
        mock_storage = AsyncMock()
        handler = TradesHandler(storage=mock_storage)
        
        handler.batch = [{"test": "data"}]
        
        # Первый flush
        await handler.flush()
        assert mock_storage.write_trades.call_count == 1
        
        # Второй flush ничего не должен делать (батч пуст)
        await handler.flush()
        assert mock_storage.write_trades.call_count == 1


class TestFundingRateHandlerFlush:
    """Тесты для FundingRateHandler.flush()."""

    @pytest.mark.asyncio
    async def test_flush_empty_batch_does_nothing(self):
        """Проверяем, что flush() с пустым батчем ничего не делает."""
        mock_storage = AsyncMock()
        handler = FundingRateHandler(storage=mock_storage)
        
        await handler.flush()
        
        mock_storage.write_funding_rates.assert_not_called()

    @pytest.mark.asyncio
    async def test_flush_with_data_writes_to_storage(self):
        """Проверяем, что flush() записывает батч в хранилище."""
        mock_storage = AsyncMock()
        handler = FundingRateHandler(storage=mock_storage)
        
        handler.batch = [{"instId": "BTC-USDT-SWAP", "fundingRate": 0.0001}]
        
        await handler.flush()
        
        mock_storage.write_funding_rates.assert_called_once()
        assert len(handler.batch) == 0


class TestOrderBookHandlerFlush:
    """Тесты для OrderBookHandler.flush()."""

    @pytest.mark.asyncio
    async def test_flush_clears_both_batches(self):
        """Проверяем, что flush() очищает оба батча (snapshots и updates)."""
        mock_storage = AsyncMock()
        handler = OrderBookHandler(storage=mock_storage)
        
        handler.batch_snapshots = [{"test": "snapshot"}]
        handler.batch_updates = [{"test": "update"}]
        
        await handler.flush()
        
        mock_storage.write_orderbook_snapshots.assert_called_once()
        mock_storage.write_orderbook_updates.assert_called_once()
        assert len(handler.batch_snapshots) == 0
        assert len(handler.batch_updates) == 0


class TestClientFlushAllHandlers:
    """Тесты для OKXWebSocketClient.flush_all_handlers()."""

    @pytest.mark.asyncio
    async def test_flush_all_handlers_calls_all_flushes(self):
        """Проверяем, что flush_all_handlers() вызывает flush у всех обработчиков."""
        from src.okx_hft.ws.client import OKXWebSocketClient
        
        # Создаём клиент с мок-настройками
        mock_settings = MagicMock()
        mock_settings.BATCH_MAX_SIZE = 50
        mock_settings.FLUSH_INTERVAL_MS = 100
        mock_settings.SNAPSHOT_INTERVAL_SEC = 30.0
        mock_settings.ORDERBOOK_MAX_DEPTH = 50
        
        client = OKXWebSocketClient(settings=mock_settings)
        
        # Мокаем методы flush всех обработчиков
        client.trades_handler.flush = AsyncMock()
        client.orderbook_handler.flush = AsyncMock()
        client.funding_rate_handler.flush = AsyncMock()
        client.mark_price_handler.flush = AsyncMock()
        client.tickers_handler.flush = AsyncMock()
        client.open_interest_handler.flush = AsyncMock()
        
        await client.flush_all_handlers()
        
        # Все обработчики должны получить вызов flush
        client.trades_handler.flush.assert_called_once()
        client.orderbook_handler.flush.assert_called_once()
        client.funding_rate_handler.flush.assert_called_once()
        client.mark_price_handler.flush.assert_called_once()
        client.tickers_handler.flush.assert_called_once()
        client.open_interest_handler.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_flush_all_handlers_continues_on_error(self):
        """Проверяем, что flush_all_handlers() продолжает работу при ошибке одного обработчика."""
        from src.okx_hft.ws.client import OKXWebSocketClient
        
        mock_settings = MagicMock()
        mock_settings.BATCH_MAX_SIZE = 50
        mock_settings.FLUSH_INTERVAL_MS = 100
        mock_settings.SNAPSHOT_INTERVAL_SEC = 30.0
        mock_settings.ORDERBOOK_MAX_DEPTH = 50
        
        client = OKXWebSocketClient(settings=mock_settings)
        
        # trades_handler выбрасывает исключение
        client.trades_handler.flush = AsyncMock(side_effect=Exception("DB error"))
        client.orderbook_handler.flush = AsyncMock()
        client.funding_rate_handler.flush = AsyncMock()
        client.mark_price_handler.flush = AsyncMock()
        client.tickers_handler.flush = AsyncMock()
        client.open_interest_handler.flush = AsyncMock()
        
        # Не должно выбросить исключение, должно продолжить к другим обработчикам
        await client.flush_all_handlers()
        
        # Остальные обработчики должны быть вызваны
        client.orderbook_handler.flush.assert_called_once()
        client.funding_rate_handler.flush.assert_called_once()


class TestPeriodicFlushCancellation:
    """Тесты для поведения periodic_flush при отмене."""

    @pytest.mark.asyncio
    async def test_periodic_flush_performs_final_flush_on_cancel(self):
        """Проверяем, что periodic_flush выполняет финальный сброс при отмене."""
        from src.okx_hft.ws.client import OKXWebSocketClient
        
        mock_settings = MagicMock()
        mock_settings.BATCH_MAX_SIZE = 50
        mock_settings.FLUSH_INTERVAL_MS = 100
        mock_settings.SNAPSHOT_INTERVAL_SEC = 30.0
        mock_settings.ORDERBOOK_MAX_DEPTH = 50
        
        client = OKXWebSocketClient(settings=mock_settings)
        
        # Счётчик вызовов flush
        flush_call_count = 0
        
        async def mock_flush():
            nonlocal flush_call_count
            flush_call_count += 1
        
        client.flush_all_handlers = mock_flush
        
        # Запускаем задачу periodic_flush
        task = asyncio.create_task(client.periodic_flush())
        
        # Даём немного поработать, затем отменяем
        await asyncio.sleep(0.1)
        task.cancel()
        
        # Ждём завершения задачи
        try:
            await task
        except asyncio.CancelledError:
            pass
        
        # flush должен быть вызван минимум один раз (финальный сброс при отмене)
        assert flush_call_count >= 1

    @pytest.mark.asyncio
    async def test_cancel_during_sleep_still_flushes(self):
        """Проверяем, что отмена во время sleep() всё равно выполняет финальный сброс."""
        from src.okx_hft.ws.client import OKXWebSocketClient
        
        mock_settings = MagicMock()
        mock_settings.BATCH_MAX_SIZE = 50
        mock_settings.FLUSH_INTERVAL_MS = 100
        mock_settings.SNAPSHOT_INTERVAL_SEC = 30.0
        mock_settings.ORDERBOOK_MAX_DEPTH = 50
        
        client = OKXWebSocketClient(settings=mock_settings)
        
        # Добавляем данные в батч обработчика
        client.trades_handler.batch = [
            {
                "instId": "BTC-USDT-SWAP",
                "ts_event_ms": 1704067200000,
                "tradeId": "999",
                "px": 50000.0,
                "sz": 1.0,
                "side": "buy",
                "ts_ingest_ms": 1704067200100
            }
        ]
        
        # Мок хранилища
        mock_storage = AsyncMock()
        client.trades_handler.storage = mock_storage
        
        # Запускаем задачу periodic_flush
        task = asyncio.create_task(client.periodic_flush())
        
        # Отменяем сразу (во время начального sleep)
        await asyncio.sleep(0.01)
        task.cancel()
        
        try:
            await task
        except asyncio.CancelledError:
            pass
        
        # Финальный сброс должен записать трейды
        mock_storage.write_trades.assert_called_once()


class TestShutdownScenario:
    """Интеграционные тесты для полного сценария остановки."""

    @pytest.mark.asyncio
    async def test_full_shutdown_flushes_all_data(self):
        """Симулируем полную остановку и проверяем сброс всех данных."""
        from src.okx_hft.ws.client import OKXWebSocketClient
        
        mock_settings = MagicMock()
        mock_settings.BATCH_MAX_SIZE = 50
        mock_settings.FLUSH_INTERVAL_MS = 100
        mock_settings.SNAPSHOT_INTERVAL_SEC = 30.0
        mock_settings.ORDERBOOK_MAX_DEPTH = 50
        
        client = OKXWebSocketClient(settings=mock_settings)
        
        # Настраиваем мок-хранилище для всех обработчиков
        mock_storage = AsyncMock()
        client.trades_handler.storage = mock_storage
        client.funding_rate_handler.storage = mock_storage
        client.mark_price_handler.storage = mock_storage
        client.tickers_handler.storage = mock_storage
        client.open_interest_handler.storage = mock_storage
        client.orderbook_handler.storage = mock_storage
        
        # Добавляем данные в каждый обработчик
        client.trades_handler.batch = [{"type": "trade", "id": 1}]
        client.funding_rate_handler.batch = [{"type": "funding", "id": 1}]
        client.mark_price_handler.batch = [{"type": "mark", "id": 1}]
        client.tickers_handler.batch = [{"type": "ticker", "id": 1}]
        client.open_interest_handler.batch = [{"type": "oi", "id": 1}]
        client.orderbook_handler.batch_snapshots = [{"type": "ob_snap", "id": 1}]
        
        # Запускаем periodic_flush
        flush_task = asyncio.create_task(client.periodic_flush())
        
        # Симулируем остановку
        await asyncio.sleep(0.01)
        flush_task.cancel()
        
        try:
            await flush_task
        except asyncio.CancelledError:
            pass
        
        # Страховочный сброс (как в run.py)
        await client.flush_all_handlers()
        
        # Проверяем, что все данные записаны
        mock_storage.write_trades.assert_called()
        mock_storage.write_funding_rates.assert_called()
        mock_storage.write_mark_prices.assert_called()
        mock_storage.write_tickers.assert_called()
        mock_storage.write_open_interest.assert_called()
        mock_storage.write_orderbook_snapshots.assert_called()
        
        # Все батчи должны быть пусты
        assert len(client.trades_handler.batch) == 0
        assert len(client.funding_rate_handler.batch) == 0
        assert len(client.mark_price_handler.batch) == 0
        assert len(client.tickers_handler.batch) == 0
        assert len(client.open_interest_handler.batch) == 0
        assert len(client.orderbook_handler.batch_snapshots) == 0



