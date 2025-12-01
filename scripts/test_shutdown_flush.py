#!/usr/bin/env python3
"""
Скрипт для проверки логики финального сброса при остановке.
Запускается локально без Docker и реальной БД.

Использование:
    python scripts/test_shutdown_flush.py

Проверяет:
1. flush_all_handlers() вызывает flush у всех обработчиков
2. periodic_flush выполняет финальный сброс при отмене
3. Данные в батчах сбрасываются при остановке
"""
import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

# Добавляем путь к src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.okx_hft.ws.client import OKXWebSocketClient


def create_mock_client():
    """Создаём клиент с мок-настройками."""
    mock_settings = MagicMock()
    mock_settings.BATCH_MAX_SIZE = 50
    mock_settings.FLUSH_INTERVAL_MS = 100
    mock_settings.SNAPSHOT_INTERVAL_SEC = 30.0
    mock_settings.ORDERBOOK_MAX_DEPTH = 50
    
    client = OKXWebSocketClient(settings=mock_settings)
    
    # Мок-хранилище
    mock_storage = AsyncMock()
    client.trades_handler.storage = mock_storage
    client.funding_rate_handler.storage = mock_storage
    client.mark_price_handler.storage = mock_storage
    client.tickers_handler.storage = mock_storage
    client.open_interest_handler.storage = mock_storage
    client.orderbook_handler.storage = mock_storage
    
    return client, mock_storage


async def test_flush_all_handlers():
    """Тест 1: flush_all_handlers() вызывает flush у всех обработчиков."""
    print("\n" + "="*60)
    print("ТЕСТ 1: flush_all_handlers() вызывает flush у всех обработчиков")
    print("="*60)
    
    client, mock_storage = create_mock_client()
    
    # Добавляем данные в батчи
    client.trades_handler.batch = [{"type": "trade", "id": 1}]
    client.funding_rate_handler.batch = [{"type": "funding", "id": 1}]
    client.mark_price_handler.batch = [{"type": "mark", "id": 1}]
    client.tickers_handler.batch = [{"type": "ticker", "id": 1}]
    client.open_interest_handler.batch = [{"type": "oi", "id": 1}]
    client.orderbook_handler.batch_snapshots = [{"type": "ob_snap", "id": 1}]
    
    print(f"До flush:")
    print(f"  trades_handler.batch: {len(client.trades_handler.batch)} элементов")
    print(f"  funding_rate_handler.batch: {len(client.funding_rate_handler.batch)} элементов")
    print(f"  mark_price_handler.batch: {len(client.mark_price_handler.batch)} элементов")
    print(f"  tickers_handler.batch: {len(client.tickers_handler.batch)} элементов")
    print(f"  open_interest_handler.batch: {len(client.open_interest_handler.batch)} элементов")
    print(f"  orderbook_handler.batch_snapshots: {len(client.orderbook_handler.batch_snapshots)} элементов")
    
    # Вызываем flush_all_handlers
    await client.flush_all_handlers()
    
    print(f"\nПосле flush_all_handlers():")
    print(f"  trades_handler.batch: {len(client.trades_handler.batch)} элементов")
    print(f"  funding_rate_handler.batch: {len(client.funding_rate_handler.batch)} элементов")
    print(f"  mark_price_handler.batch: {len(client.mark_price_handler.batch)} элементов")
    print(f"  tickers_handler.batch: {len(client.tickers_handler.batch)} элементов")
    print(f"  open_interest_handler.batch: {len(client.open_interest_handler.batch)} элементов")
    print(f"  orderbook_handler.batch_snapshots: {len(client.orderbook_handler.batch_snapshots)} элементов")
    
    # Проверяем вызовы storage
    print(f"\nВызовы хранилища:")
    print(f"  write_trades вызван: {mock_storage.write_trades.called}")
    print(f"  write_funding_rates вызван: {mock_storage.write_funding_rates.called}")
    print(f"  write_mark_prices вызван: {mock_storage.write_mark_prices.called}")
    print(f"  write_tickers вызван: {mock_storage.write_tickers.called}")
    print(f"  write_open_interest вызван: {mock_storage.write_open_interest.called}")
    print(f"  write_orderbook_snapshots вызван: {mock_storage.write_orderbook_snapshots.called}")
    
    # Проверка
    all_empty = (
        len(client.trades_handler.batch) == 0 and
        len(client.funding_rate_handler.batch) == 0 and
        len(client.mark_price_handler.batch) == 0 and
        len(client.tickers_handler.batch) == 0 and
        len(client.open_interest_handler.batch) == 0 and
        len(client.orderbook_handler.batch_snapshots) == 0
    )
    
    if all_empty:
        print("\n✅ ТЕСТ 1 ПРОЙДЕН: Все батчи очищены")
        return True
    else:
        print("\n❌ ТЕСТ 1 ПРОВАЛЕН: Не все батчи очищены")
        return False


async def test_periodic_flush_cancellation():
    """Тест 2: periodic_flush выполняет финальный сброс при отмене."""
    print("\n" + "="*60)
    print("ТЕСТ 2: periodic_flush выполняет финальный сброс при отмене")
    print("="*60)
    
    client, mock_storage = create_mock_client()
    
    # Добавляем данные в батч trades
    client.trades_handler.batch = [
        {
            "instId": "BTC-USDT-SWAP",
            "ts_event_ms": 1704067200000,
            "tradeId": "test-123",
            "px": 50000.0,
            "sz": 1.0,
            "side": "buy",
            "ts_ingest_ms": 1704067200100
        }
    ]
    
    print(f"До запуска periodic_flush:")
    print(f"  trades_handler.batch: {len(client.trades_handler.batch)} элементов")
    
    # Запускаем periodic_flush
    task = asyncio.create_task(client.periodic_flush())
    
    # Ждём немного и отменяем (симуляция Ctrl+C)
    await asyncio.sleep(0.1)
    print("\nОтменяем periodic_flush (симуляция Ctrl+C)...")
    task.cancel()
    
    # Ждём завершения
    try:
        await task
    except asyncio.CancelledError:
        pass
    
    print(f"\nПосле отмены periodic_flush:")
    print(f"  trades_handler.batch: {len(client.trades_handler.batch)} элементов")
    print(f"  write_trades вызван: {mock_storage.write_trades.called}")
    
    if len(client.trades_handler.batch) == 0 and mock_storage.write_trades.called:
        print("\n✅ ТЕСТ 2 ПРОЙДЕН: Финальный сброс выполнен при отмене")
        return True
    else:
        print("\n❌ ТЕСТ 2 ПРОВАЛЕН: Финальный сброс не выполнен")
        return False


async def test_full_shutdown_scenario():
    """Тест 3: Полный сценарий остановки как в run.py."""
    print("\n" + "="*60)
    print("ТЕСТ 3: Полный сценарий остановки (как в run.py)")
    print("="*60)
    
    client, mock_storage = create_mock_client()
    
    # Добавляем данные
    client.trades_handler.batch = [{"type": "trade", "id": 1}, {"type": "trade", "id": 2}]
    client.funding_rate_handler.batch = [{"type": "funding", "id": 1}]
    
    print(f"Начальное состояние:")
    print(f"  trades: {len(client.trades_handler.batch)} элементов")
    print(f"  funding: {len(client.funding_rate_handler.batch)} элементов")
    
    # Запускаем periodic_flush
    flush_task = asyncio.create_task(client.periodic_flush())
    
    # Симуляция работы
    await asyncio.sleep(0.05)
    
    # === Блок finally из run.py ===
    print("\n--- Симуляция finally блока из run.py ---")
    
    # 1. Отменяем и ждём flush_task
    flush_task.cancel()
    try:
        await flush_task
    except asyncio.CancelledError:
        pass
    
    print(f"После отмены flush_task:")
    print(f"  trades: {len(client.trades_handler.batch)} элементов")
    print(f"  funding: {len(client.funding_rate_handler.batch)} элементов")
    
    # 2. Страховочный сброс
    print("\nВыполняем страховочный сброс...")
    await client.flush_all_handlers()
    
    print(f"После страховочного сброса:")
    print(f"  trades: {len(client.trades_handler.batch)} элементов")
    print(f"  funding: {len(client.funding_rate_handler.batch)} элементов")
    
    # Проверка
    all_empty = (
        len(client.trades_handler.batch) == 0 and
        len(client.funding_rate_handler.batch) == 0
    )
    
    all_written = (
        mock_storage.write_trades.called and
        mock_storage.write_funding_rates.called
    )
    
    if all_empty and all_written:
        print("\n✅ ТЕСТ 3 ПРОЙДЕН: Все данные сброшены при остановке")
        return True
    else:
        print("\n❌ ТЕСТ 3 ПРОВАЛЕН")
        return False


async def test_idempotent_flush():
    """Тест 4: Повторный flush безопасен (идемпотентность)."""
    print("\n" + "="*60)
    print("ТЕСТ 4: Повторный flush безопасен (идемпотентность)")
    print("="*60)
    
    client, mock_storage = create_mock_client()
    
    # Добавляем данные
    client.trades_handler.batch = [{"type": "trade", "id": 1}]
    
    print(f"До flush: {len(client.trades_handler.batch)} элементов")
    
    # Первый flush
    await client.flush_all_handlers()
    first_call_count = mock_storage.write_trades.call_count
    print(f"После 1-го flush: {len(client.trades_handler.batch)} элементов, write_trades вызван {first_call_count} раз")
    
    # Второй flush (батч уже пуст)
    await client.flush_all_handlers()
    second_call_count = mock_storage.write_trades.call_count
    print(f"После 2-го flush: {len(client.trades_handler.batch)} элементов, write_trades вызван {second_call_count} раз")
    
    # Третий flush
    await client.flush_all_handlers()
    third_call_count = mock_storage.write_trades.call_count
    print(f"После 3-го flush: {len(client.trades_handler.batch)} элементов, write_trades вызван {third_call_count} раз")
    
    if first_call_count == 1 and second_call_count == 1 and third_call_count == 1:
        print("\n✅ ТЕСТ 4 ПРОЙДЕН: Повторные flush не вызывают лишних записей")
        return True
    else:
        print("\n❌ ТЕСТ 4 ПРОВАЛЕН: Повторные flush вызывают лишние записи")
        return False


async def main():
    print("="*60)
    print("ПРОВЕРКА ЛОГИКИ ФИНАЛЬНОГО СБРОСА ПРИ ОСТАНОВКЕ")
    print("="*60)
    
    results = []
    
    results.append(await test_flush_all_handlers())
    results.append(await test_periodic_flush_cancellation())
    results.append(await test_full_shutdown_scenario())
    results.append(await test_idempotent_flush())
    
    print("\n" + "="*60)
    print("ИТОГИ")
    print("="*60)
    
    passed = sum(results)
    total = len(results)
    
    print(f"Пройдено: {passed}/{total}")
    
    if passed == total:
        print("\n🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ!")
        return 0
    else:
        print(f"\n⚠️ Провалено тестов: {total - passed}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
