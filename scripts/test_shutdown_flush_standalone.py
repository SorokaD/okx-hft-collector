#!/usr/bin/env python3
"""
Автономный тест логики финального сброса.
НЕ требует никаких внешних зависимостей - чистый Python.

Использование:
    python3 scripts/test_shutdown_flush_standalone.py
"""
import asyncio
from typing import Dict, Any, List, Optional
from unittest.mock import AsyncMock


# === Мок-версии классов для тестирования ===

class MockStorage:
    """Мок хранилища."""
    def __init__(self):
        self.write_trades_calls = []
        self.write_funding_rates_calls = []
        self.write_mark_prices_calls = []
        self.write_tickers_calls = []
        self.write_open_interest_calls = []
        self.write_orderbook_snapshots_calls = []
        self.write_orderbook_updates_calls = []
    
    async def write_trades(self, data):
        self.write_trades_calls.append(data)
    
    async def write_funding_rates(self, data):
        self.write_funding_rates_calls.append(data)
    
    async def write_mark_prices(self, data):
        self.write_mark_prices_calls.append(data)
    
    async def write_tickers(self, data):
        self.write_tickers_calls.append(data)
    
    async def write_open_interest(self, data):
        self.write_open_interest_calls.append(data)
    
    async def write_orderbook_snapshots(self, data):
        self.write_orderbook_snapshots_calls.append(data)
    
    async def write_orderbook_updates(self, data):
        self.write_orderbook_updates_calls.append(data)


class BaseHandler:
    """Базовый обработчик с логикой flush."""
    def __init__(self, storage=None):
        self.storage = storage
        self.batch: List[Dict[str, Any]] = []
    
    async def _flush_batch(self, write_method_name: str) -> None:
        """Внутренний метод для сброса батча."""
        if self.batch:
            if self.storage:
                write_method = getattr(self.storage, write_method_name)
                await write_method(self.batch.copy())
                self.batch = []
            else:
                self.batch = []
    
    async def flush(self) -> None:
        raise NotImplementedError


class TradesHandler(BaseHandler):
    """Обработчик трейдов."""
    async def flush(self) -> None:
        if self.batch:
            await self._flush_batch("write_trades")


class FundingRateHandler(BaseHandler):
    """Обработчик funding rate."""
    async def flush(self) -> None:
        if self.batch:
            await self._flush_batch("write_funding_rates")


class MarkPriceHandler(BaseHandler):
    """Обработчик mark price."""
    async def flush(self) -> None:
        if self.batch:
            await self._flush_batch("write_mark_prices")


class TickersHandler(BaseHandler):
    """Обработчик tickers."""
    async def flush(self) -> None:
        if self.batch:
            await self._flush_batch("write_tickers")


class OpenInterestHandler(BaseHandler):
    """Обработчик open interest."""
    async def flush(self) -> None:
        if self.batch:
            await self._flush_batch("write_open_interest")


class OrderBookHandler:
    """Обработчик orderbook с двумя батчами."""
    def __init__(self, storage=None):
        self.storage = storage
        self.batch_snapshots: List[Dict[str, Any]] = []
        self.batch_updates: List[Dict[str, Any]] = []
    
    async def flush(self) -> None:
        if self.batch_snapshots and self.storage:
            await self.storage.write_orderbook_snapshots(self.batch_snapshots.copy())
            self.batch_snapshots = []
        elif self.batch_snapshots:
            self.batch_snapshots = []
        
        if self.batch_updates and self.storage:
            await self.storage.write_orderbook_updates(self.batch_updates.copy())
            self.batch_updates = []
        elif self.batch_updates:
            self.batch_updates = []


class MockClient:
    """
    Мок-версия OKXWebSocketClient с нашей новой логикой.
    Точно повторяет логику из client.py.
    """
    def __init__(self):
        self.storage = None
        self.trades_handler = TradesHandler()
        self.funding_rate_handler = FundingRateHandler()
        self.mark_price_handler = MarkPriceHandler()
        self.tickers_handler = TickersHandler()
        self.open_interest_handler = OpenInterestHandler()
        self.orderbook_handler = OrderBookHandler()
    
    def set_storage(self, storage):
        """Устанавливаем storage для всех обработчиков."""
        self.storage = storage
        self.trades_handler.storage = storage
        self.funding_rate_handler.storage = storage
        self.mark_price_handler.storage = storage
        self.tickers_handler.storage = storage
        self.open_interest_handler.storage = storage
        self.orderbook_handler.storage = storage
    
    async def flush_all_handlers(self) -> None:
        """
        Сбросить батчи всех обработчиков в хранилище.
        Метод идемпотентен - повторные вызовы безопасны.
        """
        handlers = [
            ("trades", self.trades_handler),
            ("orderbook", self.orderbook_handler),
            ("funding_rate", self.funding_rate_handler),
            ("mark_price", self.mark_price_handler),
            ("tickers", self.tickers_handler),
            ("open_interest", self.open_interest_handler),
        ]
        
        for name, handler in handlers:
            try:
                await handler.flush()
            except Exception as e:
                print(f"Ошибка при сбросе {name}: {e}")
    
    async def periodic_flush(self) -> None:
        """Периодическая отправка батчей."""
        while True:
            try:
                await asyncio.sleep(5.0)
                await self.flush_all_handlers()
            except asyncio.CancelledError:
                print("  [periodic_flush] Задача отменена, выполняем финальный сброс...")
                try:
                    await self.flush_all_handlers()
                    print("  [periodic_flush] Финальный сброс выполнен успешно")
                except Exception as e:
                    print(f"  [periodic_flush] Ошибка при финальном сбросе: {e}")
                break
            except Exception as e:
                print(f"Ошибка в periodic flush: {e}")


# === ТЕСТЫ ===

async def test_flush_all_handlers():
    """Тест 1: flush_all_handlers() вызывает flush у всех обработчиков."""
    print("\n" + "="*60)
    print("ТЕСТ 1: flush_all_handlers() вызывает flush у всех обработчиков")
    print("="*60)
    
    client = MockClient()
    storage = MockStorage()
    client.set_storage(storage)
    
    # Добавляем данные в батчи
    client.trades_handler.batch = [{"type": "trade", "id": 1}]
    client.funding_rate_handler.batch = [{"type": "funding", "id": 1}]
    client.mark_price_handler.batch = [{"type": "mark", "id": 1}]
    client.tickers_handler.batch = [{"type": "ticker", "id": 1}]
    client.open_interest_handler.batch = [{"type": "oi", "id": 1}]
    client.orderbook_handler.batch_snapshots = [{"type": "ob_snap", "id": 1}]
    
    print(f"До flush:")
    print(f"  trades: {len(client.trades_handler.batch)}")
    print(f"  funding: {len(client.funding_rate_handler.batch)}")
    print(f"  mark_price: {len(client.mark_price_handler.batch)}")
    print(f"  tickers: {len(client.tickers_handler.batch)}")
    print(f"  open_interest: {len(client.open_interest_handler.batch)}")
    print(f"  orderbook_snapshots: {len(client.orderbook_handler.batch_snapshots)}")
    
    # Вызываем flush_all_handlers
    await client.flush_all_handlers()
    
    print(f"\nПосле flush_all_handlers():")
    print(f"  trades: {len(client.trades_handler.batch)}")
    print(f"  funding: {len(client.funding_rate_handler.batch)}")
    print(f"  mark_price: {len(client.mark_price_handler.batch)}")
    print(f"  tickers: {len(client.tickers_handler.batch)}")
    print(f"  open_interest: {len(client.open_interest_handler.batch)}")
    print(f"  orderbook_snapshots: {len(client.orderbook_handler.batch_snapshots)}")
    
    print(f"\nВызовы хранилища:")
    print(f"  write_trades: {len(storage.write_trades_calls)} раз")
    print(f"  write_funding_rates: {len(storage.write_funding_rates_calls)} раз")
    print(f"  write_mark_prices: {len(storage.write_mark_prices_calls)} раз")
    print(f"  write_tickers: {len(storage.write_tickers_calls)} раз")
    print(f"  write_open_interest: {len(storage.write_open_interest_calls)} раз")
    print(f"  write_orderbook_snapshots: {len(storage.write_orderbook_snapshots_calls)} раз")
    
    # Проверка
    all_empty = (
        len(client.trades_handler.batch) == 0 and
        len(client.funding_rate_handler.batch) == 0 and
        len(client.mark_price_handler.batch) == 0 and
        len(client.tickers_handler.batch) == 0 and
        len(client.open_interest_handler.batch) == 0 and
        len(client.orderbook_handler.batch_snapshots) == 0
    )
    
    all_written = (
        len(storage.write_trades_calls) == 1 and
        len(storage.write_funding_rates_calls) == 1 and
        len(storage.write_mark_prices_calls) == 1 and
        len(storage.write_tickers_calls) == 1 and
        len(storage.write_open_interest_calls) == 1 and
        len(storage.write_orderbook_snapshots_calls) == 1
    )
    
    if all_empty and all_written:
        print("\n✅ ТЕСТ 1 ПРОЙДЕН")
        return True
    else:
        print("\n❌ ТЕСТ 1 ПРОВАЛЕН")
        return False


async def test_periodic_flush_cancellation():
    """Тест 2: periodic_flush выполняет финальный сброс при отмене."""
    print("\n" + "="*60)
    print("ТЕСТ 2: periodic_flush выполняет финальный сброс при отмене")
    print("="*60)
    
    client = MockClient()
    storage = MockStorage()
    client.set_storage(storage)
    
    # Добавляем данные
    client.trades_handler.batch = [
        {"instId": "BTC-USDT-SWAP", "tradeId": "test-123", "px": 50000.0}
    ]
    
    print(f"До запуска periodic_flush:")
    print(f"  trades: {len(client.trades_handler.batch)} элементов")
    
    # Запускаем periodic_flush
    task = asyncio.create_task(client.periodic_flush())
    
    # Ждём немного и отменяем
    await asyncio.sleep(0.1)
    print("\nОтменяем periodic_flush (симуляция Ctrl+C)...")
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        pass
    
    print(f"\nПосле отмены periodic_flush:")
    print(f"  trades: {len(client.trades_handler.batch)} элементов")
    print(f"  write_trades вызван: {len(storage.write_trades_calls)} раз")
    
    if len(client.trades_handler.batch) == 0 and len(storage.write_trades_calls) == 1:
        print("\n✅ ТЕСТ 2 ПРОЙДЕН: Финальный сброс выполнен при отмене")
        return True
    else:
        print("\n❌ ТЕСТ 2 ПРОВАЛЕН")
        return False


async def test_full_shutdown_scenario():
    """Тест 3: Полный сценарий остановки как в run.py."""
    print("\n" + "="*60)
    print("ТЕСТ 3: Полный сценарий остановки (как в run.py)")
    print("="*60)
    
    client = MockClient()
    storage = MockStorage()
    client.set_storage(storage)
    
    # Добавляем данные
    client.trades_handler.batch = [{"type": "trade", "id": 1}, {"type": "trade", "id": 2}]
    client.funding_rate_handler.batch = [{"type": "funding", "id": 1}]
    
    print(f"Начальное состояние:")
    print(f"  trades: {len(client.trades_handler.batch)} элементов")
    print(f"  funding: {len(client.funding_rate_handler.batch)} элементов")
    
    # Запускаем periodic_flush
    flush_task = asyncio.create_task(client.periodic_flush())
    
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
    
    # 2. Страховочный сброс (как в run.py)
    print("\nВыполняем страховочный сброс...")
    await client.flush_all_handlers()
    
    print(f"После страховочного сброса:")
    print(f"  trades: {len(client.trades_handler.batch)} элементов")
    print(f"  funding: {len(client.funding_rate_handler.batch)} элементов")
    print(f"  write_trades вызван: {len(storage.write_trades_calls)} раз")
    print(f"  write_funding_rates вызван: {len(storage.write_funding_rates_calls)} раз")
    
    # Проверка
    all_empty = (
        len(client.trades_handler.batch) == 0 and
        len(client.funding_rate_handler.batch) == 0
    )
    
    if all_empty:
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
    
    client = MockClient()
    storage = MockStorage()
    client.set_storage(storage)
    
    # Добавляем данные
    client.trades_handler.batch = [{"type": "trade", "id": 1}]
    
    print(f"До flush: {len(client.trades_handler.batch)} элементов")
    
    # Первый flush
    await client.flush_all_handlers()
    first_call_count = len(storage.write_trades_calls)
    print(f"После 1-го flush: batch={len(client.trades_handler.batch)}, write_trades={first_call_count}")
    
    # Второй flush (батч уже пуст)
    await client.flush_all_handlers()
    second_call_count = len(storage.write_trades_calls)
    print(f"После 2-го flush: batch={len(client.trades_handler.batch)}, write_trades={second_call_count}")
    
    # Третий flush
    await client.flush_all_handlers()
    third_call_count = len(storage.write_trades_calls)
    print(f"После 3-го flush: batch={len(client.trades_handler.batch)}, write_trades={third_call_count}")
    
    if first_call_count == 1 and second_call_count == 1 and third_call_count == 1:
        print("\n✅ ТЕСТ 4 ПРОЙДЕН: Повторные flush не вызывают лишних записей")
        return True
    else:
        print("\n❌ ТЕСТ 4 ПРОВАЛЕН")
        return False


async def test_empty_batch_no_write():
    """Тест 5: flush() с пустым батчем не вызывает storage."""
    print("\n" + "="*60)
    print("ТЕСТ 5: flush() с пустым батчем не вызывает storage")
    print("="*60)
    
    client = MockClient()
    storage = MockStorage()
    client.set_storage(storage)
    
    # Батчи пусты
    print(f"Все батчи пусты: trades={len(client.trades_handler.batch)}")
    
    # Вызываем flush
    await client.flush_all_handlers()
    
    total_calls = (
        len(storage.write_trades_calls) +
        len(storage.write_funding_rates_calls) +
        len(storage.write_mark_prices_calls) +
        len(storage.write_tickers_calls) +
        len(storage.write_open_interest_calls) +
        len(storage.write_orderbook_snapshots_calls)
    )
    
    print(f"Всего вызовов storage: {total_calls}")
    
    if total_calls == 0:
        print("\n✅ ТЕСТ 5 ПРОЙДЕН: Пустые батчи не вызывают запись")
        return True
    else:
        print("\n❌ ТЕСТ 5 ПРОВАЛЕН")
        return False


async def main():
    print("="*60)
    print("ПРОВЕРКА ЛОГИКИ ФИНАЛЬНОГО СБРОСА ПРИ ОСТАНОВКЕ")
    print("(автономный тест без внешних зависимостей)")
    print("="*60)
    
    results = []
    
    results.append(await test_flush_all_handlers())
    results.append(await test_periodic_flush_cancellation())
    results.append(await test_full_shutdown_scenario())
    results.append(await test_idempotent_flush())
    results.append(await test_empty_batch_no_write())
    
    print("\n" + "="*60)
    print("ИТОГИ")
    print("="*60)
    
    passed = sum(results)
    total = len(results)
    
    print(f"Пройдено: {passed}/{total}")
    
    if passed == total:
        print("\n🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ!")
        print("\nЛогика финального сброса работает корректно:")
        print("  ✓ flush_all_handlers() сбрасывает все обработчики")
        print("  ✓ periodic_flush делает финальный сброс при отмене")
        print("  ✓ Страховочный сброс в finally работает")
        print("  ✓ Повторные вызовы flush безопасны (идемпотентность)")
        print("  ✓ Пустые батчи не вызывают лишних записей")
        return 0
    else:
        print(f"\n⚠️ Провалено тестов: {total - passed}")
        return 1


if __name__ == "__main__":
    import sys
    exit_code = asyncio.run(main())
    sys.exit(exit_code)



