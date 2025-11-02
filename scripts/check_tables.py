#!/usr/bin/env python3
"""Скрипт для проверки наличия таблиц в ClickHouse"""

import clickhouse_connect
from okx_hft.config.settings import Settings


def check_tables():
    """Проверяет наличие всех необходимых таблиц в ClickHouse"""
    settings = Settings()
    
    print(f"Подключение к ClickHouse: {settings.CLICKHOUSE_DSN}")
    print(f"База данных: {settings.CLICKHOUSE_DB}")
    print()
    
    try:
        from urllib.parse import urlparse
        parsed = urlparse(settings.CLICKHOUSE_DSN)
        host = parsed.hostname or "localhost"
        port = parsed.port or 8123
        
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD,
            database=settings.CLICKHOUSE_DB,
        )
        
        print("✓ Подключение успешно")
        print()
        
        # Список ожидаемых таблиц
        expected_tables = [
            "trades",
            "funding_rates",
            "mark_prices",
            "tickers",
            "open_interest",
            "orderbook_snapshots",
            "orderbook_updates",
        ]
        
        # Проверяем наличие таблиц
        query = f"SHOW TABLES FROM {settings.CLICKHOUSE_DB}"
        result = client.query(query)
        existing_tables = [row[0] for row in result.result_rows]
        
        print("Проверка таблиц:")
        print("-" * 60)
        
        all_exist = True
        for table in expected_tables:
            exists = table in existing_tables
            status = "✓" if exists else "✗"
            print(f"{status} {settings.CLICKHOUSE_DB}.{table}")
            
            if not exists:
                all_exist = False
                print(f"  ⚠ Таблица не найдена!")
        
        print("-" * 60)
        
        if all_exist:
            print("\n✓ Все таблицы существуют!")
        else:
            print("\n✗ Некоторые таблицы отсутствуют!")
            print("\nПопробуйте перезапустить коллектор для создания таблиц.")
        
        # Показываем количество записей в таблицах orderbook
        print("\n" + "=" * 60)
        print("Статистика по таблицам orderbook:")
        print("-" * 60)
        
        for table in ["orderbook_snapshots", "orderbook_updates"]:
            try:
                count_query = f"SELECT count() FROM {settings.CLICKHOUSE_DB}.{table}"
                result = client.query(count_query)
                count = result.result_rows[0][0]
                print(f"{table}: {count:,} записей")
            except Exception as e:
                print(f"{table}: Ошибка при проверке - {str(e)}")
        
        print("=" * 60)
        
    except Exception as e:
        print(f"✗ Ошибка подключения: {str(e)}")
        return False
    
    return all_exist


if __name__ == "__main__":
    check_tables()

