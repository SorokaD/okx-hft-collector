-- Проверка структуры таблицы orderbook_snapshots
DESCRIBE TABLE okx_raw.orderbook_snapshots;

-- Проверка количества строк
SELECT COUNT(*) as total_rows FROM okx_raw.orderbook_snapshots;

-- Проверка последних записей (если есть)
SELECT * FROM okx_raw.orderbook_snapshots 
ORDER BY ts_event_ms DESC 
LIMIT 10;

-- Проверка структуры таблицы подробно
SHOW CREATE TABLE okx_raw.orderbook_snapshots;

