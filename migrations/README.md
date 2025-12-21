# Миграции базы данных

## 001: Добавление ts_ingest_ms в orderbook_updates

**Дата:** 2024-12-21  
**Файл:** `001_add_ts_ingest_ms_to_orderbook_updates.sql`

### Зачем нужно?

Колонка `ts_ingest_ms` необходима для:
- Инкрементальной репликации `okx_raw → okx_core` по watermark
- Отслеживания задержки между событием на бирже и записью в БД
- Дедупликации и upsert операций

### Как применить миграцию

**Шаг 1: Добавить колонку (nullable)**
```bash
psql -h 167.86.110.201 -U postgres -d okx_hft -c "
ALTER TABLE okx_raw.orderbook_updates 
ADD COLUMN IF NOT EXISTS ts_ingest_ms BIGINT;
"
```

**Шаг 2: Задеплоить новый код collector**
```bash
cd /opt/hft/okx-hft-collector/docker
git pull
docker-compose down && docker-compose up -d --build
```
После этого новые записи будут писаться с `ts_ingest_ms`.

**Шаг 3: Backfill исторических данных**
```bash
psql -h 167.86.110.201 -U postgres -d okx_hft -c "
UPDATE okx_raw.orderbook_updates 
SET ts_ingest_ms = ts_event_ms 
WHERE ts_ingest_ms IS NULL;
"
```

**Шаг 4: Установить NOT NULL**
```bash
psql -h 167.86.110.201 -U postgres -d okx_hft -c "
ALTER TABLE okx_raw.orderbook_updates 
ALTER COLUMN ts_ingest_ms SET NOT NULL;
"
```

**Шаг 5: Создать индексы (вне транзакции!)**
```bash
psql -h 167.86.110.201 -U postgres -d okx_hft -c "
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orderbook_updates_ts_ingest_ms 
ON okx_raw.orderbook_updates(ts_ingest_ms);
"

psql -h 167.86.110.201 -U postgres -d okx_hft -c "
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orderbook_updates_instid_ts_ingest 
ON okx_raw.orderbook_updates(instid, ts_ingest_ms);
"
```

### Проверка

**1. Нет NULL значений:**
```sql
SELECT COUNT(*) as null_count 
FROM okx_raw.orderbook_updates 
WHERE ts_ingest_ms IS NULL;
-- Ожидается: 0
```

**2. Распределение задержки (ts_ingest_ms - ts_event_ms):**
```sql
SELECT 
    COUNT(*) as total_rows,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY ts_ingest_ms - ts_event_ms) as p50_ms,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY ts_ingest_ms - ts_event_ms) as p95_ms,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY ts_ingest_ms - ts_event_ms) as p99_ms,
    MIN(ts_ingest_ms - ts_event_ms) as min_ms,
    MAX(ts_ingest_ms - ts_event_ms) as max_ms
FROM okx_raw.orderbook_updates;
```

**3. Индексы созданы:**
```sql
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'orderbook_updates' AND schemaname = 'okx_raw';
```

### Откат (если нужно)

```sql
DROP INDEX CONCURRENTLY IF EXISTS okx_raw.idx_orderbook_updates_ts_ingest_ms;
DROP INDEX CONCURRENTLY IF EXISTS okx_raw.idx_orderbook_updates_instid_ts_ingest;
ALTER TABLE okx_raw.orderbook_updates DROP COLUMN IF EXISTS ts_ingest_ms;
```

