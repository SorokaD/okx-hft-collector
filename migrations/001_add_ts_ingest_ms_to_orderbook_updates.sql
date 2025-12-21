-- Migration: Add ts_ingest_ms to okx_raw.orderbook_updates
-- Date: 2024-12-21
-- Purpose: Enable incremental replication from okx_raw → okx_core using watermark on ts_ingest_ms
--
-- IMPORTANT: This migration is designed to be safe for:
--   - Large tables (batched backfill)
--   - TimescaleDB hypertables
--   - Zero-downtime deployments
--
-- Run order:
--   1. First run Step 1 (add column as nullable)
--   2. Deploy new collector code (starts writing ts_ingest_ms)
--   3. Run Step 2 (backfill historical data)
--   4. Run Step 3 (set NOT NULL)
--   5. Run Step 4 (create indexes - run OUTSIDE transaction!)

-- ============================================================================
-- STEP 1: Add column as NULLABLE (instant, no lock)
-- ============================================================================
-- This is safe and instant for any table size
ALTER TABLE okx_raw.orderbook_updates 
ADD COLUMN IF NOT EXISTS ts_ingest_ms BIGINT;

-- Verify column was added
-- SELECT column_name, data_type, is_nullable 
-- FROM information_schema.columns 
-- WHERE table_schema = 'okx_raw' AND table_name = 'orderbook_updates';

-- ============================================================================
-- STEP 2: Backfill existing rows (batched, safe for large tables)
-- ============================================================================
-- For historical rows, use ts_event_ms as best available proxy
-- This runs in batches to avoid long locks

-- Option A: Simple backfill (for smaller tables < 10M rows)
UPDATE okx_raw.orderbook_updates 
SET ts_ingest_ms = ts_event_ms 
WHERE ts_ingest_ms IS NULL;

-- Option B: Batched backfill (for larger tables)
-- Run this in a loop until affected rows = 0:
/*
DO $$
DECLARE
    batch_size INT := 100000;
    affected INT := 1;
BEGIN
    WHILE affected > 0 LOOP
        WITH batch AS (
            SELECT ctid 
            FROM okx_raw.orderbook_updates 
            WHERE ts_ingest_ms IS NULL 
            LIMIT batch_size
            FOR UPDATE SKIP LOCKED
        )
        UPDATE okx_raw.orderbook_updates t
        SET ts_ingest_ms = t.ts_event_ms
        FROM batch b
        WHERE t.ctid = b.ctid;
        
        GET DIAGNOSTICS affected = ROW_COUNT;
        RAISE NOTICE 'Updated % rows', affected;
        
        -- Small pause to reduce load
        PERFORM pg_sleep(0.1);
    END LOOP;
END $$;
*/

-- ============================================================================
-- STEP 3: Set NOT NULL constraint (after backfill complete)
-- ============================================================================
-- Only run this AFTER verifying all rows have ts_ingest_ms filled
-- Verify first:
-- SELECT COUNT(*) FROM okx_raw.orderbook_updates WHERE ts_ingest_ms IS NULL;
-- Should return 0

ALTER TABLE okx_raw.orderbook_updates 
ALTER COLUMN ts_ingest_ms SET NOT NULL;

-- ============================================================================
-- STEP 4: Create indexes for incremental sync (run OUTSIDE transaction!)
-- ============================================================================
-- CONCURRENTLY requires no transaction block, so run these separately

-- Index for watermark-based incremental sync
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orderbook_updates_ts_ingest_ms 
ON okx_raw.orderbook_updates(ts_ingest_ms);

-- Composite index for dedup/upsert operations (optional, for okx_core sync)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orderbook_updates_instid_ts_ingest 
ON okx_raw.orderbook_updates(instid, ts_ingest_ms);

-- ============================================================================
-- VALIDATION QUERIES
-- ============================================================================
-- Run these after migration to verify:

-- 1. Check no NULL values remain
-- SELECT COUNT(*) as null_count FROM okx_raw.orderbook_updates WHERE ts_ingest_ms IS NULL;
-- Expected: 0

-- 2. Check distribution of (ts_ingest_ms - ts_event_ms) latency
-- SELECT 
--     COUNT(*) as total_rows,
--     percentile_cont(0.50) WITHIN GROUP (ORDER BY ts_ingest_ms - ts_event_ms) as p50_latency_ms,
--     percentile_cont(0.95) WITHIN GROUP (ORDER BY ts_ingest_ms - ts_event_ms) as p95_latency_ms,
--     percentile_cont(0.99) WITHIN GROUP (ORDER BY ts_ingest_ms - ts_event_ms) as p99_latency_ms,
--     MIN(ts_ingest_ms - ts_event_ms) as min_latency_ms,
--     MAX(ts_ingest_ms - ts_event_ms) as max_latency_ms
-- FROM okx_raw.orderbook_updates;

-- 3. Verify indexes exist
-- SELECT indexname, indexdef FROM pg_indexes 
-- WHERE tablename = 'orderbook_updates' AND schemaname = 'okx_raw';

-- ============================================================================
-- ROLLBACK (if needed)
-- ============================================================================
-- DROP INDEX CONCURRENTLY IF EXISTS okx_raw.idx_orderbook_updates_ts_ingest_ms;
-- DROP INDEX CONCURRENTLY IF EXISTS okx_raw.idx_orderbook_updates_instid_ts_ingest;
-- ALTER TABLE okx_raw.orderbook_updates DROP COLUMN IF EXISTS ts_ingest_ms;

