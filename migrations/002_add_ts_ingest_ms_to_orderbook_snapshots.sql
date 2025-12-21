-- Migration: Add ts_ingest_ms to okx_raw.orderbook_snapshots
-- Date: 2024-12-21
-- Purpose: Enable incremental replication from okx_raw → okx_core using watermark on ts_ingest_ms
--
-- IMPORTANT: This migration is designed to be safe for:
--   - Large tables (batched backfill)
--   - TimescaleDB hypertables
--   - Zero-downtime deployments

-- ============================================================================
-- STEP 1: Add column as NULLABLE (instant, no lock)
-- ============================================================================
ALTER TABLE okx_raw.orderbook_snapshots 
ADD COLUMN IF NOT EXISTS ts_ingest_ms BIGINT;

-- ============================================================================
-- STEP 2: Backfill existing rows
-- ============================================================================
-- For historical rows, use ts_event_ms as best available proxy
UPDATE okx_raw.orderbook_snapshots 
SET ts_ingest_ms = ts_event_ms 
WHERE ts_ingest_ms IS NULL;

-- ============================================================================
-- STEP 3: Set NOT NULL constraint (after backfill complete)
-- ============================================================================
ALTER TABLE okx_raw.orderbook_snapshots 
ALTER COLUMN ts_ingest_ms SET NOT NULL;

-- ============================================================================
-- STEP 4: Create indexes for incremental sync (run OUTSIDE transaction!)
-- ============================================================================
-- Index for watermark-based incremental sync
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orderbook_snapshots_ts_ingest_ms 
ON okx_raw.orderbook_snapshots(ts_ingest_ms);

-- ============================================================================
-- VALIDATION QUERIES
-- ============================================================================
-- 1. Check no NULL values remain
-- SELECT COUNT(*) as null_count FROM okx_raw.orderbook_snapshots WHERE ts_ingest_ms IS NULL;

-- 2. Check latency distribution
-- SELECT 
--     COUNT(*) as total_rows,
--     percentile_cont(0.50) WITHIN GROUP (ORDER BY ts_ingest_ms - ts_event_ms) as p50_ms,
--     percentile_cont(0.95) WITHIN GROUP (ORDER BY ts_ingest_ms - ts_event_ms) as p95_ms
-- FROM okx_raw.orderbook_snapshots;

-- ============================================================================
-- ROLLBACK (if needed)
-- ============================================================================
-- DROP INDEX CONCURRENTLY IF EXISTS okx_raw.idx_orderbook_snapshots_ts_ingest_ms;
-- ALTER TABLE okx_raw.orderbook_snapshots DROP COLUMN IF EXISTS ts_ingest_ms;

