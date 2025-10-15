
# OKX WS messages (public)

## books-l2-tbt (snapshot & incrementals)
Fields: arg{channel,instId}, action, data[ { asks, bids, ts, checksum, seqId, prevSeqId } ]
- asks/bids: arrays of [price, size]
- checksum: CRC32 over first N levels (TODO confirm)
- Apply: snapshot -> increments by seqId; validate prevSeqId continuity

## trades
data[ { instId, tradeId, px, sz, side, ts } ]
Dedup by (instId, tradeId).
