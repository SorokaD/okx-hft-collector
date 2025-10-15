
from okx_hft.dq.interfaces import IDQ

class SimpleDQ(IDQ):
    def check_checksum(self, checksum: int, book_levels: int = 25) -> bool:
        # TODO: implement OKX CRC32 spec (books-l2-tbt); placeholder returns True
        return True

    def check_sequence(self, prev_seq: int, curr_seq: int) -> bool:
        return curr_seq == prev_seq + 1

    def staleness_ms(self, event_ts_ms: int, now_ms: int) -> int:
        return max(0, now_ms - event_ts_ms)
