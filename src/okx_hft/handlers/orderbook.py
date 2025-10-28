import time
from typing import Dict, Any, List, Tuple
from okx_hft.handlers.interfaces import IOrderBookHandler
from okx_hft.storage.interfaces import IStorage
from okx_hft.utils.logging import get_logger

log = get_logger(__name__)


class OrderBookHandler(IOrderBookHandler):
    def __init__(self, storage: IStorage = None) -> None:
        self.storage = storage
        self.batch: List[Dict[str, Any]] = []
        self.batch_max_size = 100  # Батч размер для order book
        # Храним снапшоты по инструментам
        self.snapshots: Dict[str, Dict[str, Any]] = {}

    async def on_snapshot(self, msg: Dict[str, Any]) -> None:
        """Обработка снапшота стакана заявок"""
        try:
            arg = msg.get("arg", {})
            inst_id = arg.get("instId", "")
            data = msg.get("data", [])

            if not data:
                return

            for snapshot in data:
                processed_snapshot = self._process_snapshot(snapshot, inst_id)
                if processed_snapshot:
                    self.batch.append(processed_snapshot)
                    # Сохраняем снапшот для последующих инкрементальных обновлений
                    self.snapshots[inst_id] = snapshot

            # Отправляем батч если достигли максимального размера
            if len(self.batch) >= self.batch_max_size:
                await self._flush_batch()

        except Exception as e:
            log.error(f"Error processing order book snapshot: {str(e)}")

    async def on_increment(self, msg: Dict[str, Any]) -> None:
        """Обработка инкрементального обновления стакана заявок"""
        try:
            arg = msg.get("arg", {})
            inst_id = arg.get("instId", "")
            data = msg.get("data", [])

            if not data:
                return

            for update in data:
                processed_update = self._process_increment(update, inst_id)
                if processed_update:
                    self.batch.append(processed_update)

            # Отправляем батч если достигли максимального размера
            if len(self.batch) >= self.batch_max_size:
                await self._flush_batch()

        except Exception as e:
            log.error(f"Error processing order book increment: {str(e)}")

    def _process_snapshot(
        self, snapshot: Dict[str, Any], inst_id: str
    ) -> Dict[str, Any] | None:
        """Обработка снапшота"""
        try:
            bids = self._parse_price_levels(snapshot.get("bids", []))
            asks = self._parse_price_levels(snapshot.get("asks", []))

            spread, mid = self._calculate_spread_mid(bids, asks)

            return {
                "instId": inst_id,
                "ts_event_ms": int(snapshot.get("ts", 0)),
                "seqId": int(snapshot.get("seqId", 0)),
                "bids": bids,
                "asks": asks,
                "spread": spread,
                "mid": mid,
                "cs_ok": 1,  # TODO: реализовать проверку checksum
                "ts_ingest_ms": int(time.time() * 1000),
            }
        except (ValueError, TypeError) as e:
            log.error(f"Error processing snapshot: {str(e)}")
            return None

    def _process_increment(
        self, update: Dict[str, Any], inst_id: str
    ) -> Dict[str, Any] | None:
        """Обработка инкрементального обновления"""
        try:
            # Получаем текущий снапшот
            current_snapshot = self.snapshots.get(inst_id, {})
            current_bids = self._parse_price_levels(current_snapshot.get("bids", []))
            current_asks = self._parse_price_levels(current_snapshot.get("asks", []))

            # Применяем инкрементальные обновления
            new_bids = self._apply_increments(current_bids, update.get("bids", []))
            new_asks = self._apply_increments(current_asks, update.get("asks", []))

            # Обновляем снапшот
            current_snapshot["bids"] = new_bids
            current_snapshot["asks"] = new_asks
            current_snapshot["ts"] = update.get("ts", 0)
            current_snapshot["seqId"] = update.get("seqId", 0)
            self.snapshots[inst_id] = current_snapshot

            spread, mid = self._calculate_spread_mid(new_bids, new_asks)

            return {
                "instId": inst_id,
                "ts_event_ms": int(update.get("ts", 0)),
                "seqId": int(update.get("seqId", 0)),
                "bids": new_bids,
                "asks": new_asks,
                "spread": spread,
                "mid": mid,
                "cs_ok": 1,  # TODO: реализовать проверку checksum
                "ts_ingest_ms": int(time.time() * 1000),
            }
        except (ValueError, TypeError) as e:
            log.error(f"Error processing increment: {str(e)}")
            return None

    def _parse_price_levels(self, levels: List[List[str]]) -> List[Tuple[float, float]]:
        """Парсинг уровней цен"""
        return [
            (float(level[0]), float(level[1])) for level in levels if len(level) >= 2
        ]

    def _apply_increments(
        self, current: List[Tuple[float, float]], increments: List[List[str]]
    ) -> List[Tuple[float, float]]:
        """Применение инкрементальных обновлений"""
        result = current.copy()

        for increment in increments:
            if len(increment) < 2:
                continue

            price = float(increment[0])
            size = float(increment[1])

            # Удаляем существующий уровень с такой ценой
            result = [(p, s) for p, s in result if p != price]

            # Добавляем новый уровень если размер > 0
            if size > 0:
                result.append((price, size))

        # Сортируем: bids по убыванию цены, asks по возрастанию
        return sorted(result, key=lambda x: x[0], reverse=True)

    def _calculate_spread_mid(
        self, bids: List[Tuple[float, float]], asks: List[Tuple[float, float]]
    ) -> Tuple[float, float]:
        """Расчет спреда и средней цены"""
        if not bids or not asks:
            return 0.0, 0.0

        best_bid = bids[0][0] if bids else 0.0
        best_ask = asks[0][0] if asks else 0.0

        spread = best_ask - best_bid if best_ask > 0 and best_bid > 0 else 0.0
        mid = (best_bid + best_ask) / 2 if best_ask > 0 and best_bid > 0 else 0.0

        return spread, mid

    async def _flush_batch(self) -> None:
        """Отправка батча в хранилище"""
        if self.batch:
            if self.storage:
                try:
                    # Удаляем запись в несуществующую таблицу lob_updates
                    log.info(
                        f"Skipping flush of {len(self.batch)} order book updates - "
                        f"lob_updates table removed"
                    )
                    self.batch = []
                except Exception as e:
                    log.error(f"Error flushing order book batch: {str(e)}")
            else:
                log.info(
                    f"No storage available, skipping flush of {len(self.batch)} order book updates"
                )
                self.batch = []

    async def flush(self) -> None:
        """Принудительная отправка оставшихся данных"""
        if self.batch:
            await self._flush_batch()
