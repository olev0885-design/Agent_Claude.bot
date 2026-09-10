# =============================================================================
# test_batch.py — состояние "тестовой серии" сделок сканера: жёсткий
# счётчик из N сделок, переключение в режим "только мониторинг/закрытие"
# после открытия N-й, и агрегация статистики закрытых сделок для финального
# отчёта (общий PnL, среднее время удержания, win-rate).
# =============================================================================
# Сама логика открытия/закрытия ордеров и общения с биржами остаётся в
# scanner.py/trade_executor.py — этот класс только СЧИТАЕТ и ХРАНИТ
# состояние серии, никаких сетевых вызовов здесь нет (легко тестировать
# изолированно, без CCXT/Telegram).
# =============================================================================
import time
from dataclasses import dataclass


PROSPECTING = "prospecting"  # ищем новые связки, открываем сделки серии
MONITORING = "monitoring"    # серия заполнена (N/N открыто) — только мониторим/закрываем
DONE = "done"                # все N закрыты, финальный отчёт отправлен


@dataclass
class TestBatchPosition:
    coin: str
    long_exchange: str
    short_exchange: str
    long_symbol: str
    short_symbol: str
    entry_spread_pct: float
    opened_at: float  # time.monotonic() — для точного, не зависящего от системных часов, holding time
    # Нужны для оценки СУММАРНОГО PnL связки (Long+Short) ДО реального
    # закрытия — см. scanner.py:_monitor_test_batch() и комментарий там:
    # закрываем, только когда спред сошёлся И это уже прибыльно, а не по
    # одному только текущему спреду.
    long_entry_price: float
    short_entry_price: float
    amount_usdt: float  # объём ОДНОЙ ноги (не суммарный на обе)
    # Комиссия за вход (обе ноги, USDT) — известна из position_store сразу
    # после открытия; используется как ОЦЕНКА комиссии за выход тоже (та же
    # монета/биржи/объём — ставка обычно та же), чтобы прикидывать ЧИСТУЮ
    # (после комиссий) прибыль ещё ДО фактического закрытия. None, если
    # ставку не удалось узнать — тогда сравниваем по gross PnL (без
    # поправки на комиссии, см. _monitor_test_batch).
    entry_fee_total: "float | None" = None


@dataclass
class ClosedTestTrade:
    coin: str
    long_exchange: str
    short_exchange: str
    entry_spread_pct: float
    exit_spread_pct: float
    long_pnl: "float | None"   # результат ИМЕННО long-ноги (реальный, из close_structured_signal_detailed)
    short_pnl: "float | None"  # результат ИМЕННО short-ноги
    pnl_amount: float          # суммарный PnL обеих ног (long_pnl + short_pnl, после комиссий, если известны)
    pnl_percent: float
    holding_seconds: float


class TestBatchTracker:
    """Держит состояние одной "тестовой серии": сколько уже открыто/закрыто,
    какие позиции сейчас в мониторинге, статистику по закрытым сделкам.

    Жизненный цикл: PROSPECTING (открываем, пока не наберём `size` штук) ->
    MONITORING (все `size` открыты, ждём закрытия каждой по своему целевому
    спреду) -> DONE (все закрыты, дальше ничего не делаем)."""

    def __init__(
        self, size: int, amount_usdt: float, min_spread_pct: float,
        close_spread_pct: float, close_spread_max_pct: float = 1.0,
    ):
        self.size = size
        self.amount_usdt = amount_usdt
        self.min_spread_pct = min_spread_pct
        self.close_spread_pct = close_spread_pct
        # Верхняя граница диапазона закрытия (см. TestBatchConfig в
        # config.py за подробным комментарием) — по умолчанию 1.0 для
        # обратной совместимости с прямыми вызовами без этого аргумента.
        self.close_spread_max_pct = close_spread_max_pct

        self.state = PROSPECTING
        self.open_positions: dict[str, TestBatchPosition] = {}
        self.closed_trades: list[ClosedTestTrade] = []

    @property
    def opened_count(self) -> int:
        """Сколько сделок серии УЖЕ было открыто за всё время (открытые
        сейчас + уже закрытые) — именно это число сравнивается с `size`,
        а не текущее количество ОТКРЫТЫХ (то падает по мере закрытия)."""
        return len(self.open_positions) + len(self.closed_trades)

    def can_open_more(self) -> bool:
        return self.state == PROSPECTING and self.opened_count < self.size

    def record_open(
        self,
        coin: str,
        long_exchange: str,
        short_exchange: str,
        long_symbol: str,
        short_symbol: str,
        entry_spread_pct: float,
        long_entry_price: float,
        short_entry_price: float,
        amount_usdt: float,
        entry_fee_total: "float | None" = None,
    ) -> None:
        """Регистрирует УСПЕШНО открытую позицию серии. Если это была
        `size`-я по счёту — сразу переключает состояние в MONITORING,
        чтобы _scan_once() (см. scanner.py) на следующем же цикле перестал
        искать новые связки и перешёл в режим мониторинга/закрытия."""
        self.open_positions[coin] = TestBatchPosition(
            coin=coin,
            long_exchange=long_exchange,
            short_exchange=short_exchange,
            long_symbol=long_symbol,
            short_symbol=short_symbol,
            entry_spread_pct=entry_spread_pct,
            opened_at=time.monotonic(),
            long_entry_price=long_entry_price,
            short_entry_price=short_entry_price,
            amount_usdt=amount_usdt,
            entry_fee_total=entry_fee_total,
        )
        if self.opened_count >= self.size:
            self.state = MONITORING

    def record_close(
        self,
        coin: str,
        exit_spread_pct: float,
        long_pnl: "float | None",
        short_pnl: "float | None",
        pnl_amount: float,
        pnl_percent: float,
    ):
        """Регистрирует УСПЕШНО закрытую позицию серии. Возвращает
        ClosedTestTrade (для форматирования Telegram-алерта закрытия) или
        None, если этой монеты не было среди открытых позиций серии
        (защита от повторного/чужого закрытия). Если это была последняя
        ОТКРЫТАЯ позиция серии — переключает состояние в DONE."""
        position = self.open_positions.pop(coin, None)
        if position is None:
            return None

        holding_seconds = time.monotonic() - position.opened_at
        trade = ClosedTestTrade(
            coin=coin,
            long_exchange=position.long_exchange,
            short_exchange=position.short_exchange,
            entry_spread_pct=position.entry_spread_pct,
            exit_spread_pct=exit_spread_pct,
            long_pnl=long_pnl,
            short_pnl=short_pnl,
            pnl_amount=pnl_amount,
            pnl_percent=pnl_percent,
            holding_seconds=holding_seconds,
        )
        self.closed_trades.append(trade)

        if not self.open_positions and self.opened_count >= self.size:
            self.state = DONE

        return trade

    def summary(self) -> dict:
        """Финальная сводка по серии — см. scanner.py:_send_batch_summary().
        Вызывать, когда self.state == DONE (но технически можно и раньше —
        просто будет частичная сводка по уже закрытым на этот момент)."""
        n = len(self.closed_trades)
        total_pnl = sum(t.pnl_amount for t in self.closed_trades)
        avg_holding = (sum(t.holding_seconds for t in self.closed_trades) / n) if n else 0.0
        wins = sum(1 for t in self.closed_trades if t.pnl_amount > 0)
        losses = n - wins
        return {
            "closed_count": n,
            "total_pnl": total_pnl,
            "avg_holding_seconds": avg_holding,
            "wins": wins,
            "losses": losses,
            "win_rate_pct": (wins / n * 100) if n else 0.0,
        }
