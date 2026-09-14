# =============================================================================
# trade_ledger.py — ПЕРСИСТЕНТНЫЙ журнал ЗАВЕРШЁННЫХ (открыта+закрыта)
# сделок. По прямой просьбе пользователя 2026-09-10 ("нет персистентного
# журнала сделок — весь PnL-анализ вручную, реконструкцией по истории
# ордеров на биржах") — раньше единственная история была: (а) в памяти
# test_batch.py (сбрасывается при каждом рестарте, а рестартов за сессию
# были десятки) и (б) на самих биржах (fetch_my_trades — не даёт готового
# net_pnl/причину закрытия, только сырые сделки, нужно реконструировать
# вручную скриптами каждый раз, как это делалось весь этот разговор).
#
# ЭТО НЕ источник истины для того, что СЕЙЧАС открыто — это
# position_store.py, journal сюда не пишется. Ledger — только для
# ОТЧЁТНОСТИ и анализа ПОСЛЕ факта (win-rate, средний PnL, какие монеты/
# биржи систематически убыточны и т.п.).
#
# Формат — JSONL (одна сделка = одна строка JSON), не единый JSON-массив:
# запись — это ДОЗАПИСЬ в конец файла, не нужно перечитывать и
# перезаписывать весь файл на каждую сделку (в отличие от position_store/
# blocked_coins_store, где всего несколько записей одновременно и
# перезапись всего файла дёшева — здесь счёт может пойти на сотни/тысячи
# сделок за время работы бота).
# =============================================================================
import json
import os
from datetime import datetime, timezone

_LEDGER_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "trade_ledger.jsonl",
)


def record_trade(entry: dict) -> None:
    """Дописывает ОДНУ завершённую сделку в конец журнала. НЕ бросает
    исключение наружу при сбое записи (сбой логирования не должен ронять
    реальное закрытие сделки, которое уже произошло) — только печатает
    предупреждение."""
    entry = dict(entry)
    entry.setdefault("recorded_at", datetime.now(timezone.utc).isoformat())
    try:
        with open(_LEDGER_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as exc:
        print(f"[trade_ledger] не удалось записать сделку {entry.get('coin')} в журнал: {exc}")


def read_all() -> list:
    """Читает ВЕСЬ журнал целиком — для отчётов/анализа, не для горячего
    пути (та же логика, что list_positions() в position_store.py, но
    построчно, т.к. формат JSONL, а не единый JSON-объект)."""
    if not os.path.exists(_LEDGER_PATH):
        return []
    trades = []
    with open(_LEDGER_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                trades.append(json.loads(line))
            except json.JSONDecodeError:
                continue  # повреждённая строка (напр. обрыв записи при крахе) — пропускаем, не роняем весь журнал
    return trades


def daily_stats(target_date=None, trades: "list | None" = None) -> dict:
    """Сводка ЗА ОДНИ КОНКРЕТНЫЕ СУТКИ (UTC) — по прямой просьбе
    пользователя 2026-09-12 ("отчёт по задержкам на ноги и какой +пнл за
    сутки", ежедневная рассылка в @Depositik/@G_Pobedonosec, см.
    scanner.py:_daily_report_loop). target_date — datetime.date, по
    умолчанию сегодня (UTC). Различает событие "open" (см. _finalize_open
    в trade_executor.py — elapsed_ms там — это задержка МЕЖДУ НОГАМИ при
    ОТКРЫТИИ) от обычных записей закрытых сделок (elapsed_ms там — это
    задержка между ногами при ЗАКРЫТИИ, см. _finalize_close) — старые
    записи журнала (до 2026-09-12) не имеют поля "event" вообще, для них
    считаем это закрытием (все записи ДО этой даты — именно закрытия,
    "open" начали писать только сейчас)."""
    if trades is None:
        trades = read_all()
    if target_date is None:
        target_date = datetime.now(timezone.utc).date()

    def _entry_date(entry: dict):
        ts = entry.get("closed_at") or entry.get("opened_at") or entry.get("recorded_at")
        if not ts:
            return None
        try:
            return datetime.fromisoformat(ts).date()
        except (TypeError, ValueError):
            return None

    todays = [t for t in trades if _entry_date(t) == target_date]
    opens = [t for t in todays if t.get("event") == "open"]
    closes = [t for t in todays if t.get("event") != "open"]

    open_latencies = [t["elapsed_ms"] for t in opens if t.get("elapsed_ms") is not None]
    close_latencies = [t["elapsed_ms"] for t in closes if t.get("elapsed_ms") is not None]
    net_pnls = [t["net_pnl"] for t in closes if t.get("net_pnl") is not None]

    return {
        "date": target_date.isoformat(),
        "opens_count": len(opens),
        "closes_count": len(closes),
        "avg_open_latency_ms": (sum(open_latencies) / len(open_latencies)) if open_latencies else None,
        "avg_close_latency_ms": (sum(close_latencies) / len(close_latencies)) if close_latencies else None,
        "total_net_pnl": sum(net_pnls) if net_pnls else 0.0,
        "wins": len([p for p in net_pnls if p > 0]),
        "losses": len([p for p in net_pnls if p <= 0]),
    }


def summary_stats(trades: "list | None" = None) -> dict:
    """Быстрая сводка по журналу — win-rate, средний/суммарный net PnL и
    т.п. Удобно для отчётов вроде "дай отчёт по пнл" — раньше это
    считалось вручную специальными скриптами каждый раз заново."""
    if trades is None:
        trades = read_all()
    if not trades:
        return {"count": 0}
    net_pnls = [t.get("net_pnl") for t in trades if t.get("net_pnl") is not None]
    wins = [p for p in net_pnls if p > 0]
    losses = [p for p in net_pnls if p <= 0]
    total = sum(net_pnls) if net_pnls else 0.0
    return {
        "count": len(trades),
        "with_pnl": len(net_pnls),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": (len(wins) / len(net_pnls) * 100) if net_pnls else None,
        "total_net_pnl": total,
        "avg_net_pnl": (total / len(net_pnls)) if net_pnls else None,
    }
