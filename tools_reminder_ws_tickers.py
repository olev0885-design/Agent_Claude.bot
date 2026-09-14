# =============================================================================
# tools_reminder_ws_tickers.py — разовое напоминание в Telegram @Depositik.
# =============================================================================
# По прямой просьбе пользователя 2026-09-14: "давай пока оставим эти
# изменения, посмотрим как ставки будут идти ночью, а об этом обновлении
# напомни завтра в телеграм @Depositik в 15.00".
#
# Запускается Планировщиком Windows (задача "AgentClaudeBot-ReminderWS",
# 2026-09-15 15:00 локального времени). НЕ открывает Telegram сам — кладёт
# письмо в telegram_outbox/, откуда его отправит работающий бот своим
# клиентом (см. scanner.py:_telegram_outbox_loop). Если бот в 15:00 не
# запущен, письмо дождётся его запуска и уйдёт первым делом.
#
# К напоминанию прикладывается короткая сводка по сделкам с вечера —
# пользователь как раз хотел «посмотреть, как ставки пойдут ночью».
# =============================================================================
import json
import os
import sys
from datetime import datetime, timezone, timedelta

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(ROOT, "src"))

SINCE_UTC = datetime(2026, 9, 14, 20, 0, tzinfo=timezone.utc)  # ~22:00 локального 14.09


def overnight_summary() -> str:
    path = os.path.join(ROOT, "trade_ledger.jsonl")
    opens = closes = 0
    net = 0.0
    lat_open, lat_close = [], []
    coins = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                r = json.loads(line)
                ts_raw = r.get("closed_at") or r.get("opened_at") or r.get("recorded_at")
                try:
                    ts = datetime.fromisoformat(ts_raw)
                except Exception:
                    continue
                if ts < SINCE_UTC:
                    continue
                if r.get("event") == "open":
                    opens += 1
                    if r.get("elapsed_ms") is not None:
                        lat_open.append(r["elapsed_ms"])
                else:
                    closes += 1
                    if r.get("net_pnl") is not None:
                        net += r["net_pnl"]
                        coins.append(f"{r.get('coin')} {r['net_pnl']:+.3f}")
                    if r.get("elapsed_ms") is not None:
                        lat_close.append(r["elapsed_ms"])
    except FileNotFoundError:
        return "Журнал сделок не найден."
    if not opens and not closes:
        return "За ночь сделок не было."
    def med(v):
        return f"{sorted(v)[len(v)//2]:.0f} мс" if v else "—"
    parts = [
        f"открытий {opens}, закрытий {closes}, итог по закрытым **{net:+.3f} USDT**",
        f"задержка на вход (медиана) {med(lat_open)}, на выход {med(lat_close)}",
    ]
    if coins:
        parts.append("закрыто: " + ", ".join(coins[:8]) + (" …" if len(coins) > 8 else ""))
    return "\n".join("• " + p for p in parts)


def main() -> int:
    text = (
        "⏰ **Напоминание (поставлено вчера вечером)**\n\n"
        "Следующий шаг по скорости бота — **тикеры по WebSocket**. Сейчас сканер "
        "опрашивает биржи по REST циклом ~14 с, то есть спред замечается в среднем "
        "через 7 с после появления, а короткоживущие спреды (5–10 с) не видны вовсе. "
        "С потоковыми тикерами обнаружение станет <1 с.\n\n"
        "План: сначала mexc/hyperliquid/binance (все тикеры одним потоком), затем "
        "gate/bitget/bybit/aster (подписка на 921 «парную» монету партиями), REST "
        "остаётся запасным путём. Работа на несколько часов с обкаткой.\n\n"
        "Решение было отложено, чтобы посмотреть на вчерашние изменения "
        "(потоковые ордера/позиции, стакан для кандидатов) в ночных сделках.\n\n"
        f"**С вечера 14.09:**\n{overnight_summary()}\n\n"
        "Если готовы — скажите «делаем тикеры», и я начну."
    )
    outbox = os.path.join(ROOT, "telegram_outbox")
    os.makedirs(outbox, exist_ok=True)
    name = f"{datetime.now(timezone.utc):%Y%m%d_%H%M%S}_reminder_ws_tickers.txt"
    with open(os.path.join(outbox, name), "w", encoding="utf-8") as f:
        f.write("to: @Depositik, me\n\n" + text)
    print(f"письмо положено в ящик: {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
