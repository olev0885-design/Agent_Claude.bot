# =============================================================================
# deploy/check_server.py — проверка сервера ПЕРЕД первым запуском бота.
# =============================================================================
# Отвечает на четыре вопроса, каждый из которых на ноутбуке уже стоил денег
# или нервов:
#   1. Часы: расхождение с биржами (clock_guard) — должно быть < 1с.
#   2. Публичный доступ: fetch_time / load_markets с этого IP — ловит
#      гео-блокировки (как KuCoin «you reside in a country…») ДО того, как
#      бот начнёт торговать.
#   3. Приватный доступ: fetch_balance с ключами из .env — проверяет, что
#      ключи работают с НОВОГО IP (если на бирже стоит whitelist IP — здесь
#      это и всплывёт).
#   4. Задержка: RTT до каждой биржи — ради этого и переезжали; сравнить с
#      ноутбуком (там было 200–300мс).
# Запуск на сервере (после копирования .env):
#   sudo -u bot /opt/agent-claude-bot/venv/bin/python deploy/check_server.py
# Ничего не торгует и ничего не меняет.
# =============================================================================
import asyncio
import os
import statistics
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from dotenv import load_dotenv  # noqa: E402

load_dotenv(os.path.join(ROOT, ".env"))

import ccxt.async_support as ccxt  # noqa: E402
from bot_crew import clock_guard  # noqa: E402
from bot_crew.tools.trade_tool import TradeExecutionTool, EXCHANGE_ALIASES, EXCHANGE_QUOTE_CURRENCY  # noqa: E402

EXCHANGES = [e.strip() for e in (os.getenv("TEST_BATCH_SAFE_EXCHANGES") or "gate,mexc,bitget,hyperliquid,bybit,binance").split(",") if e.strip()]
# Кандидаты на добавление — проверяем только публичный доступ (ключей нет).
EXTRA_PUBLIC = ["okx", "kucoinfutures", "bingx", "htx"]


async def rtt_ms(ex, n=5):
    samples = []
    for _ in range(n):
        t0 = time.perf_counter()
        try:
            await ex.fetch_time()
        except Exception:
            # у части бирж fetch_time нет — берём самый лёгкий публичный вызов
            try:
                await ex.fetch_ticker(next(iter(ex.symbols)) if ex.symbols else "BTC/USDT:USDT")
            except Exception:
                return None
        samples.append((time.perf_counter() - t0) * 1000)
    return statistics.median(samples)


async def public_check(name: str):
    ex_id = EXCHANGE_ALIASES.get(name, name)
    ex = getattr(ccxt, ex_id)({"options": {"defaultType": "swap"}, "timeout": 15000})
    try:
        await ex.load_markets()
        swaps = sum(1 for m in ex.markets.values() if m.get("swap") and m.get("linear"))
        rtt = await rtt_ms(ex)
        return name, f"OK  рынков-свопов {swaps:>4}   RTT {rtt:6.0f} мс" if rtt is not None else f"OK  рынков {swaps} (RTT не измерен)", True
    except Exception as exc:
        text = str(exc)
        geo = any(k in text.lower() for k in ("reside", "restricted", "not available in your", "region", "country", "451", "403"))
        return name, f"ОШИБКА{' — ПОХОЖЕ НА ГЕО-БЛОКИРОВКУ' if geo else ''}: {type(exc).__name__}: {text[:110]}", False
    finally:
        try:
            await ex.close()
        except Exception:
            pass


async def private_check(tool, name: str):
    try:
        ex = await tool._get_ready_client(name)
        t0 = time.perf_counter()
        bal = await ex.fetch_balance()
        ms = (time.perf_counter() - t0) * 1000
        quote = EXCHANGE_QUOTE_CURRENCY.get(EXCHANGE_ALIASES.get(name, name), "USDT")
        total = (bal.get(quote) or {}).get("total")
        return name, f"OK  баланс {total if total is not None else '?'} {quote}   fetch_balance {ms:.0f} мс", True
    except Exception as exc:
        return name, f"ОШИБКА: {type(exc).__name__}: {str(exc)[:110]}", False


async def main() -> int:
    ok_all = True
    print("=== 1. ЧАСЫ ===")
    off = await clock_guard.measure_offset()
    if off is None:
        print("  не удалось измерить (сеть?)"); ok_all = False
    else:
        good = abs(off) < 1.0
        ok_all &= good
        print(f"  {clock_guard.describe(off)}  ->  {'OK' if good else 'ПЛОХО: проверьте chrony/timedatectl'}")

    print("\n=== 2. ПУБЛИЧНЫЙ ДОСТУП + ЗАДЕРЖКА (наши биржи) ===")
    for name, text, good in await asyncio.gather(*(public_check(n) for n in EXCHANGES)):
        ok_all &= good
        print(f"  {name:<12} {text}")

    print("\n=== 2b. КАНДИДАТЫ НА ДОБАВЛЕНИЕ (только публичный доступ) ===")
    for name, text, _ in await asyncio.gather(*(public_check(n) for n in EXTRA_PUBLIC)):
        print(f"  {name:<12} {text}")

    print("\n=== 3. ПРИВАТНЫЙ ДОСТУП (ключи из .env, с ЭТОГО IP) ===")
    tool = TradeExecutionTool()
    for name, text, good in await asyncio.gather(*(private_check(tool, n) for n in EXCHANGES)):
        ok_all &= good
        print(f"  {name:<12} {text}")

    print("\n=== ИТОГ ===")
    print("  ВСЁ В ПОРЯДКЕ — можно запускать службу." if ok_all else "  ЕСТЬ ПРОБЛЕМЫ — см. выше. Не запускать, пока не решены.")
    return 0 if ok_all else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
