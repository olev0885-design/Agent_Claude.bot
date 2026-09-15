# =============================================================================
# deploy/measure_ticker_streams.py — ЗАМЕР нагрузки потоковых цен. Ничего не
# торгует. v2 (2026-09-15): гибрид вместо «подписаться на всё» — первый замер
# (906 монет, ~4400 подписок) дал ~480 сообщ/с и 100% одного ядра, bitget
# отвергал пакеты по 50, mexc не принимает watch_bids_asks без списка.
#   - mexc / hyperliquid / binance: ВСЕ тикеры одним потоком (watch_tickers());
#   - gate / bitget / bybit: только «горячий список» из HOT символов, пакетами
#     по BATCH (bitget не любит большие пакеты).
# Считаем РЕАЛЬНЫЕ события (изменение bid/ask по символу), а не размер
# возвращаемых словарей. Запуск на сервере под nice.
# =============================================================================
import asyncio, os, sys, time, collections, resource
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from dotenv import load_dotenv; load_dotenv(os.path.join(ROOT, ".env"))
import ccxt.pro as ccxtpro
from bot_crew.tools.trade_tool import EXCHANGE_ALIASES

EX = [e.strip() for e in (os.getenv("TEST_BATCH_SAFE_EXCHANGES") or "").split(",") if e.strip()]
DURATION = float(sys.argv[1]) if len(sys.argv) > 1 else 90.0
HOT = int(sys.argv[2]) if len(sys.argv) > 2 else 150
BATCH = int(sys.argv[3]) if len(sys.argv) > 3 else 20
ALL_AT_ONCE = {"mexc", "hyperliquid", "binance"}

events = collections.Counter(); frames = collections.Counter(); errors = collections.Counter()
seen = collections.defaultdict(dict)  # ex -> symbol -> (bid, ask)

async def load(n):
    ex = getattr(ccxtpro, EXCHANGE_ALIASES.get(n, n))({"options": {"defaultType": "swap"}, "enableRateLimit": True})
    await ex.load_markets()
    ms = [m for m in ex.markets.values() if m.get("swap") and m.get("linear") and m.get("active", True)]
    return n, ex, ms

def note(n, sym, t):
    b, a = t.get("bid"), t.get("ask")
    if b is None and a is None: return
    prev = seen[n].get(sym)
    if prev != (b, a):
        seen[n][sym] = (b, a); events[n] += 1

async def pump(n, ex, symbols, stop_at):
    while time.monotonic() < stop_at:
        try:
            if symbols is None:
                data = await asyncio.wait_for(ex.watch_tickers(), timeout=40)
            elif ex.has.get("watchBidsAsks"):
                data = await asyncio.wait_for(ex.watch_bids_asks(symbols), timeout=40)
            else:
                data = await asyncio.wait_for(ex.watch_tickers(symbols), timeout=40)
            frames[n] += 1
            for sym, t in (data.items() if isinstance(data, dict) else []):
                note(n, sym, t)
        except asyncio.TimeoutError:
            errors[n + ":timeout"] += 1
        except Exception as e:
            errors[n + ":" + type(e).__name__] += 1
            await asyncio.sleep(2)

async def main():
    loaded = await asyncio.gather(*(load(n) for n in EX))
    presence = collections.Counter(m["base"] for _, _, ms in loaded for m in ms)
    stop_at = time.monotonic() + DURATION
    tasks = []; plan = []
    for n, ex, ms in loaded:
        pairable = [m for m in ms if presence[m["base"]] >= 2]
        if n in ALL_AT_ONCE:
            plan.append(f"  {n:<12} все тикеры одним потоком ({len(pairable)} парных монет)")
            tasks.append(pump(n, ex, None, stop_at))
        else:
            # «горячий список»: топ по обороту среди парных — суррогат того, что
            # в боте будет формироваться из результатов REST-сканера
            hot = sorted(pairable, key=lambda m: -(float((m.get("info") or {}).get("volume_24h_quote") or 0) if isinstance(m.get("info"), dict) else 0))[:HOT]
            if len(hot) < HOT: hot = pairable[:HOT]
            syms = [m["symbol"] for m in hot]
            batches = [syms[i:i+BATCH] for i in range(0, len(syms), BATCH)]
            plan.append(f"  {n:<12} горячий список {len(syms)} монет — {len(batches)} пакетов по {BATCH}")
            for b in batches:
                tasks.append(pump(n, ex, b, stop_at))
    print("\n".join(plan)); print(f"\nзамер {DURATION:.0f}с...", flush=True)
    cpu0 = time.process_time(); t0 = time.monotonic()
    await asyncio.gather(*tasks, return_exceptions=True)
    wall = time.monotonic() - t0; cpu = time.process_time() - cpu0
    print(f"\n=== РЕЗУЛЬТАТ за {wall:.0f}с ===")
    for n in EX:
        print(f"  {n:<12} кадров {frames[n]/wall:6.1f}/с   реальных изменений bid/ask {events[n]/wall:7.1f}/с   монет с данными {len(seen[n])}")
    print(f"  {'ИТОГО':<12} кадров {sum(frames.values())/wall:6.1f}/с   изменений {sum(events.values())/wall:7.1f}/с")
    print(f"  CPU процесса: {cpu/wall*100:.0f}% одного ядра   RSS: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024:.0f} MB")
    if errors: print("  ошибки:", dict(errors))
    for _, ex, _ in loaded:
        try: await ex.close()
        except Exception: pass

asyncio.run(main())
