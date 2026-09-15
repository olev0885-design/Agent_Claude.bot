# =============================================================================
# deploy/measure_ticker_streams.py — ЗАМЕР нагрузки потоковых bid/ask по всей
# торгуемой вселенной (монеты, есть >= на 2 наших биржах). Ничего не торгует.
# Отвечает: сколько сообщений/сек и CPU съест событийное обнаружение спредов
# по стакану, если подписаться на всё. Запуск на сервере под nice.
# =============================================================================
import asyncio, os, sys, time, collections, resource
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from dotenv import load_dotenv; load_dotenv(os.path.join(ROOT, ".env"))
import ccxt.pro as ccxtpro
from bot_crew.tools.trade_tool import EXCHANGE_ALIASES, _build_symbol

EX = [e.strip() for e in (os.getenv("TEST_BATCH_SAFE_EXCHANGES") or "").split(",") if e.strip()]
DURATION = float(sys.argv[1]) if len(sys.argv) > 1 else 90.0
BATCH = 50  # символов на один вызов watch_bids_asks для бирж без «все разом»

counts = collections.Counter(); last_ts = {}; lat_samples = collections.defaultdict(list); errors = collections.Counter()

async def load(n):
    ex = getattr(ccxtpro, EXCHANGE_ALIASES.get(n, n))({"options": {"defaultType": "swap"}, "enableRateLimit": True})
    await ex.load_markets()
    coins = {m["base"]: m["symbol"] for m in ex.markets.values() if m.get("swap") and m.get("linear") and m.get("active", True)}
    return n, ex, coins

async def pump(n, ex, symbols, stop_at):
    method = "watch_bids_asks" if ex.has.get("watchBidsAsks") else "watch_tickers"
    while time.monotonic() < stop_at:
        try:
            data = await asyncio.wait_for(getattr(ex, method)(symbols) if symbols else getattr(ex, method)(), timeout=30)
            now = time.time() * 1000
            items = data.values() if isinstance(data, dict) else [data]
            for t in items:
                counts[n] += 1
                ts = t.get("timestamp")
                if ts: lat_samples[n].append(now - ts)
        except asyncio.TimeoutError:
            errors[n + ":timeout"] += 1
        except Exception as e:
            errors[n + ":" + type(e).__name__] += 1
            await asyncio.sleep(2)

async def main():
    loaded = await asyncio.gather(*(load(n) for n in EX))
    presence = collections.Counter(c for _, _, coins in loaded for c in coins)
    universe = {c for c, k in presence.items() if k >= 2}
    print(f"вселенная: {len(universe)} монет на >=2 биржах")
    stop_at = time.monotonic() + DURATION
    tasks = []
    plan = []
    for n, ex, coins in loaded:
        syms = [coins[c] for c in coins if c in universe]
        all_at_once = n in ("mexc", "hyperliquid", "binance")
        if all_at_once:
            plan.append(f"  {n:<12} {len(syms):>4} монет — один поток на все")
            tasks.append(pump(n, ex, None, stop_at))
        else:
            batches = [syms[i:i+BATCH] for i in range(0, len(syms), BATCH)]
            plan.append(f"  {n:<12} {len(syms):>4} монет — {len(batches)} пакетов по {BATCH}")
            for b in batches:
                tasks.append(pump(n, ex, b, stop_at))
    print("\n".join(plan)); print(f"\nзамер {DURATION:.0f}с...", flush=True)
    cpu0 = time.process_time(); t0 = time.monotonic()
    await asyncio.gather(*tasks, return_exceptions=True)
    wall = time.monotonic() - t0; cpu = time.process_time() - cpu0
    print(f"\n=== РЕЗУЛЬТАТ за {wall:.0f}с ===")
    tot = 0
    for n in EX:
        c = counts[n]; tot += c
        lats = sorted(lat_samples[n]); med = lats[len(lats)//2] if lats else float("nan")
        print(f"  {n:<12} {c/wall:8.1f} сообщ/с   задержка биржа->мы (медиана) {med:6.0f} мс")
    print(f"  {'ИТОГО':<12} {tot/wall:8.1f} сообщ/с")
    print(f"  CPU процесса: {cpu/wall*100:.0f}% одного ядра   RSS: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024:.0f} MB")
    if errors: print("  ошибки:", dict(errors))
    for _, ex, _ in loaded:
        try: await ex.close()
        except Exception: pass

asyncio.run(main())
