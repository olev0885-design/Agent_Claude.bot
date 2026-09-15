# =============================================================================
# tools_status.py — состояние бота одной командой, БЕЗ тихих сбоев.
# =============================================================================
# Добавлено 2026-09-15 после того, как проверочный однострочник PowerShell в
# двух часовых отчётах подряд показал «в учёте: пусто» при реально открытой
# позиции T: ошибка чтения JSON ушла в поток ошибок, а пустой результат
# выглядел как «позиций нет». Здесь учёт читается тем же кодом, что и у бота
# (position_store), сверяется с реальными позициями на биржах, и любая
# ошибка печатается как ошибка — «нет данных» никогда не выглядит как «пусто».
#
# Запуск: venv/Scripts/python.exe tools_status.py [--no-exchanges]
# =============================================================================
import asyncio
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(ROOT, "src"))
from dotenv import load_dotenv  # noqa: E402

load_dotenv(os.path.join(ROOT, ".env"))

from bot_crew import position_store  # noqa: E402


def bot_processes() -> list:
    try:
        out = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-CimInstance Win32_Process -Filter \"Name like '%python%'\" | "
             "Where-Object { $_.CommandLine -like '*bot_crew.main*' } | ForEach-Object { $_.ProcessId }"],
            capture_output=True, text=True, timeout=20,
        )
        return [p for p in out.stdout.split() if p.strip().isdigit()]
    except Exception as exc:
        return [f"ошибка проверки процесса: {exc}"]


def log_summary(path: str) -> dict:
    res = {"heartbeat": None, "traceback": 0, "net_errors": 0, "trades": [], "guards": 0}
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                if "[heartbeat]" in line:
                    res["heartbeat"] = line.strip()
                if "Traceback" in line:
                    res["traceback"] += 1
                if re.search(r"ExchangeNotAvailable|NetworkError|RequestTimeout", line):
                    res["net_errors"] += 1
                if re.search(r"Открываю сделку|Закрыта #|УСЫНОВЛЕНА|НЕУЧТЁННАЯ|ПРОПАВШАЯ", line):
                    res["trades"].append(line.strip()[:140])
                if "[entry-guard]" in line or "[close-guard]" in line:
                    res["guards"] += 1
    except Exception as exc:
        res["error"] = f"{type(exc).__name__}: {exc}"
    return res


async def exchange_positions(exchanges) -> dict:
    from bot_crew.tools.trade_tool import TradeExecutionTool
    tool = TradeExecutionTool()

    async def one(name):
        try:
            ex = await tool._get_ready_client(name)
            params = {"settle": "usdt"} if name == "gate" else {}
            poss = await ex.fetch_positions(params=params)
            live = []
            for p in poss:
                c = p.get("contracts") or 0
                if c:
                    coin = (p.get("symbol") or "").split("/")[0]
                    live.append((coin, p.get("side"), c, p.get("unrealizedPnl")))
            return name, live, None
        except Exception as exc:
            return name, [], f"{type(exc).__name__}: {str(exc)[:80]}"

    return dict((n, (live, err)) for n, live, err in await asyncio.gather(*(one(e) for e in exchanges)))


def main() -> int:
    print("=== ПРОЦЕСС ===")
    pids = bot_processes()
    print("  бот:", ("PID " + ", ".join(pids)) if pids and all(p.isdigit() for p in pids) else ("НЕТ ПРОЦЕССА" if not pids else pids[0]))

    print("=== ЛОГ ===")
    ls = log_summary(os.path.join(ROOT, "listen.log"))
    if "error" in ls:
        print("  ОШИБКА чтения лога:", ls["error"])
    print("  последний heartbeat:", ls["heartbeat"] or "НЕТ")
    print(f"  Traceback: {ls['traceback']}   сетевых ошибок: {ls['net_errors']}   срабатываний guard: {ls['guards']}")
    for t in ls["trades"][-4:]:
        print("  ", t)

    print("=== УЧЁТ (position_store) ===")
    try:
        tracked = position_store.list_positions()
    except Exception as exc:
        print("  ОШИБКА чтения учёта:", type(exc).__name__, exc)
        tracked = None
    if tracked is not None:
        if not tracked:
            print("  позиций нет (файл прочитан успешно)")
        for coin, pos in tracked.items():
            print(f"  {coin}: LONG {pos.get('long_exchange')} {pos.get('long_amount_coin')} @ {pos.get('long_entry_price')} / "
                  f"SHORT {pos.get('short_exchange')} {pos.get('short_amount_coin')} @ {pos.get('short_entry_price')}"
                  f"{'  [provisional]' if pos.get('provisional') else ''}{'  [adopted]' if pos.get('adopted') else ''}")

    if "--no-exchanges" not in sys.argv:
        print("=== БИРЖИ (реальные позиции) ===")
        exchanges = [e.strip() for e in (os.getenv("TEST_BATCH_SAFE_EXCHANGES") or "").split(",") if e.strip()]
        res = asyncio.run(exchange_positions(exchanges))
        seen = set()
        for name in exchanges:
            live, err = res.get(name, ([], "нет данных"))
            if err:
                print(f"  {name:<12} ОШИБКА: {err}")
                continue
            for coin, side, c, upnl in live:
                seen.add(coin.upper())
                print(f"  {name:<12} {coin} {side} contracts={c} uPnL={upnl}")
        if tracked is not None:
            store_coins = {c.upper() for c in tracked}
            missing_on_ex = store_coins - seen
            not_in_store = seen - store_coins
            if missing_on_ex:
                print("  ⚠ в учёте есть, на биржах НЕТ:", ", ".join(sorted(missing_on_ex)))
            if not_in_store:
                print("  ⚠ на биржах есть, в учёте НЕТ:", ", ".join(sorted(not_in_store)))
            if not missing_on_ex and not not_in_store:
                print("  учёт и биржи совпадают ✓")
    return 0


if __name__ == "__main__":
    sys.exit(main())
