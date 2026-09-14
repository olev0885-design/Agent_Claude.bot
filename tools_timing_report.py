"""Отчёт по задержкам ПО РЕАЛЬНЫМ сделкам из журнала.

Создан 2026-09-14 после разбора с пользователем: чтобы измерить скорость
входа, я запустил отдельную тестовую сделку — и она пошла в обход сканера,
без готового стакана. Числа получились не про боевой путь, а про стенд,
плюс реальные $0.14 на комиссии и проскальзывание.

Правильный способ — здесь: бот и так пишет разбивку по каждой настоящей
сделке. Смотреть надо сюда, а не устраивать синтетику.

Запуск:  venv/Scripts/python.exe tools_timing_report.py
"""
import io
import statistics as st
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, "src")

from bot_crew import trade_ledger  # noqa: E402


def _fmt(values):
    if not values:
        return "нет данных"
    return f"медиана {st.median(values):6.0f}мс   мин {min(values):5.0f}   макс {max(values):5.0f}   (n={len(values)})"


def main():
    rows = [t for t in trade_ledger.read_all() if t.get("timings")]
    if not rows:
        print("В журнале ещё нет сделок с разбивкой задержек.")
        return

    opens = [t for t in rows if t.get("event") == "open"]
    closes = [t for t in rows if t.get("event") != "open"]

    # Разделяем боевой путь (стакан пришёл готовым из сканера) и всё
    # остальное — смешивать их в одной статистике и есть та самая ошибка.
    live = [t for t in opens if t["timings"].get("book_snapshot_reused")]
    synthetic = [t for t in opens if not t["timings"].get("book_snapshot_reused")]

    def col(items, key):
        return [t["timings"][key] for t in items if t["timings"].get(key) is not None]

    print("=== ОТКРЫТИЕ — БОЕВОЙ ПУТЬ (стакан готов из сканера) ===")
    if live:
        print(f"  стакан+баланс : {_fmt(col(live,'book_snapshot_ms'))}")
        print(f"  проверки      : {_fmt(col(live,'pre_order_checks_ms'))}")
        print(f"  ордера        : {_fmt(col(live,'orders_ms'))}")
        print(f"  комиссии      : {_fmt(col(live,'fee_lookup_ms'))}")
        print(f"  ВСЕГО         : {_fmt(col(live,'total_ms'))}")
    else:
        print("  пока нет — появится после первой сделки, открытой сканером")

    if synthetic:
        print("\n=== ОТКРЫТИЕ — В ОБХОД СКАНЕРА (не показатель) ===")
        print(f"  ВСЕГО         : {_fmt(col(synthetic,'total_ms'))}")
        print("  ^ стакан здесь запрашивался на месте (~1.5с), боевой вход так не делает")

    print("\n=== ЗАКРЫТИЕ ===")
    print(f"  ордера        : {_fmt(col(closes,'orders_ms'))}")
    print(f"  комиссии      : {_fmt(col(closes,'fee_lookup_ms'))}")
    print(f"  ВСЕГО         : {_fmt(col(closes,'total_ms'))}")


if __name__ == "__main__":
    main()
