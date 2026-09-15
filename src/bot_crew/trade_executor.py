# =============================================================================
# trade_executor.py — детерминированное (без LLM) открытие/закрытие спреда
# по УЖЕ СТРУКТУРИРОВАННЫМ данным (coin, long_exchange, short_exchange).
# =============================================================================
# Вынесено из main.py в отдельный модуль, чтобы этой же логикой мог
# пользоваться не только Telegram-листенер (main.py: open_signal/close_signal
# сначала парсят текст сигнала через LLM, потом вызывают функции отсюда),
# но и scanner.py (FundingScanner находит связку САМ через CCXT — у него
# уже есть готовые coin/long_exchange/short_exchange, LLM ему для этого не
# нужен вовсе). Общий код здесь предотвращает дублирование логики учёта
# позиций/комиссий/уведомлений между двумя источниками сигналов.
# =============================================================================
import asyncio
from datetime import datetime, timezone
from typing import Optional

from bot_crew.tools.trade_tool import TradeExecutionTool
from bot_crew import position_store
from bot_crew import trade_ledger


# =============================================================================
# Форматирование комиссий (taker) в отчётах — реальная ставка берётся с
# биржи через trade_tool._get_taker_fee_rate (см. там подробный комментарий,
# почему именно taker, а не maker/усреднённые цифры "из интернета").
# =============================================================================
def _format_fee(leg: dict) -> str:
    rate = leg.get("taker_fee_rate")
    fee = leg.get("fee_usdt")
    if rate is None:
        return "ставка неизвестна (не удалось получить с биржи)"
    fee_str = f"{fee:.4f} USDT" if fee is not None else "сумма неизвестна"
    return f"taker {rate * 100:.3f}% ({fee_str})"


def _sum_fees(*legs: dict):
    """Суммирует fee_usdt по нескольким ногам; None, если хотя бы для
    одной ноги комиссию не удалось узнать (лучше явно показать "неизвестно",
    чем тихо занизить сумму, пропустив неизвестное слагаемое)."""
    return _sum_fees_values(*(leg.get("fee_usdt") for leg in legs))


def _sum_fees_values(*fees):
    """То же самое, но принимает уже готовые числа (например, значения,
    сохранённые в position_store при открытии), а не словари ног."""
    fees = list(fees)
    if any(f is None for f in fees):
        return None
    return sum(fees)


# =============================================================================
# OPEN — открытие спреда по уже готовым (coin, long_exchange, short_exchange).
# =============================================================================
# ВАЖНО: здесь СОЗНАТЕЛЬНО нет никакого LLM — цену входа, объём и order_id
# для реальных денег надёжнее брать напрямую из фактического ответа CCXT
# (TradeExecutionTool.open_spread), а не из текста, сгенерированного языковой
# моделью. main.py.open_signal() парсит сырой текст сигнала через LLM и
# передаёт сюда уже структурные coin/long_exchange/short_exchange; scanner.py
# передаёт их напрямую, тоже без всякого LLM.
# =============================================================================
def open_structured_signal(
    coin: str,
    long_exchange: str,
    short_exchange: str,
    spread_percent=None,
    notifier=None,
    amount_usdt=None,
    extra_position_fields: dict = None,
) -> str:
    """Открывает обе ноги спреда и ставит позицию на учёт (position_store)
    для последующего закрытия. Возвращает текстовый отчёт.

    spread_percent — только для отображения в мгновенном уведомлении
    (necessary поле сигнала может отсутствовать — тогда notifier покажет
    "н/д"), на саму торговую логику не влияет.

    amount_usdt — переопределяет размер ОДНОЙ ноги в USDT (по умолчанию —
    общий TRADE_SIZE_USDT из .env, см. trade_tool.py). Используется, например,
    scanner.py, у которого свой отдельный размер сделки (AUTO_TRADE_AMOUNT_USDT).

    extra_position_fields (добавлено 2026-09-12, реальный случай ANTHROPIC
    — см. _finalize_open) — произвольные дополнительные поля, которые
    нужно сохранить в position_store ВМЕСТЕ с позицией (например,
    "wide_spread": True/False у scanner.py) — иначе такие метки живут
    ТОЛЬКО в памяти (test_batch.TestBatchPosition) и теряются при любом
    рестарте бота, когда позиция восстанавливается заново из файла.

    notifier (опционально) — если передан, СРАЗУ после того, как ордера
    реально отправлены на биржи (result уже получен от tool.open_spread),
    шлёт мгновенное Telegram-уведомление — ДО дальнейшей обработки (расчёт
    суммарной комиссии, запись в position_store), которая теоретически
    может упасть с ошибкой уже ПОСЛЕ того, как сделка реально совершена."""
    tool = TradeExecutionTool()
    result = tool.open_spread(coin, long_exchange, short_exchange, amount_usdt=amount_usdt)
    return _finalize_open(result, spread_percent, notifier, extra_position_fields)


async def open_structured_signal_async(
    coin: str,
    long_exchange: str,
    short_exchange: str,
    spread_percent=None,
    notifier=None,
    amount_usdt=None,
    extra_position_fields: dict = None,
    prefetched_books=None,
    prefetched_books_at=None,
) -> str:
    """То же самое, что open_structured_signal(), но НАПРЯМУЮ await'ит
    TradeExecutionTool._open_both_legs_async() вместо tool.open_spread()
    (синхронная обёртка через _run_coro_blocking — та же двойная петля
    "поток -> run_coroutine_threadsafe -> обратно на loop бота", что была
    у close_spread() до 2026-09-10, см. close_structured_signal_detailed_async).

    По прямой просьбе пользователя 2026-09-11 ("как ускорить открытие и
    закрытие ног") — вызывающий код (scanner.py: _trigger_trade/
    _trigger_test_batch_trade) сам УЖЕ работает на event loop бота, поэтому
    вместо run_in_executor(execute_arbitrage_trade) (создаёт НОВЫЙ поток +
    внутри него ещё и planning корутины обратно на loop бота через
    run_coroutine_threadsafe, с блокировкой этого потока в ожидании) — тот
    же самый event loop просто await'ит корутину напрямую. Экономит
    создание потока и одно лишнее переключение контекста между вызовом и
    реальной отправкой ордеров открытия на биржи (тот же выигрыш, что
    закрытие уже получило раньше)."""
    tool = TradeExecutionTool()
    result = await tool._open_both_legs_async(
        coin, long_exchange, short_exchange, amount_usdt=amount_usdt,
        prefetched_books=prefetched_books, prefetched_books_at=prefetched_books_at,
    )
    return _finalize_open(result, spread_percent, notifier, extra_position_fields)


def _finalize_open(result: dict, spread_percent, notifier, extra_position_fields: dict = None) -> str:
    """Общая часть open_structured_signal()/_async() ПОСЛЕ того, как result
    (dict от _open_both_legs_async, синхронно или через await) уже получен
    — уведомление, отчёт, запись в position_store. Вынесено в отдельную
    функцию по тому же принципу, что и _finalize_close (не дублировать
    ~150 строк форматирования/учёта между sync- и async-вариантами)."""
    # _open_both_legs_async() может ПОМЕНЯТЬ long_exchange/short_exchange
    # МЕСТАМИ (см. "ПРОВЕРКА НАПРАВЛЕНИЯ ВХОДА" в
    # trade_tool.py:_open_both_legs_async) — если к моменту исполнения
    # long-биржа сигнала оказалась ДОРОЖЕ short-биржи (реальный случай с
    # XRP 2026-09-06: канал назвал LONG биржу, ставшую дороже — вошли бы с
    # отрицательным спредом). Ниже ВСЕГДА ориентируемся на то, что РЕАЛЬНО
    # произошло (result), а не на исходные аргументы сигнала — иначе
    # отчёт/уведомление/position_store подпишут ноги под неверную биржу.
    # Ключей может не быть в cancelled-ветке (сделка отменена ДО того, как
    # _open_both_legs_async успела определить финальное направление) —
    # там они и не нужны, cancelled-ответ ниже их не использует.
    long_exchange = result.get("long_exchange")
    short_exchange = result.get("short_exchange")

    if result["cancelled"]:
        # ХОЛОСТОЙ КРУГ В ЖУРНАЛ (добавлено 2026-09-15, случай LSK): если
        # обе ноги успели исполниться и были закрыты сразу (проверка
        # фактического спреда), это РЕАЛЬНЫЕ деньги — комиссии за четыре
        # ордера и пересечение двух стаканов (~−0.10 USDT на $25). Раньше
        # такой круг не попадал ни в журнал, ни в дневной отчёт — потери
        # были невидимы для статистики. Записываем честно, что знаем.
        try:
            long_leg = result.get("long") or {}
            short_leg = result.get("short") or {}
            lrb, srb = long_leg.get("rollback") or {}, short_leg.get("rollback") or {}
            if long_leg.get("price") and short_leg.get("price") and lrb.get("price") and srb.get("price"):
                la = long_leg.get("amount_coin") or 0
                sa = short_leg.get("amount_coin") or 0
                long_pnl = la * (lrb["price"] - long_leg["price"])
                short_pnl = sa * (short_leg["price"] - srb["price"])
                fees = sum(x for x in (
                    long_leg.get("fee_usdt"), short_leg.get("fee_usdt"), lrb.get("fee_usdt"), srb.get("fee_usdt")
                ) if x) or None
                trade_ledger.record_trade({
                    "event": "close",
                    "coin": result["coin"],
                    "long_exchange": long_exchange, "short_exchange": short_exchange,
                    "long_entry_price": long_leg["price"], "short_entry_price": short_leg["price"],
                    "long_exit_price": lrb["price"], "short_exit_price": srb["price"],
                    "long_amount_coin": la, "short_amount_coin": sa,
                    "amount_usdt": long_leg.get("amount_usdt"),
                    "long_pnl": round(long_pnl, 6), "short_pnl": round(short_pnl, 6),
                    "gross_pnl": round(long_pnl + short_pnl, 6),
                    "net_pnl": round(long_pnl + short_pnl - (fees or 0.0), 6),
                    "fees_known": fees is not None,
                    "close_reason": f"холостой круг: {result['reason']}",
                    "opened_at": datetime.now(timezone.utc).isoformat(),
                    "closed_at": datetime.now(timezone.utc).isoformat(),
                    "holding_seconds": 0,
                    "cancelled_round_trip": True,
                })
        except Exception as exc:
            print(f"[trade_ledger] не удалось записать холостой круг {result.get('coin')}: {exc}")
        if notifier:
            notifier.notify_error(
                symbol=result["coin"],
                error_message=f"Сделка не открыта ни на одной ноге: {result['reason']}",
            )
        return f"Монета: {result['coin']}\nСделка НЕ открыта: {result['reason']}"

    long_leg, short_leg = result["long"], result["short"]
    ok_statuses = ("OK", "DRY_RUN_OK")
    long_ok = long_leg["status"] in ok_statuses
    short_ok = short_leg["status"] in ok_statuses
    both_ok = long_ok and short_ok

    # УЧЁТ — ДО уведомлений и отчёта (перенесено сюда 2026-09-15 после
    # инцидента BONER: раньше запись стояла в самом конце, ПОСЛЕ сборки
    # отчёта и отправки в Telegram, и любое исключение по дороге — хоть
    # форматирование числа — оставляло реально открытые ноги без учёта).
    # trade_tool уже положил предварительную запись сразу после исполнения
    # (см. _persist_provisional_position); здесь она ПЕРЕЗАПИСЫВАЕТСЯ полной
    # версией — с комиссиями и флагами (wide_spread и т.п.).
    if both_ok:
        try:
            position_fields = {
                "coin": result["coin"],
                "long_exchange": long_exchange,
                "short_exchange": short_exchange,
                "long_amount_coin": long_leg.get("amount_coin"),
                "short_amount_coin": short_leg.get("amount_coin"),
                "long_entry_price": long_leg.get("price"),
                "short_entry_price": short_leg.get("price"),
                "amount_usdt": long_leg.get("amount_usdt"),
                "leverage": result.get("leverage"),
                "long_order_id": long_leg.get("order_id"),
                "short_order_id": short_leg.get("order_id"),
                "long_entry_fee_usdt": long_leg.get("fee_usdt"),
                "short_entry_fee_usdt": short_leg.get("fee_usdt"),
                "long_taker_fee_rate": long_leg.get("taker_fee_rate"),
                "short_taker_fee_rate": short_leg.get("taker_fee_rate"),
            }
            # См. docstring extra_position_fields — реальный случай
            # 2026-09-12 (ANTHROPIC): без этого "wide_spread": True терялся
            # при каждом рестарте бота.
            if extra_position_fields:
                position_fields.update(extra_position_fields)
            position_store.record_open(result["coin"], position_fields)
        except Exception as exc:
            # Предварительная запись от trade_tool уже лежит в учёте —
            # позиция не потеряна, просто без комиссий. Кричим, не падаем.
            print(f"[position-store] {result['coin']}: полная запись не удалась ({type(exc).__name__}: {exc}) — остаётся предварительная.")
    # Ни одна нога не открылась вообще (реальный случай 2026-09-12:
    # MICRODUCK — обе ноги отклонены биржами: mexc "contract not
    # activated", gate "insufficient margin") — откатывать НЕЧЕГО, деньги
    # не потрачены, риска нет. В ОТЛИЧИЕ от случая "одна нога открылась, а
    # rollback второй не удался" (см. ниже), это безопасный исход, и
    # тревожная формулировка "ТРЕБУЕТСЯ РУЧНАЯ ПРОВЕРКА" тут вводила в
    # заблуждение — исправлено по прямой обратной связи пользователя.
    neither_opened = not long_ok and not short_ok

    # --- Мгновенное уведомление — сразу после исполнения ордеров -----------
    if notifier:
        try:
            if both_ok:
                notifier.notify_open(
                    symbol=result["coin"],
                    long_exchange=long_exchange,
                    long_price=long_leg.get("price"),
                    short_exchange=short_exchange,
                    short_price=short_leg.get("price"),
                    spread=spread_percent,
                    elapsed_ms=result.get("elapsed_ms"),
                )
            else:
                rollback = long_leg.get("rollback") or short_leg.get("rollback")
                if rollback:
                    rollback_note = f" Открывшаяся нога автоматически закрыта (rollback: {rollback['status']})."
                elif neither_opened:
                    rollback_note = " Ни одна нога не открылась — деньги не потрачены, откатывать нечего, действие не требуется."
                else:
                    rollback_note = " ⚠️ Одна нога открылась, а автоматический откат не удался — проверьте вручную!"
                notifier.notify_error(
                    symbol=result["coin"],
                    error_message=(
                        f"LONG {long_exchange}: {long_leg['status']}; "
                        f"SHORT {short_exchange}: {short_leg['status']} — "
                        f"не все ноги открылись.{rollback_note}"
                    ),
                )
        except Exception as exc:
            # Сбой самого уведомления НЕ должен мешать сформировать
            # текстовый отчёт ниже — торговая логика уже отработала.
            print(f"[notifier] сбой при формировании уведомления об открытии: {exc}")

    report_lines = [
        f"Монета: {result['coin']}",
        f"LONG на {long_exchange}: {long_leg['status']} "
        f"(order_id={long_leg['order_id']}, цена={long_leg['price']}, "
        f"комиссия за вход: {_format_fee(long_leg)})",
        f"SHORT на {short_exchange}: {short_leg['status']} "
        f"(order_id={short_leg['order_id']}, цена={short_leg['price']}, "
        f"комиссия за вход: {_format_fee(short_leg)})",
        f"Задержка между ногами: {result['elapsed_ms']} мс",
    ]

    entry_fee_total = _sum_fees(long_leg, short_leg)
    if entry_fee_total is not None:
        invested = (long_leg.get("amount_usdt") or 0) * 2
        pct = (entry_fee_total / invested * 100) if invested else None
        report_lines.append(
            f"Суммарная комиссия за вход: {entry_fee_total:.4f} USDT"
            + (f" ({pct:.2f}% от {invested:.2f} USDT вложенных)" if pct is not None else "")
        )

    if both_ok:
        # Запись в position_store уже сделана ВЫШЕ (сразу после both_ok) —
        # здесь остаётся только журнал и отчёт.

        # ЗАПИСЬ "OPEN" В ЖУРНАЛ (добавлено 2026-09-12, по прямой просьбе
        # пользователя — ежедневный отчёт "задержка на ноги + PnL за
        # сутки", см. trade_ledger.daily_stats/scanner.py:_daily_report_
        # loop) — раньше журнал писал ТОЛЬКО закрытые сделки; задержка при
        # ОТКРЫТИИ (result["elapsed_ms"] — "Задержка между ногами" в
        # текстовом отчёте ниже) нигде не сохранялась персистентно, только
        # мелькала в консоли/Telegram и терялась. Сбой самой записи не
        # должен мешать открытой сделке — trade_ledger.record_trade сам
        # никогда не бросает исключение наружу.
        trade_ledger.record_trade({
            "event": "open",
            "coin": result["coin"],
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "elapsed_ms": result.get("elapsed_ms"),
            # Разбивка задержки по этапам (стакан/проверки/ордера/комиссии)
            # — см. timings в trade_tool.py:_open_both_legs_async.
            "timings": result.get("timings"),
            "trigger_source": (extra_position_fields or {}).get("trigger_source"),
            "opened_at": datetime.now(timezone.utc).isoformat(),
        })
        report_lines.append(
            '✅ Позиция поставлена на учёт — будет закрыта по сигналу '
            '"aligned in" из канала (или сканером, если спред сойдётся).'
        )
    else:
        # Одна из ног не открылась (несмотря на pre-check в trade_tool) —
        # например, биржа отклонила ордер по причине, непредсказуемой
        # заранее (лимит объёма/плеча на конкретный символ, требование
        # подписать соглашение и т.п.). trade_tool._open_both_legs_async
        # уже САМ попытался автоматически закрыть открывшуюся ногу
        # (rollback) — см. поле "rollback" внутри long_leg/short_leg.
        rollback = long_leg.get("rollback") or short_leg.get("rollback")
        if rollback:
            report_lines.append(
                f"✅ Не все ноги открылись успешно, но открывшаяся нога "
                f"АВТОМАТИЧЕСКИ закрыта (rollback): {rollback['status']} "
                f"(order_id={rollback.get('order_id')}). Незахеджированной позиции не осталось."
            )
        elif neither_opened:
            report_lines.append(
                "Ни одна нога не открылась ни на одной бирже — деньги не потрачены, "
                "откатывать нечего, наличие позиции проверять не нужно."
            )
        else:
            report_lines.append(
                "⚠️ ВНИМАНИЕ: одна нога открылась, а автоматический откат "
                "второй не удался (или не удалось определить объём) — "
                "проверьте вручную на биржах!"
            )

    return "\n".join(report_lines)


# =============================================================================
# CLOSE — закрытие позиции по монете.
# =============================================================================
# Здесь СОЗНАТЕЛЬНО нет никакого LLM — детерминированная логика (найти
# позицию по тикеру, закрыть обе ноги, посчитать PnL арифметикой) не
# должна зависеть от вероятностной генерации текста, особенно когда на
# кону реальные деньги и точный расчёт прибыли/убытка.
# =============================================================================
def close_structured_signal(coin: str, notifier=None, reason: str = None) -> str:
    """Закрывает позицию по монете — тонкая обёртка над
    close_structured_signal_detailed() для вызывающего кода, которому нужен
    только текстовый отчёт (main.py:close_signal). Возвращает None, если
    по этой монете позиции нет (сигнал не про нашу сделку)."""
    result = close_structured_signal_detailed(coin, notifier=notifier, reason=reason)
    return result["report"] if result is not None else None


def close_structured_signal_detailed(coin: str, notifier=None, reason: str = None) -> Optional[dict]:
    """То же самое, что close_structured_signal(), но возвращает СТРУКТУРНЫЙ
    словарь (report/both_ok/position/long_leg/short_leg/gross_pnl/net_pnl/…)
    вместо готового текста — нужен вызывающему коду, которому важны сами
    числа PnL, а не только человеко-читаемый отчёт (например, test_batch.py:
    финальная сводка по серии тестовых сделок считает средний PnL/holding
    time/win-rate по РЕАЛЬНЫМ числам, а не парсит их обратно из текста).
    Возвращает None, если по этой монете позиции нет.

    reason — человекочитаемая причина закрытия для уведомления (например,
    'сигнал "aligned in" из канала' или 'спред сошёлся (сканер рынка)');
    по умолчанию — общая формулировка.

    notifier — см. open_structured_signal(): шлёт мгновенный алерт сразу
    после закрытия ордеров, до финального форматирования текстового отчёта."""
    position = position_store.get_position(coin)
    if not position:
        return None

    dry_run_result = _pop_dry_run_position(coin, position)
    if dry_run_result is not None:
        return dry_run_result

    tool = TradeExecutionTool()
    close_result = tool.close_spread(
        coin,
        position["long_exchange"],
        position["short_exchange"],
        position["long_amount_coin"],
        position["short_amount_coin"],
    )
    return _finalize_close(coin, position, close_result, notifier, reason)


async def _verify_close_still_worth_it(coin: str, position: dict, min_net_pnl: float) -> "tuple[bool, float | None]":
    """ФИНАЛЬНАЯ проверка ПРЯМО ПЕРЕД отправкой ордеров закрытия: пересчитывает
    чистый PnL по ЖИВОМУ стакану (VWAP на реальный объём позиции) и говорит,
    стоит ли закрываться ПРЯМО СЕЙЧАС. Возвращает (стоит_ли, пересчитанный_net_pnl).

    Добавлено 2026-09-13 по прямой просьбе пользователя после разбора реальной
    сделки STORJ: решение о закрытии принималось при спреде 0.99%, а РЕАЛЬНЫЕ
    цены исполнения дали спред 3.28% — монета в тот момент падала на 12%, и
    книги gate/mexc разъехались за те ~1.4с, что шло исполнение. Итог -$0.05
    вместо ожидаемой прибыли.

    Ассиметрия, которую это закрывает: у ОТКРЫТИЯ такой гейт есть давно (см.
    "ПРОВЕРКА АКТУАЛЬНОСТИ СПРЕДА" в trade_tool.py:_open_both_legs_async —
    сделка отменяется ДО отправки ордеров, если спред ушёл), а у ЗАКРЫТИЯ не
    было ничего: решили -> сразу шлём рыночные ордера, что бы ни стало с ценой.

    ВАЖНО — fail-OPEN: если стакан получить не удалось (сеть, пустая книга,
    нет цен входа), возвращаем True (закрываемся). Позиция, которая НЕ МОЖЕТ
    закрыться из-за сбоя этой проверки — хуже, чем закрытие по чуть худшей
    цене: висящая позиция копит риск неограниченно. Гейт нужен против
    ПРЕДСКАЗУЕМО плохого исполнения, а не против любой неопределённости."""
    long_entry = position.get("long_entry_price")
    short_entry = position.get("short_entry_price")
    long_amount = position.get("long_amount_coin")
    short_amount = position.get("short_amount_coin")
    amount_usdt = position.get("amount_usdt")
    if not long_entry or not short_entry or not long_amount or not short_amount or not amount_usdt:
        return True, None  # нечем считать — не блокируем закрытие (см. fail-open выше)

    tool = TradeExecutionTool()
    try:
        long_snapshot, short_snapshot = await asyncio.gather(
            tool._get_book_snapshot(position["long_exchange"], coin, amount_usdt),
            tool._get_book_snapshot(position["short_exchange"], coin, amount_usdt),
        )
    except Exception as exc:
        print(f"[close-check] {coin}: не удалось получить стакан ({type(exc).__name__}: {exc}) — закрываю без проверки.")
        return True, None
    if not long_snapshot or not short_snapshot:
        return True, None

    # LONG закрывается ПРОДАЖЕЙ (walk по bids -> sell_vwap), SHORT —
    # ПОКУПКОЙ (walk по asks -> buy_vwap). Та же логика, что и в
    # scanner.py:check_close_vwap, только на секунду позже — прямо перед
    # самой отправкой ордеров.
    long_exit = long_snapshot.get("sell_vwap")
    short_exit = short_snapshot.get("buy_vwap")
    if not long_exit or not short_exit:
        return True, None

    gross = (long_exit - long_entry) * long_amount + (short_entry - short_exit) * short_amount
    entry_fee_total = _sum_fees_values(
        position.get("long_entry_fee_usdt"), position.get("short_entry_fee_usdt")
    )
    # Комиссию за ВЫХОД оцениваем той же суммой, что и за вход (тот же
    # объём/биржи) — тот же приём, что и в scanner.py:_estimate_total_pnl.
    net = gross - entry_fee_total * 2 if entry_fee_total is not None else gross
    return net > min_net_pnl, net


async def close_structured_signal_detailed_async(
    coin: str, notifier=None, reason: str = None, min_net_pnl: "float | None" = None
) -> Optional[dict]:
    """То же самое, что close_structured_signal_detailed(), но НАПРЯМУЮ
    await'ит TradeExecutionTool._close_both_legs_async() вместо
    tool.close_spread() (синхронная обёртка, которая внутри либо создаёт
    ОДНОРАЗОВЫЙ event loop через asyncio.run(), либо — если вызывается уже
    ИЗ потока, отличного от event loop бота — планирует корутину обратно
    на loop бота через run_coroutine_threadsafe() и БЛОКИРУЕТ текущий
    поток в ожидании результата).

    По прямой просьбе пользователя 2026-09-10 ("нагружай максимально,
    если поможет — сделай параллельным, чтобы закрывались по нужной
    цене") — вызывающий код (scanner.py: _close_test_batch_position) сам
    УЖЕ работает на event loop бота (никакой отдельный поток/executor не
    нужен), поэтому естественный await эту двойную петлю "поток ->
    run_coroutine_threadsafe -> обратно на loop бота" полностью убирает —
    экономит переключения контекста между вызовом и реальной отправкой
    ордеров закрытия на биржи."""
    position = position_store.get_position(coin)
    if not position:
        return None

    dry_run_result = _pop_dry_run_position(coin, position)
    if dry_run_result is not None:
        return dry_run_result

    # min_net_pnl (добавлено 2026-09-13) — если передан, ПРЯМО ПЕРЕД
    # отправкой ордеров пересчитываем PnL по живому стакану и отменяем
    # закрытие, если прибыль успела испариться (см.
    # _verify_close_still_worth_it, реальный случай STORJ). Передаёт его
    # только ДОБРОВОЛЬНОЕ закрытие "по прибыли" из scanner.py — вынужденные
    # закрытия (сигнал канала, reconcile) его НЕ передают и работают как
    # раньше: позицию, которую надо закрыть по внешней причине, эта
    # проверка блокировать не должна.
    if min_net_pnl is not None:
        worth_it, fresh_net = await _verify_close_still_worth_it(coin, position, min_net_pnl)
        if not worth_it:
            print(
                f"[close-check] {coin}: ОТМЕНЯЮ закрытие — по свежему стакану чистый PnL "
                f"{fresh_net:+.4f} USDT (порог {min_net_pnl:+.4f}). Позиция остаётся открытой, "
                f"следующий цикл мониторинга проверит снова."
            )
            # both_ok=False -> вызывающий код (scanner.py:_close_test_batch_
            # position) оставляет позицию в мониторинге и попробует снова
            # на следующем цикле. Уведомление НЕ шлём: это не ошибка, а
            # штатное "ещё не время".
            return {
                "report": f"Монета: {coin}\nЗакрытие отменено: прибыль не подтвердилась по свежему стакану.",
                "both_ok": False,
                "close_aborted": True,
                "fresh_net_pnl": fresh_net,
                "position": position,
                "long_leg": None,
                "short_leg": None,
                "long_pnl": None,
                "short_pnl": None,
                "gross_pnl": None,
                "gross_pnl_pct": None,
                "net_pnl": None,
                "net_pnl_pct": None,
            }

    tool = TradeExecutionTool()
    close_result = await tool._close_both_legs_async(
        coin,
        position["long_exchange"],
        position["short_exchange"],
        position["long_amount_coin"],
        position["short_amount_coin"],
    )
    return _finalize_close(coin, position, close_result, notifier, reason)


def _pop_dry_run_position(coin: str, position: dict) -> Optional[dict]:
    """Общая ветка для close_structured_signal_detailed()/_async(): позиция
    была открыта в DRY_RUN без реального объёма в монете — физически
    закрывать нечего, просто снимаем с учёта. Возвращает готовый словарь-
    отчёт, либо None, если это НЕ DRY_RUN-случай (вызывающий код должен
    продолжить обычным закрытием)."""
    if position.get("long_amount_coin") is not None and position.get("short_amount_coin") is not None:
        return None
    # both_ok=True (не False!): с точки зрения вызывающего кода (см.
    # scanner.py:_close_test_batch_position) это УСПЕШНОЕ завершение —
    # позиция снята с учёта, повторять закрытие уже нечего и незачем.
    # both_ok=False там означает "нужно попробовать ещё раз следующим
    # циклом" — а тут пробовать больше нечего в принципе.
    position_store.pop_position(coin)
    return {
        "report": (
            f"Монета: {coin}\n"
            f"Получен сигнал закрытия, но позиция была открыта в DRY_RUN "
            f"(нет реального объёма в монете) — снята с учёта без реального "
            f"закрытия ордеров."
        ),
        "both_ok": True,
        "position": position,
        "long_leg": None,
        "short_leg": None,
        "long_pnl": None,
        "short_pnl": None,
        "gross_pnl": None,
        "gross_pnl_pct": None,
        "net_pnl": None,
        "net_pnl_pct": None,
    }


def _finalize_close(coin: str, position: dict, close_result: dict, notifier=None, reason: str = None) -> dict:
    """Общая часть close_structured_signal_detailed()/_async() ПОСЛЕ того,
    как close_result (dict от _close_both_legs_async, синхронно или через
    await) уже получен — снятие с учёта, расчёт PnL, уведомление, отчёт.
    Вынесено в отдельную функцию, чтобы sync- и async-варианты не
    дублировали ~150 строк форматирования/PnL-логики."""
    long_leg, short_leg = close_result["long"], close_result["short"]
    ok_statuses = ("OK", "DRY_RUN_OK")
    both_ok = long_leg["status"] in ok_statuses and short_leg["status"] in ok_statuses

    # Снимаем с учёта ТОЛЬКО если обе ноги закрылись — если что-то пошло
    # не так, позиция остаётся в учёте (лучше "зависшая" запись, которую
    # видно, чем незаметно потерянный риск на реальные деньги).
    if both_ok:
        position_store.pop_position(coin)

    pnl_line = "PnL (до комиссий): недоступен (нет реальных цен входа/выхода — DRY_RUN или ошибка ноги)"
    net_pnl_line = None
    # Инициализация на случай, если ветка ниже не выполнится (нет цен) —
    # чтобы блок уведомления после неё мог безопасно на них сослаться.
    total_pnl = pnl_pct = net_pnl = net_pnl_pct = None
    long_pnl = short_pnl = None
    if (
        long_leg.get("price") and short_leg.get("price")
        and position.get("long_entry_price") and position.get("short_entry_price")
    ):
        # LONG зарабатывает, когда цена ВЫРОСЛА; SHORT — когда УПАЛА.
        long_pnl = (long_leg["price"] - position["long_entry_price"]) * position["long_amount_coin"]
        short_pnl = (position["short_entry_price"] - short_leg["price"]) * position["short_amount_coin"]
        total_pnl = long_pnl + short_pnl
        invested = position["amount_usdt"] * 2  # обе ноги по amount_usdt
        pnl_pct = (total_pnl / invested * 100) if invested else None
        pnl_line = (
            f"PnL (до комиссий): {total_pnl:+.4f} USDT"
            + (f" ({pnl_pct:+.2f}% от {invested:.2f} USDT вложенных)" if pnl_pct is not None else "")
        )

        # Комиссии за весь круг (вход + выход, обе ноги, taker) — вход
        # берём из того, что записали при открытии, выход — из результата
        # закрытия только что.
        entry_fee_total = _sum_fees_values(
            position.get("long_entry_fee_usdt"), position.get("short_entry_fee_usdt")
        )
        exit_fee_total = _sum_fees(long_leg, short_leg)
        if entry_fee_total is not None and exit_fee_total is not None:
            round_trip_fee = entry_fee_total + exit_fee_total
            net_pnl = total_pnl - round_trip_fee
            net_pnl_pct = (net_pnl / invested * 100) if invested else None
            net_pnl_line = (
                f"Комиссии за весь круг (вход+выход): {round_trip_fee:.4f} USDT\n"
                f"PnL (после комиссий): {net_pnl:+.4f} USDT"
                + (f" ({net_pnl_pct:+.2f}% от {invested:.2f} USDT вложенных)" if net_pnl_pct is not None else "")
            )

    close_reason = reason or 'сигнал "aligned in" из канала'

    # --- Мгновенное уведомление — сразу после исполнения ордеров закрытия --
    if notifier:
        try:
            if both_ok:
                # Предпочитаем PnL "после комиссий" (net) как более честный
                # "итоговый" результат; если комиссии не удалось узнать —
                # используем PnL "до комиссий" (gross) как запасной вариант.
                final_pnl = net_pnl if net_pnl is not None else total_pnl
                final_pnl_pct = net_pnl_pct if net_pnl_pct is not None else pnl_pct
                notifier.notify_close(
                    symbol=coin,
                    reason=close_reason,
                    pnl_amount=final_pnl,
                    pnl_percent=final_pnl_pct,
                    elapsed_ms=close_result.get("elapsed_ms"),
                )
            else:
                notifier.notify_error(
                    symbol=coin,
                    error_message=(
                        f"LONG {position['long_exchange']}: {long_leg['status']}; "
                        f"SHORT {position['short_exchange']}: {short_leg['status']} — "
                        f"не все ноги закрылись, позиция ОСТАЛАСЬ в учёте, "
                        f"требуется ручная проверка!"
                    ),
                )
        except Exception as exc:
            print(f"[notifier] сбой при формировании уведомления о закрытии: {exc}")

    report_lines = [
        f"Монета: {coin}",
        f"Причина закрытия: {close_reason}",
        f"Вход:  LONG {position['long_exchange']}@{position.get('long_entry_price')}, "
        f"SHORT {position['short_exchange']}@{position.get('short_entry_price')}",
        f"Выход: LONG {position['long_exchange']}: {long_leg['status']} "
        f"(order_id={long_leg['order_id']}, цена={long_leg['price']}, "
        f"комиссия за выход: {_format_fee(long_leg)})",
        f"       SHORT {position['short_exchange']}: {short_leg['status']} "
        f"(order_id={short_leg['order_id']}, цена={short_leg['price']}, "
        f"комиссия за выход: {_format_fee(short_leg)})",
        pnl_line,
    ]
    if net_pnl_line:
        report_lines.append(net_pnl_line)
    report_lines.append(f"Задержка между ногами закрытия: {close_result['elapsed_ms']} мс")
    if not both_ok:
        report_lines.append(
            "⚠️ ВНИМАНИЕ: не обе ноги закрылись — проверьте позицию "
            "вручную на биржах!"
        )

    if both_ok:
        # ПЕРСИСТЕНТНЫЙ ЖУРНАЛ (см. trade_ledger.py) — по прямой просьбе
        # пользователя 2026-09-10. Единая точка для ВСЕХ завершённых
        # сделок независимо от источника (сканер/канал/SkySpreads) — все
        # они проходят через _finalize_close. Пишем ТОЛЬКО реально
        # закрытые (both_ok=True), не DRY_RUN (см. _pop_dry_run_position —
        # та ветка сюда вообще не доходит) и не "не обе ноги закрылись"
        # (те остаются в position_store, ещё не завершены).
        try:
            opened_at = position.get("opened_at")
            holding_seconds = None
            if opened_at:
                try:
                    opened_dt = datetime.fromisoformat(opened_at)
                    holding_seconds = (datetime.now(timezone.utc) - opened_dt).total_seconds()
                except ValueError:
                    pass
            trade_ledger.record_trade({
                "event": "close",  # см. trade_ledger.daily_stats — отличает от "open"-записей
                "coin": coin.upper(),
                "long_exchange": position.get("long_exchange"),
                "short_exchange": position.get("short_exchange"),
                "long_entry_price": position.get("long_entry_price"),
                "short_entry_price": position.get("short_entry_price"),
                "long_exit_price": long_leg.get("price"),
                "short_exit_price": short_leg.get("price"),
                "long_amount_coin": position.get("long_amount_coin"),
                "short_amount_coin": position.get("short_amount_coin"),
                "amount_usdt": position.get("amount_usdt"),
                "long_pnl": long_pnl,
                "short_pnl": short_pnl,
                "gross_pnl": total_pnl,
                "net_pnl": net_pnl,
                "net_pnl_pct": net_pnl_pct,
                "close_reason": close_reason,
                "opened_at": opened_at,
                "closed_at": datetime.now(timezone.utc).isoformat(),
                "holding_seconds": holding_seconds,
                "elapsed_ms": close_result.get("elapsed_ms"),
                "timings": close_result.get("timings"),
            })
        except Exception as exc:
            print(f"[trade_ledger] сбой при записи сделки {coin}: {exc}")

    return {
        "report": "\n".join(report_lines),
        "both_ok": both_ok,
        "position": position,
        "long_leg": long_leg,
        "short_leg": short_leg,
        # PnL ПО КАЖДОЙ НОГЕ ОТДЕЛЬНО (до комиссий) — дельта-нейтральная
        # связка почти всегда даёт одну ногу в плюс, другую в минус;
        # именно поэтому суммарный (gross_pnl/net_pnl) — не "сумма двух
        # прибылей", а РАЗНОСТЬ выигравшей и проигравшей ноги. Нужны
        # вызывающему коду, которому важна именно эта разбивка (например,
        # scanner.py: алерт закрытия тестовой серии показывает обе ноги
        # отдельно, чтобы было видно, как плюс перекрыл минус).
        "long_pnl": long_pnl,
        "short_pnl": short_pnl,
        "gross_pnl": total_pnl,
        "gross_pnl_pct": pnl_pct,
        "net_pnl": net_pnl,
        "net_pnl_pct": net_pnl_pct,
        # Раньше считалась (см. report_lines выше), но НЕ попадала в
        # возвращаемый словарь — вызывающий код (scanner.py: закрытие
        # тестовой серии использует именно _detailed, не текстовый
        # close_structured_signal) не мог её увидеть и залогировать,
        # хотя цифра для него куда полезнее, чем для текстового отчёта.
        "elapsed_ms": close_result["elapsed_ms"],
    }


# =============================================================================
# execute_arbitrage_trade — точка входа для АВТОНОМНОЙ авто-торговли
# (scanner.py, AUTO_TRADE_ENABLED=True). Тонкая обёртка над
# open_structured_signal() с явным, самодокументирующим набором полей
# (Symbol / Long Exchange / Short Exchange / Amount in USDT) — та же
# детерминированная логика (pre-check обеих ног, position_store, notifier),
# что и у сигналов канала, никакого отдельного пути исполнения нет.
# =============================================================================
def execute_arbitrage_trade(
    symbol: str,
    long_exchange: str,
    short_exchange: str,
    amount_usdt: float,
    spread_percent=None,
    notifier=None,
    extra_position_fields: dict = None,
) -> str:
    """Исполняет рыночный вход в арбитражный спред: Market-ордер LONG на
    long_exchange + Market-ордер SHORT на short_exchange, объём amount_usdt
    USDT на КАЖДУЮ ногу (не суммарно). Возвращает текстовый отчёт об
    исполнении — success/error по каждой ноге виден внутри него (см.
    open_structured_signal: статусы "OK"/"DRY_RUN_OK"/"ERROR: ..." на
    каждой ноге, что и есть логирование успеха/ошибки, которое вызывающий
    код обычно сразу print()-ит — см. scanner.py:_trigger_trade).

    extra_position_fields — см. open_structured_signal()/_finalize_open."""
    return open_structured_signal(
        symbol, long_exchange, short_exchange,
        spread_percent=spread_percent, notifier=notifier, amount_usdt=amount_usdt,
        extra_position_fields=extra_position_fields,
    )


async def execute_arbitrage_trade_async(
    symbol: str,
    long_exchange: str,
    short_exchange: str,
    amount_usdt: float,
    spread_percent=None,
    notifier=None,
    extra_position_fields: dict = None,
    prefetched_books=None,
    prefetched_books_at=None,
) -> str:
    """То же самое, что execute_arbitrage_trade(), но напрямую await'ит
    open_structured_signal_async() — по прямой просьбе пользователя
    2026-09-11 ("как ускорить открытие ног") заменяет
    scanner.py:_trigger_trade/_trigger_test_batch_trade's
    run_in_executor(execute_arbitrage_trade) (отдельный поток + двойная
    петля обратно на loop бота) на прямой await на том же event loop бота,
    на котором уже выполняется вызывающий код."""
    return await open_structured_signal_async(
        symbol, long_exchange, short_exchange,
        spread_percent=spread_percent, notifier=notifier, amount_usdt=amount_usdt,
        extra_position_fields=extra_position_fields,
        prefetched_books=prefetched_books, prefetched_books_at=prefetched_books_at,
    )
