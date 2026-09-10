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
) -> str:
    """Открывает обе ноги спреда и ставит позицию на учёт (position_store)
    для последующего закрытия. Возвращает текстовый отчёт.

    spread_percent — только для отображения в мгновенном уведомлении
    (necessary поле сигнала может отсутствовать — тогда notifier покажет
    "н/д"), на саму торговую логику не влияет.

    amount_usdt — переопределяет размер ОДНОЙ ноги в USDT (по умолчанию —
    общий TRADE_SIZE_USDT из .env, см. trade_tool.py). Используется, например,
    scanner.py, у которого свой отдельный размер сделки (AUTO_TRADE_AMOUNT_USDT).

    notifier (опционально) — если передан, СРАЗУ после того, как ордера
    реально отправлены на биржи (result уже получен от tool.open_spread),
    шлёт мгновенное Telegram-уведомление — ДО дальнейшей обработки (расчёт
    суммарной комиссии, запись в position_store), которая теоретически
    может упасть с ошибкой уже ПОСЛЕ того, как сделка реально совершена."""
    tool = TradeExecutionTool()
    result = tool.open_spread(coin, long_exchange, short_exchange, amount_usdt=amount_usdt)

    # tool.open_spread() может ПОМЕНЯТЬ long_exchange/short_exchange МЕСТАМИ
    # (см. "ПРОВЕРКА НАПРАВЛЕНИЯ ВХОДА" в trade_tool.py:_open_both_legs_async) —
    # если к моменту исполнения long-биржа сигнала оказалась ДОРОЖЕ
    # short-биржи (реальный случай с XRP 2026-09-06: канал назвал LONG
    # биржу, ставшую дороже — вошли бы с отрицательным спредом). Дальше по
    # функции ВСЕГДА ориентируемся на то, что РЕАЛЬНО произошло (result), а
    # не на исходные аргументы сигнала — иначе отчёт/уведомление/
    # position_store подпишут ноги под неверную биржу.
    long_exchange = result.get("long_exchange", long_exchange)
    short_exchange = result.get("short_exchange", short_exchange)

    if result["cancelled"]:
        if notifier:
            notifier.notify_error(
                symbol=result["coin"],
                error_message=f"Сделка не открыта ни на одной ноге: {result['reason']}",
            )
        return f"Монета: {result['coin']}\nСделка НЕ открыта: {result['reason']}"

    long_leg, short_leg = result["long"], result["short"]
    ok_statuses = ("OK", "DRY_RUN_OK")
    both_ok = long_leg["status"] in ok_statuses and short_leg["status"] in ok_statuses

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
                )
            else:
                rollback = long_leg.get("rollback") or short_leg.get("rollback")
                rollback_note = (
                    f" Открывшаяся нога автоматически закрыта (rollback: {rollback['status']})."
                    if rollback
                    else " ⚠️ Ни одна нога не была закрыта автоматически — проверьте вручную!"
                )
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
        # Записываем позицию на учёт — по ней должен прийти "aligned in"
        # (или закрывающий сигнал сканера), и тогда close_structured_signal()
        # найдёт её здесь.
        position_store.record_open(
            result["coin"],
            {
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
            },
        )
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
        else:
            report_lines.append(
                "⚠️ ВНИМАНИЕ: не все ноги открылись успешно, и автоматический "
                "откат не удался (или не потребовался определить объём) — "
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


async def close_structured_signal_detailed_async(coin: str, notifier=None, reason: str = None) -> Optional[dict]:
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
) -> str:
    """Исполняет рыночный вход в арбитражный спред: Market-ордер LONG на
    long_exchange + Market-ордер SHORT на short_exchange, объём amount_usdt
    USDT на КАЖДУЮ ногу (не суммарно). Возвращает текстовый отчёт об
    исполнении — success/error по каждой ноге виден внутри него (см.
    open_structured_signal: статусы "OK"/"DRY_RUN_OK"/"ERROR: ..." на
    каждой ноге, что и есть логирование успеха/ошибки, которое вызывающий
    код обычно сразу print()-ит — см. scanner.py:_trigger_trade)."""
    return open_structured_signal(
        symbol, long_exchange, short_exchange,
        spread_percent=spread_percent, notifier=notifier, amount_usdt=amount_usdt,
    )
