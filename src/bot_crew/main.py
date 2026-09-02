# =============================================================================
# main.py — ТОЧКА ВХОДА В ПРИЛОЖЕНИЕ. Здесь запускается вся команда агентов
# и (при запуске listen()) живое прослушивание Telegram-канала с сигналами.
# =============================================================================
# Запуск из корня проекта (см. README.md за подробностями установки):
#   1) pip install -r requirements.txt
#   2) pip install -e .          <- делает пакет "bot_crew" импортируемым
#   3) python -m bot_crew.main            — разовый тестовый прогон
#   3) python -m bot_crew.main --listen   — живое прослушивание канала
# =============================================================================

# --- Импорты стандартной библиотеки -----------------------------------------
import asyncio
import os
import re
import sys

# На Windows консоль по умолчанию использует кодировку вроде cp1251, которая
# не умеет печатать эмодзи (например, 🚨 из тестового сигнала ниже) и падает
# с UnicodeEncodeError. Принудительно переключаем stdout/stderr на UTF-8.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

# --- python-dotenv: загрузка переменных окружения из файла .env -------------
# load_dotenv() читает файл .env (если он есть рядом) и добавляет все его
# строки вида KEY=VALUE в os.environ, откуда их потом читают crew.py и
# trade_tool.py через os.getenv(...).
from dotenv import load_dotenv

# Наш класс сборки команды агентов из crew.py и инструмент сделок.
from bot_crew.crew import BotCrew
from bot_crew.tools.trade_tool import TradeExecutionTool
from bot_crew import position_store
from bot_crew.notifier import TelegramNotifier


# =============================================================================
# КЛАССИФИКАЦИЯ СООБЩЕНИЙ КАНАЛА
# =============================================================================
# В канале два типа сигналов (см. разбор реальных сообщений 27.08.2026):
#   OPEN  — "#TAC | Spread: 3.31%" со строками Short/Long — открываем сделку.
#   CLOSE — "#TAC aligned in 179:51" (таймер) — закрываем ранее открытую
#           позицию по этой монете, если она у нас есть в учёте.
# Любые другие сообщения (или "aligned in" по монете, которую мы не
# открывали) не обрабатываются.
# =============================================================================
_COIN_HASHTAG_RE = re.compile(r"#([A-Za-z0-9]{2,15})")


def classify_signal(text: str) -> str:
    """Возвращает 'OPEN', 'CLOSE' или 'UNKNOWN' по тексту сообщения."""
    if not text:
        return "UNKNOWN"
    if "Spread:" in text:
        return "OPEN"
    if "aligned in" in text and _COIN_HASHTAG_RE.search(text):
        return "CLOSE"
    return "UNKNOWN"


def extract_coin_from_signal(text: str):
    """Достаёт тикер из первого хештега вида '#TAC' (без самой решётки)."""
    match = _COIN_HASHTAG_RE.search(text or "")
    return match.group(1).upper() if match else None


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
# OPEN — открытие позиции по Spread-сигналу
# =============================================================================
# ВАЖНО: парсинг текста делает LLM (BotCrew.parse_signal — единственная
# задача, где реально нужна языковая модель: вытащить структурные поля из
# полу-хаотичного текста). САМО ОТКРЫТИЕ СДЕЛКИ идёт напрямую через
# TradeExecutionTool.open_spread() — обычным Python-вызовом, БЕЗ
# LLM-агента trade_executor. Так надёжнее: цену входа, объём и order_id
# для реальных денег лучше брать из фактического ответа CCXT, а не из
# текста, сгенерированного языковой моделью.
# =============================================================================
def open_signal(raw_text: str, notifier: "TelegramNotifier | None" = None) -> str:
    """Парсит OPEN-сигнал, открывает обе ноги и ставит позицию на учёт
    (position_store) для последующего закрытия. Возвращает текстовый отчёт.

    notifier (опционально) — если передан, СРАЗУ после того, как ордера
    реально отправлены на биржи (result уже получен от tool.open_spread),
    шлёт мгновенное Telegram-уведомление — ДО дальнейшей обработки (расчёт
    суммарной комиссии, запись в position_store), которая теоретически
    может упасть с ошибкой уже ПОСЛЕ того, как сделка реально совершена."""
    parsed = BotCrew().parse_signal(raw_text)

    coin = parsed.get("coin")
    long_exchange = parsed.get("long_exchange")
    short_exchange = parsed.get("short_exchange")

    if not coin or not long_exchange or not short_exchange:
        return (
            f"Сигнал не распознан как валидный Spread-сигнал "
            f"(распарсено: {parsed}) — сделка не открывается."
        )

    tool = TradeExecutionTool()
    result = tool.open_spread(coin, long_exchange, short_exchange)

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
                    spread=parsed.get("spread_percent"),
                )
            else:
                notifier.notify_error(
                    symbol=result["coin"],
                    error_message=(
                        f"LONG {long_exchange}: {long_leg['status']}; "
                        f"SHORT {short_exchange}: {short_leg['status']} — "
                        f"не все ноги открылись, позиция НЕ поставлена на "
                        f"автозакрытие, требуется ручная проверка!"
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
        # с тем же тикером, и тогда close_signal() найдёт её здесь.
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
            '"aligned in" из канала.'
        )
    else:
        # Одна из ног не открылась (несмотря на pre-check в trade_tool) —
        # например, ERROR уже на боевом ордере (недостаточно средств и
        # т.п.). НЕ ставим на автозакрытие: если открылась ровно одна
        # нога, это реальная незахеджированная позиция, требующая
        # ручного вмешательства, а не тихого автоматического учёта.
        report_lines.append(
            "⚠️ ВНИМАНИЕ: не все ноги открылись успешно — позиция НЕ "
            "поставлена на автоматическое закрытие. Проверьте вручную "
            "на биржах!"
        )

    return "\n".join(report_lines)


# =============================================================================
# CLOSE — закрытие позиции по сигналу "aligned in"
# =============================================================================
# Здесь СОЗНАТЕЛЬНО нет никакого LLM — детерминированная логика (найти
# позицию по тикеру, закрыть обе ноги, посчитать PnL арифметикой) не
# должна зависеть от вероятностной генерации текста, особенно когда на
# кону реальные деньги и точный расчёт прибыли/убытка.
# =============================================================================
def close_signal(coin: str, notifier: "TelegramNotifier | None" = None) -> str:
    """Закрывает позицию по монете, если она есть в учёте. Возвращает
    текстовый отчёт с PnL, либо None, если по этой монете позиции нет
    (значит, сигнал не про нашу сделку — не обрабатываем).

    notifier — см. open_signal(): шлёт мгновенный алерт сразу после
    закрытия ордеров, до финального форматирования текстового отчёта."""
    position = position_store.get_position(coin)
    if not position:
        return None

    if position.get("long_amount_coin") is None or position.get("short_amount_coin") is None:
        # Позиция была открыта в DRY_RUN без реального объёма в монете —
        # физически закрывать нечего, просто снимаем с учёта.
        position_store.pop_position(coin)
        return (
            f"Монета: {coin}\n"
            f"Получен сигнал закрытия, но позиция была открыта в DRY_RUN "
            f"(нет реального объёма в монете) — снята с учёта без реального "
            f"закрытия ордеров."
        )

    tool = TradeExecutionTool()
    close_result = tool.close_spread(
        coin,
        position["long_exchange"],
        position["short_exchange"],
        position["long_amount_coin"],
        position["short_amount_coin"],
    )

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
                    reason="спред сошёлся (сигнал \"aligned in\" из канала)",
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

    return "\n".join(report_lines)


# =============================================================================
# run() — сохранён для обратной совместимости и разового тестового прогона
# (python -m bot_crew.main без --listen). Прогоняет ПОЛНЫЙ Crew (парсинг +
# LLM-агент trade_executor) — только для демонстрации/отладки; в живом
# прослушивании (listen()) используется open_signal()/close_signal() выше.
# =============================================================================
def run(raw_signal_text: str) -> str:
    bot_crew = BotCrew().crew()
    result = bot_crew.kickoff(inputs={"raw_signal_text": raw_signal_text})
    return str(result)


def _check_llm_key_configured() -> None:
    if not (
        os.getenv("ANTHROPIC_API_KEY")
        or os.getenv("OPENAI_API_KEY")
        or os.getenv("OPENROUTER_API_KEY")
    ):
        print(
            "ОШИБКА: не найден ни один из ключей ANTHROPIC_API_KEY, "
            "OPENAI_API_KEY, OPENROUTER_API_KEY.\n"
            "Скопируйте .env.example в .env и впишите свой ключ API."
        )
        sys.exit(1)


def run_demo() -> None:
    """Разовый тестовый прогон на реальном примере сигнала (без Telegram)."""
    _check_llm_key_configured()

    example_signal_text = (
        "**📈****📈****#PURR**** | Spread: 3.16%**\n"
        "**📌 PURR_USDT (COPY: **`PURR`)\n"
        "\n"
        "```🔴Short MEXC    : $0.146400000\n"
        "🟢Long  HLIQUID : $0.141920000\n"
        "\n"
        "🌗Funding MEXC    :  0.02%\n"
        "🌓Funding HLIQUID :  0.00%\n"
        "```**\n"
        "🔍 Additional Info:**\n"
        "```⚖️MEXC Max Size     :  $111\n"
        "\n"
        "⏱️F/Interval MEXC   :  4H | 20:00 UTC\n"
        "⏱️F/Funding HLIQUID :  1H | 17:00 UTC```\n"
        "\n"
        "📝 **All Exchanges Overview:**\n"
        "```🟢HYPERLIQUID: $0.14193\n"
        "⚪️MEXC       : $0.1464\n"
        "⚫️BYBIT      : $14.082```"
    )

    print("=" * 70)
    print("ЗАПУСК CREW НА ТЕСТОВОМ СИГНАЛЕ:")
    print(example_signal_text)
    print("=" * 70)

    final_report = run(example_signal_text)

    print("=" * 70)
    print("ИТОГОВЫЙ ОТЧЁТ:")
    print(final_report)
    print("=" * 70)


# =============================================================================
# listen() — ЖИВОЕ прослушивание TELEGRAM_SIGNAL_CHANNEL: на каждое новое
# сообщение классифицирует его (OPEN/CLOSE/UNKNOWN) и вызывает
# open_signal()/close_signal(). Каждый отчёт печатается в консоль И
# отправляется вам в Telegram (в Saved Messages того же аккаунта, которым
# бот залогинен — bot_session.session).
# =============================================================================
def listen() -> None:
    # Импорт telethon — только здесь, чтобы разовый demo-запуск (run_demo)
    # не требовал даже устанавливать/поднимать этот клиент.
    from telethon import TelegramClient, events

    _check_llm_key_configured()

    channel_id = int(os.getenv("TELEGRAM_SIGNAL_CHANNEL"))
    client = TelegramClient(
        "bot_session",
        int(os.getenv("TELEGRAM_API_ID")),
        os.getenv("TELEGRAM_API_HASH"),
    )

    # notifier шлёт мгновенные алерты о входе/выходе/ошибках прямо из
    # open_signal()/close_signal() (см. main.py выше) — те выполняются в
    # ОТДЕЛЬНОМ потоке (run_in_executor ниже), поэтому notifier принимает
    # именно event loop клиента (client.loop), чтобы безопасно планировать
    # отправку из чужого потока через run_coroutine_threadsafe.
    notifier = TelegramNotifier(client, client.loop)

    dry_run = os.getenv("DRY_RUN", "True").lower() == "true"
    print("=" * 70)
    print(f"ЗАПУСК ЖИВОГО ПРОСЛУШИВАНИЯ КАНАЛА {channel_id}")
    print(f"DRY_RUN={dry_run} " + ("(симуляция, реальные ордера НЕ отправляются)"
                                    if dry_run else "(БОЕВОЙ РЕЖИМ — реальные деньги!)"))
    print("=" * 70)

    @client.on(events.NewMessage(chats=channel_id))
    async def handler(event):
        text = event.raw_text or ""
        signal_type = classify_signal(text)

        # ВАЖНО: open_signal()/close_signal() — синхронные функции, а
        # внутри они вызывают либо CrewAI Crew.kickoff(), либо
        # asyncio.run() (в trade_tool.py) — и то, и другое ЗАПРЕЩЕНО
        # вызывать напрямую из async-кода, если event loop УЖЕ работает
        # (а здесь он работает — это Telethon-обработчик). Оба падают с
        # RuntimeError "invoked synchronously from within a running event
        # loop" при прямом вызове. Решение: выполнить их в отдельном
        # потоке через run_in_executor — там своего running loop нет, и
        # kickoff()/asyncio.run() внутри отрабатывают нормально.
        loop = asyncio.get_running_loop()

        # ВАЖНО: любое НЕПРЕДВИДЕННОЕ исключение внутри open_signal()/
        # close_signal() (а не штатный "ERROR"-статус ноги, который уже
        # обрабатывается внутри них) раньше тихо обрывало handler() ДО
        # print(report)/send_message — трейд мог уже реально исполниться
        # на бирже, а уведомление о нём просто пропадало. Оборачиваем в
        # try/except, чтобы такой сбой тоже дошёл до вас алертом, а не
        # терялся молча.
        try:
            if signal_type == "OPEN":
                print(f"\n[OPEN-сигнал получен] {text[:60]}...")
                report = await loop.run_in_executor(None, open_signal, text, notifier)
            elif signal_type == "CLOSE":
                coin = extract_coin_from_signal(text)
                print(f"\n[CLOSE-сигнал получен] монета={coin}")
                report = await loop.run_in_executor(None, close_signal, coin, notifier)
                if report is None:
                    # Позиции по этой монете у нас нет — сигнал не для нас,
                    # молча пропускаем (НЕ шлём отчёт впустую).
                    return
            else:
                return  # не наш тип сообщения — пропускаем
        except Exception as exc:
            print(f"[КРИТИЧЕСКАЯ ОШИБКА обработки сигнала] {exc}")
            notifier.notify_error(
                symbol=extract_coin_from_signal(text) or "?",
                error_message=f"Необработанное исключение при обработке сигнала: {exc}",
            )
            return

        print("-" * 70)
        print(report)
        print("-" * 70)

        # Отправляем отчёт в Saved Messages того же аккаунта — это и есть
        # запрошенная у бота отчётность (поставили ордер / пришёл сигнал /
        # закрыли / баланс / PnL — всё внутри report).
        try:
            await client.send_message("me", report)
        except Exception as exc:
            print(f"Не удалось отправить отчёт в Telegram: {exc}")

    client.start(os.getenv("TELEGRAM_PHONE"))
    client.run_until_disconnected()


def main() -> None:
    """Точка входа при запуске файла напрямую (python -m ...)."""
    load_dotenv()

    if "--listen" in sys.argv:
        listen()
    else:
        run_demo()


# Стандартная Python-идиома: код внутри блока if выполняется, только
# когда файл запущен НАПРЯМУЮ (python main.py), а не при импорте модуля.
if __name__ == "__main__":
    main()
