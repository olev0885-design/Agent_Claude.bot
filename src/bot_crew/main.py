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
import time

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
from bot_crew import trade_executor
from bot_crew import position_store
from bot_crew import blocked_coins_store
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
# OPEN/CLOSE — тонкие обёртки поверх trade_executor.py (детерминированная
# логика открытия/закрытия по структурным coin/long_exchange/short_exchange,
# без LLM — см. подробный комментарий в самом trade_executor.py). Здесь,
# в main.py, добавляется ТОЛЬКО то, что специфично именно для Telegram-
# сигналов канала: парсинг сырого текста через LLM (open_signal) и разбор
# формата "aligned in" (close_signal). scanner.py вызывает
# trade_executor.open_structured_signal()/close_structured_signal() НАПРЯМУЮ,
# минуя эти обёртки — у него уже готовые структурные данные, LLM не нужен.
# =============================================================================
def open_signal(raw_text: str, notifier: "TelegramNotifier | None" = None) -> str:
    """Парсит OPEN-сигнал канала через LLM (BotCrew.parse_signal — единственное
    место, где он реально нужен: вытащить структурные поля из полу-хаотичного
    текста) и делегирует открытие сделки в trade_executor.

    ГЕЙТЫ ВХОДА (добавлены 2026-09-06 по явной просьбе пользователя — "те же
    правила, что и у сканера"): сигналы канала раньше открывали сделку БЕЗ
    какой-либо проверки — ни порога спреда, ни лимита "не более 1 позиции
    одновременно", ни списка исключённых монет (SCANNER_EXCLUDED_COINS), в
    отличие от сканера (scanner.py:_handle_test_batch_opportunity). Теперь
    сигнал канала проходит ТЕ ЖЕ три проверки, что и находки сканера, ПЕРЕД
    тем как уйти в trade_executor — единообразно, независимо от источника
    связки."""
    parsed = BotCrew().parse_signal(raw_text)

    coin = parsed.get("coin")
    long_exchange = parsed.get("long_exchange")
    short_exchange = parsed.get("short_exchange")
    spread_percent = parsed.get("spread_percent")

    if not coin or not long_exchange or not short_exchange:
        return (
            f"Сигнал не распознан как валидный Spread-сигнал "
            f"(распарсено: {parsed}) — сделка не открывается."
        )

    excluded_coins = {
        name.strip().upper()
        for name in os.getenv("SCANNER_EXCLUDED_COINS", "").split(",")
        if name.strip()
    }
    if coin.upper() in excluded_coins:
        return f"Монета #{coin} в списке исключённых (SCANNER_EXCLUDED_COINS) — сигнал пропущен."

    # Точечное исключение "монета+биржа" (см. blocked_coins_store.py) — по
    # прямой просьбе пользователя 2026-09-09: монета торгуется нормально
    # везде, кроме конкретной биржи (напр. DELTA на MEXC).
    for bad_exchange in (long_exchange, short_exchange):
        if blocked_coins_store.is_coin_exchange_excluded(coin, bad_exchange):
            return (
                f"Сигнал #{coin}: монета исключена именно для биржи {bad_exchange} "
                f"(SCANNER_EXCLUDED_COIN_EXCHANGE_PAIRS) — сигнал пропущен."
            )

    if blocked_coins_store.is_blocked(coin):
        info = blocked_coins_store.get_block_info(coin) or {}
        return (
            f"Монета #{coin} АВТОМАТИЧЕСКИ ЗАБЛОКИРОВАНА после повторных реальных откатов "
            f"(последняя причина: {(info.get('reasons') or ['?'])[-1]}) — сигнал пропущен до ручной разблокировки."
        )

    min_spread = float(os.getenv("TEST_BATCH_MIN_SPREAD", os.getenv("AUTO_TRADE_MIN_SPREAD", "3.0")))
    if spread_percent is not None and spread_percent < min_spread:
        return (
            f"Сигнал #{coin}: спред {spread_percent}% ниже порога входа "
            f"{min_spread}% — сделка не открывается."
        )

    # НЕ БОЛЕЕ 1 ОТКРЫТОЙ НОГИ НА БИРЖУ ОДНОВРЕМЕННО (по явной просьбе
    # пользователя 2026-09-07 — та же проверка, что и в scanner.py:
    # _handle_test_batch_opportunity, см. комментарий в position_store.
    # busy_exchanges() — снижает риск каскада под cross margin).
    busy = position_store.busy_exchanges()
    if long_exchange.lower() in busy or short_exchange.lower() in busy:
        return (
            f"Сигнал #{coin}: на бирже {long_exchange if long_exchange.lower() in busy else short_exchange} "
            f"уже есть открытая нога другой позиции (лимит — 1 нога на биржу) — сигнал пропущен."
        )

    # ИСКЛЮЧЕНИЕ (по явной просьбе пользователя 2026-09-06): разрешён РОВНО
    # ОДИН сигнал канала сверх обычного лимита сканера "не более 1 позиции
    # одновременно" — т.е. сигнал канала пропускается, только если открытых
    # позиций УЖЕ 2 или больше (а не 1, как для находок самого сканера).
    if len(position_store.list_positions()) >= 2:
        return (
            f"Сигнал #{coin}: уже есть 2 открытые позиции (обычный лимит "
            f"сканера — 1, для сигналов канала сделано разовое исключение "
            f"ещё на 1) — сигнал пропущен."
        )

    return trade_executor.open_structured_signal(
        coin, long_exchange, short_exchange,
        spread_percent=spread_percent,
        notifier=notifier,
    )


def close_signal(coin: str, notifier: "TelegramNotifier | None" = None) -> str:
    """Закрывает позицию по сигналу "aligned in" из канала — тонкая обёртка
    над trade_executor.close_structured_signal() с соответствующей причиной
    закрытия для отчёта/уведомления."""
    return trade_executor.close_structured_signal(
        coin, notifier=notifier, reason='сигнал "aligned in" из канала'
    )


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
        # connection_retries=None — БЕСКОНЕЧНЫЕ попытки переподключения
        # (добавлено 2026-09-15 после трёх тихих остановок бота за сутки).
        # По умолчанию Telethon сдаётся после 5 попыток; после сна ноутбука
        # (журнал Windows: 23:24:33 «переход в спящий режим», последний
        # heartbeat 23:24:16) сокеты мёртвы, пять попыток не проходят,
        # run_until_disconnected() ВОЗВРАЩАЕТСЯ — и listen() тихо
        # завершается кодом 0: ни Traceback, ни строки в stderr. Бот
        # просто «заканчивался». Телеграм для нас — не причина умирать:
        # сканер, мониторинг и ордера от него не зависят.
        connection_retries=None,
        retry_delay=2,
        auto_reconnect=True,
    )

    # notifier шлёт мгновенные алерты о входе/выходе/ошибках прямо из
    # open_signal()/close_signal() (см. main.py выше) — те выполняются в
    # ОТДЕЛЬНОМ потоке (run_in_executor ниже), поэтому notifier принимает
    # именно event loop клиента (client.loop), чтобы безопасно планировать
    # отправку из чужого потока через run_coroutine_threadsafe.
    notifier = TelegramNotifier(client, client.loop)

    # См. подробный комментарий у _bot_event_loop в trade_tool.py — по
    # просьбе пользователя 2026-09-08 ("уменьшить задержку на мониторинг,
    # проверку цены и вход"): сообщаем trade_tool.py ЭТОТ ЖЕ (Telethon/
    # scanner) постоянный event loop, чтобы реальное исполнение сделок
    # планировалось на него (run_coroutine_threadsafe, тот же механизм,
    # что и у notifier выше) и переиспользовало живые соединения с
    # биржами между сделками — вместо одноразового loop на КАЖДЫЙ
    # ордер/проверку/закрытие. При TRADE_USE_PERSISTENT_CONNECTIONS=False
    # (мгновенный откат без правки кода) эта настройка просто игнорируется.
    from bot_crew.tools import trade_tool
    trade_tool.set_bot_event_loop(client.loop)

    dry_run = os.getenv("DRY_RUN", "True").lower() == "true"
    demo_trading = os.getenv("DEMO_TRADING", "False").lower() == "true"
    print("=" * 70)
    print(f"ЗАПУСК ЖИВОГО ПРОСЛУШИВАНИЯ КАНАЛА {channel_id}")
    print(f"DRY_RUN={dry_run} " + ("(симуляция, реальные ордера НЕ отправляются)"
                                    if dry_run else "(ордера реально отправляются на биржи)"))
    if not dry_run:
        # DEMO_TRADING сам по себе не защищает биржи вне
        # DEMO_TRADING_SUPPORTED (mexc, aster) — они по умолчанию просто
        # пропускаются целиком, КРОМЕ явно разрешённых через
        # DEMO_ALLOW_LIVE_EXCHANGES (см. trade_tool.py _is_exchange_configured)
        # — те торгуют РЕАЛЬНЫМИ деньгами даже во время demo. Баннер ниже
        # явно называет их, чтобы это не терялось в логах.
        live_allowed = sorted(
            n.strip() for n in os.getenv("DEMO_ALLOW_LIVE_EXCHANGES", "").split(",") if n.strip()
        )
        print(
            "DEMO_TRADING=" + str(demo_trading)
            + (
                " (bybit/bitget/gate — виртуальные деньги; остальные сигналы"
                " пропускаются целиком"
                + (
                    f", КРОМЕ {', '.join(live_allowed)} — торгует(ют) РЕАЛЬНЫМИ деньгами!)"
                    if live_allowed
                    else ")"
                )
                if demo_trading
                else " (⚠️ БОЕВОЙ РЕЖИМ НА ВСЕХ БИРЖАХ — реальные деньги!)"
            )
        )
    print("=" * 70)

    # =========================================================================
    # СКАНЕР РЫНКА (scanner.py) — опционально, SCANNER_ENABLED=True в .env.
    # Ищет спреды/фандинг САМ через CCXT, не дожидаясь сигналов канала, и
    # шлёт вам Telegram-алерты (+ авто-торговля, если ещё и AUTO_TRADE=True).
    # Запускается КАК ФОНОВАЯ ЗАДАЧА в ТОМ ЖЕ event loop, что и Telegram-
    # клиент (client.loop.create_task(...)) — не отдельный процесс/поток, а
    # ещё одна корутина, выполняющаяся параллельно с обработкой сообщений
    # канала. Планируем задачу СЕЙЧАС, а не await — сама она начнёт
    # выполняться, когда цикл действительно закрутится внутри
    # client.run_until_disconnected() ниже.
    # =========================================================================
    scanner_enabled = os.getenv("SCANNER_ENABLED", "False").lower() == "true"
    if scanner_enabled:
        from bot_crew.scanner import FundingScanner

        scanner = FundingScanner(notifier)
        client.loop.create_task(scanner.run())
        print(f"[scanner] Включён (SCANNER_ENABLED=True). AUTO_TRADE_ENABLED={scanner.auto_trade.enabled}")
    else:
        print("[scanner] Выключен (SCANNER_ENABLED=False в .env).")
    print("=" * 70)

    # ПРОСЛУШИВАНИЕ КАНАЛА СИГНАЛОВ — отключаемо ОТДЕЛЬНО от самого
    # Telegram-клиента (по явной просьбе пользователя 2026-09-06:
    # "отключись от телеграм канала", оставив сканер и уведомления
    # рабочими). CHANNEL_SIGNALS_ENABLED=False просто не регистрирует
    # обработчик новых сообщений канала — сам client (и, значит,
    # notifier/сканер) продолжает работать как обычно.
    channel_signals_enabled = os.getenv("CHANNEL_SIGNALS_ENABLED", "True").lower() == "true"
    if not channel_signals_enabled:
        print(f"[channel] Прослушивание канала {channel_id} ОТКЛЮЧЕНО (CHANNEL_SIGNALS_ENABLED=False).")
    print("=" * 70)

    # ВТОРОЙ ИСТОЧНИК СИГНАЛОВ — канал внешнего сервиса SkySpreads.net (по
    # прямой просьбе пользователя 2026-09-09). Формат сообщений полностью
    # другой (см. skyspreads_signals.py) — отдельный классификатор/парсер
    # БЕЗ LLM (формат стабильный), отдельный event-handler на СВОЙ channel
    # ID, те же гейты входа (excluded_coins/автоблокировка/лимит ноги на
    # биржу/лимит позиций/порог спреда) + доп.проверка "обе биржи сигнала
    # реально поддерживаются ботом" (SkySpreads мониторит 18+ бирж, мы
    # торгуем на 6 — большинство сигналов оттуда не наши, это норма).
    skyspreads_enabled = os.getenv("SKYSPREADS_SIGNALS_ENABLED", "False").lower() == "true"
    skyspreads_channel_id_raw = os.getenv("SKYSPREADS_CHANNEL_ID")
    if skyspreads_enabled and not skyspreads_channel_id_raw:
        print("[skyspreads] SKYSPREADS_SIGNALS_ENABLED=True, но SKYSPREADS_CHANNEL_ID не задан — прослушивание НЕ запущено.")
        skyspreads_enabled = False
    if skyspreads_enabled:
        print(f"[skyspreads] Прослушивание канала SkySpreads {skyspreads_channel_id_raw} ВКЛЮЧЕНО.")
    else:
        print("[skyspreads] Прослушивание канала SkySpreads ОТКЛЮЧЕНО (SKYSPREADS_SIGNALS_ENABLED=False).")
    print("=" * 70)

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

    # Регистрируем обработчик ТОЛЬКО если прослушивание канала включено —
    # см. CHANNEL_SIGNALS_ENABLED выше. Client.on(...) как декоратор всегда
    # регистрирует безусловно, поэтому здесь — явный add_event_handler
    # внутри if, а не декоратор над функцией handler.
    if channel_signals_enabled:
        client.add_event_handler(handler, events.NewMessage(chats=channel_id))

    # ВАЖНО: канал SkySpreads состоит из аккаунта ВТОРОГО номера пользователя
    # (SKYSPREADS_TELEGRAM_PHONE, не TELEGRAM_PHONE основного рабочего
    # аккаунта бота) — по прямой просьбе пользователя 2026-09-09, у него
    # нет возможности добавить рабочий аккаунт бота в этот чат напрямую.
    # Поэтому слушаем этот канал ВТОРЫМ, отдельным TelegramClient (своя
    # сессия skyspreads_session.session, вход выполнен один раз заранее),
    # но на ТОМ ЖЕ event loop, что и основной client (loop=client.loop) —
    # критично для scanner.py/trade_tool.py, которые уже привязаны к этому
    # конкретному loop (см. set_bot_event_loop выше). Отчёты всё равно
    # шлём через ОСНОВНОЙ client в "me" — единая точка уведомлений,
    # независимо от того, каким аккаунтом сигнал был прочитан.
    skyspreads_client = None
    if skyspreads_enabled:
        from bot_crew import skyspreads_signals

        skyspreads_channel_id = int(skyspreads_channel_id_raw)
        skyspreads_session = os.getenv("SKYSPREADS_TELEGRAM_SESSION", "skyspreads_session")

        skyspreads_client = TelegramClient(
            skyspreads_session,
            int(os.getenv("TELEGRAM_API_ID")),
            os.getenv("TELEGRAM_API_HASH"),
            loop=client.loop,
            connection_retries=None,  # см. комментарий у основного client
            retry_delay=2,
            auto_reconnect=True,
        )

        async def skyspreads_handler(event):
            text = event.raw_text or ""
            if skyspreads_signals.classify_skyspreads_signal(text) != "OPEN":
                return
            print(f"\n[SkySpreads OPEN-сигнал получен] {text[:60]}...")
            loop = asyncio.get_running_loop()
            try:
                report = await loop.run_in_executor(
                    None, skyspreads_signals.open_skyspreads_signal, text, notifier
                )
            except Exception as exc:
                print(f"[КРИТИЧЕСКАЯ ОШИБКА обработки SkySpreads-сигнала] {exc}")
                notifier.notify_error(
                    symbol="?",
                    error_message=f"Необработанное исключение при обработке SkySpreads-сигнала: {exc}",
                )
                return
            if report is None:
                # Сигнал не наш (неподдерживаемая биржа/мусорный спред) —
                # намеренно НЕ шлём уведомление, см. skyspreads_signals.py.
                return
            print("-" * 70)
            print(report)
            print("-" * 70)
            try:
                await client.send_message("me", report)
            except Exception as exc:
                print(f"Не удалось отправить отчёт в Telegram: {exc}")

        skyspreads_client.add_event_handler(skyspreads_handler, events.NewMessage(chats=skyspreads_channel_id))

    # НЕ ДАВАТЬ WINDOWS УСНУТЬ ПО БЕЗДЕЙСТВИЮ, пока бот работает (2026-09-15).
    # SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED) — штатный
    # способ, которым плееры и загрузчики держат систему бодрой; прав
    # администратора не требует. От закрытия крышки и ручного «Сон» это НЕ
    # спасает — на это бот повлиять не может, только пережить (см.
    # супервизор ниже). Ошибка здесь не критична — просто логируем.
    if os.name == "nt":
        try:
            import ctypes
            ES_CONTINUOUS, ES_SYSTEM_REQUIRED = 0x80000000, 0x00000001
            if ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED):
                print("[startup] Сон Windows по бездействию заблокирован на время работы бота.")
            else:
                print("[startup] Не удалось заблокировать сон по бездействию (SetThreadExecutionState вернул 0).")
        except Exception as exc:
            print(f"[startup] Не удалось заблокировать сон по бездействию: {exc}")

    client.start(os.getenv("TELEGRAM_PHONE"))

    if skyspreads_client is not None:
        # Сессия уже авторизована (вход выполнен заранее отдельным
        # скриптом) — start() без телефона просто подключается, не
        # запрашивая код повторно.
        skyspreads_client.start()
        print(f"[skyspreads] Второй аккаунт подключён и слушает канал {skyspreads_channel_id_raw}.")

    # =========================================================================
    # УВЕДОМЛЕНИЕ О ПРАВКАХ/РЕСТАРТЕ В @Depositik — по явной просьбе
    # пользователя 2026-09-07: "как будешь вносить какие-то правки или
    # что-то подобное, то сообщай в телеграмме @Depositik". Правки в код/
    # .env применяются только после перезапуска бота (см. комментарии в
    # config.py/scanner.py про "читается один раз при старте"), поэтому
    # каждый рестарт — естественная точка для отчёта. Заметку ПЕРЕД
    # рестартом кладут в restart_note.txt (в корне проекта, рядом с
    # open_positions.json) — читаем и сразу удаляем файл, чтобы не
    # продублировать её при следующем обычном рестарте без новой заметки.
    #
    # notifier здесь намеренно НЕ используется (его _dispatch планирует
    # отправку через run_coroutine_threadsafe в УЖЕ КРУТЯЩИЙСЯ event loop
    # — client.loop входит в этот режим только внутри run_until_disconnected
    # ниже) — вместо этого отправляем СИНХРОННО через run_until_complete,
    # как и сам client.start() парой строк выше.
    # =========================================================================
    _restart_note_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "restart_note.txt",
    )
    _restart_note = ""
    if os.path.exists(_restart_note_path):
        try:
            with open(_restart_note_path, "r", encoding="utf-8") as _f:
                _restart_note = _f.read().strip()
            os.remove(_restart_note_path)
        except OSError as exc:
            print(f"[startup] Не удалось прочитать/удалить restart_note.txt: {exc}")
    _restart_text = "🔄 Бот перезапущен" + (f"\n\n{_restart_note}" if _restart_note else "")
    try:
        client.loop.run_until_complete(
            asyncio.gather(
                client.send_message("me", _restart_text),
                client.send_message("@Depositik", _restart_text),
                # @G_Pobedonosec добавлен 2026-09-12 по прямой просьбе
                # пользователя — оба аккаунта должны видеть рестарты/правки
                # в боте, наравне с входом/выходом и дневным отчётом.
                client.send_message("@G_Pobedonosec", _restart_text),
                return_exceptions=True,
            )
        )
    except Exception as exc:
        print(f"[startup] Не удалось отправить уведомление о рестарте: {exc}")

    # =========================================================================
    # ПРОЦЕСС НЕ ИМЕЕТ ПРАВА ЗАВЕРШИТЬСЯ ИЗ-ЗА TELEGRAM (добавлено 2026-09-15).
    #
    # Раньше здесь была одна строка client.run_until_disconnected(). Она
    # возвращается, когда Telethon окончательно теряет связь, — и на этом
    # listen() заканчивался, а вместе с ним умирали сканер, мониторинг
    # открытых позиций и сверка. Без единой ошибки в логе: процесс просто
    # выходил с кодом 0. Так бот тихо остановился трижды за 14.09 — каждый
    # раз ровно после короткого сна ноутбука (журнал Windows, Kernel-Power 42).
    #
    # Теперь: если run_until_disconnected() вернулся — переподключаемся и
    # входим в него снова, бесконечно. Пока loop не крутится (между обрывом
    # и переподключением), задачи сканера стоят на паузе — это секунды, и
    # это несравнимо лучше, чем бот, которого нет. После восстановления
    # шлём в Telegram пометку, чтобы было видно: связь рвалась.
    # =========================================================================
    _reconnect_delay = 5.0
    while True:
        try:
            client.run_until_disconnected()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as exc:
            print(f"[main] run_until_disconnected завершился ошибкой: {type(exc).__name__}: {exc}")
        print(f"[main] Telegram отключён — бот НЕ завершается, переподключаюсь через {_reconnect_delay:.0f}с.")
        time.sleep(_reconnect_delay)
        try:
            if not client.is_connected():
                client.loop.run_until_complete(client.connect())
            if skyspreads_client is not None and not skyspreads_client.is_connected():
                client.loop.run_until_complete(skyspreads_client.connect())
            print("[main] Telegram переподключён — продолжаю.")
            # notifier планирует отправку на loop — она уйдёт сразу, как только
            # loop снова закрутится внутри run_until_disconnected() ниже.
            try:
                notifier.notify_error(
                    symbol="СВЯЗЬ",
                    error_message="Соединение с Telegram обрывалось (сон ПК/сеть) — бот пережил обрыв и работает дальше.",
                )
            except Exception:
                pass
            _reconnect_delay = 5.0
        except Exception as exc:
            print(f"[main] переподключение не удалось ({type(exc).__name__}: {exc}) — повторю через {_reconnect_delay:.0f}с.")
            _reconnect_delay = min(_reconnect_delay * 2, 60.0)


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
