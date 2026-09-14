# =============================================================================
# scanner.py — АВТОНОМНЫЙ СКАНЕР РЫНКА: сам ищет арбитражные связки
# (спред цены + аномальный фандинг) между биржами через CCXT, не дожидаясь
# сигналов из Telegram-канала. Находит связку -> шлёт вам HTML-алерт в
# Telegram -> (опционально, если AUTO_TRADE_ENABLED=True) сам передаёт её в
# trade_executor.py на исполнение — теми же проверенными путями
# (position_store, pre-check обеих ног, notifier), что и сигналы канала.
# =============================================================================
# ВАЖНО про происхождение данных для реальных денег: сканер НЕ использует
# LLM вообще — coin/long_exchange/short_exchange/цены здесь получены
# напрямую из CCXT (структурные данные), поэтому trade_executor вызывается
# сразу со структурными аргументами, минуя main.py:open_signal() (тот нужен
# только для парсинга ПОЛУХАОТИЧНОГО ТЕКСТА реальных сообщений канала).
# =============================================================================

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from html import escape as html_escape

import ccxt.async_support as ccxt_async

from bot_crew import position_store
from bot_crew import blocked_coins_store
from bot_crew import trade_executor
from bot_crew import trade_ledger
from bot_crew import book_stream
from bot_crew import private_stream
from bot_crew import clock_guard
from bot_crew.config import load_auto_trade_config, load_test_batch_config
from bot_crew import test_batch as test_batch_mod
from bot_crew.test_batch import TestBatchTracker
from bot_crew.tools.trade_tool import (
    EXCHANGE_ALIASES,
    EXCHANGE_QUOTE_CURRENCY,
    TradeExecutionTool,
    _build_symbol,
    _get_usdc_usdt_rate,
    _to_usdt_sync,
)


# =============================================================================
# КОНФИГУРАЦИЯ (переменные окружения, см. .env.example за подробностями и
# дефолтами). Читаются один раз при создании FundingScanner — перезапуск
# бота нужен, чтобы подхватить изменения (как и остальные настройки .env).
# =============================================================================
async def _already(value):
    """Обёртка, чтобы в asyncio.gather() можно было смешивать реальные
    запросы и уже готовые значения (см. check_close_vwap: одна нога взята
    из потока, вторая требует REST)."""
    return value


def _get_bool_env(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() == "true"


def _get_float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _get_int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _get_list_env(name: str, default: str) -> list[str]:
    raw = os.getenv(name, default)
    return [item.strip().lower() for item in raw.split(",") if item.strip()]


# Ставка фандинга у CCXT — доля за один интервал выплаты (например, 0.0001 =
# 0.01% за ближайшую выплату), а НЕ годовая ставка — так же, как её
# показывает сам канал сигналов ("Funding MEXC: 0.02%"), поэтому используем
# ту же шкалу для порога в .env, без пересчёта в APR.


class FundingScanner:
    """Периодически опрашивает несколько бирж (SCANNER_EXCHANGES), ищет
    пары одинаковых монет с ценовым спредом и/или аномальным фандингом,
    шлёт Telegram-алерт и (если AUTO_TRADE_ENABLED=True) передаёт связку на
    исполнение в trade_executor.

    Запускается как фоновая asyncio-задача ПАРАЛЛЕЛЬНО с Telegram-
    листенером — см. main.py:listen() (client.loop.create_task(scanner.run())
    перед client.run_until_disconnected()) — оба используют один и тот же
    event loop, поэтому это не отдельный процесс/поток, а просто ещё одна
    корутина в том же цикле.
    """

    def __init__(self, notifier):
        self.notifier = notifier

        self.exchanges = _get_list_env("SCANNER_EXCHANGES", "bybit,gate,bitget,mexc")
        # float, а не int — по просьбе пользователя 2026-09-07 ("сделай
        # сканер вместо 45 секунд до 0.5 сек"), тот же паттерн, что и
        # test_batch_monitor_interval (TEST_BATCH_MONITOR_INTERVAL_SECONDS).
        self.interval_seconds = _get_float_env("SCANNER_INTERVAL_SECONDS", 60.0)
        # Жёсткий потолок на ОДИН цикл сканирования (см. run()) — защита
        # от зависания на зависшем сетевом вызове. Обычный цикл занимает
        # доли секунды — нескольких секунд, так что 45с — щедрый запас,
        # но всё ещё меньше самого интервала (60с), чтобы циклы не копились.
        self._cycle_timeout_seconds = _get_int_env("SCANNER_CYCLE_TIMEOUT_SECONDS", 45)
        self.spread_threshold_pct = _get_float_env("SCANNER_SPREAD_THRESHOLD_PERCENT", 2.0)
        self.funding_threshold_pct = _get_float_env("SCANNER_FUNDING_THRESHOLD_PERCENT", 0.5)
        # Приоритетный диапазон спреда (%) для ПОРЯДКА попытки входа (см.
        # _find_opportunities) — по явной просьбе пользователя 2026-09-06:
        # если среди найденных связок есть спред 4-6%, пробуем открыть
        # именно её первой; если таких нет — берём остальные найденные
        # (не ниже TEST_BATCH_MIN_SPREAD/AUTO_TRADE_MIN_SPREAD, как обычно).
        self.PRIORITY_SPREAD_MIN = _get_float_env("SCANNER_PRIORITY_SPREAD_MIN", 4.0)
        self.PRIORITY_SPREAD_MAX = _get_float_env("SCANNER_PRIORITY_SPREAD_MAX", 6.0)
        # Минимальная ликвидность (объём за 24ч в USDT, на менее ликвидной
        # из двух бирж связки) — монеты ниже этого порога полностью
        # исключаются из поиска связок (см. _evaluate_pair), а не просто
        # получают низкий приоритет: по явной просьбе пользователя
        # 2026-09-06 после реальных случаев ANSEM/BONER — мгновенное
        # проскальзывание рыночного ордера на тонком стакане "съедает"
        # весь заявленный спред.
        self.min_liquidity_usdt = _get_float_env("SCANNER_MIN_LIQUIDITY_USDT", 50000.0)
        # Больше не используется для подавления повторных алертов (см.
        # _handle_opportunity — по просьбе пользователя 2026-09-06 заменено
        # на проверку "уже в открытой позиции" вместо таймера); оставлено
        # прочитанным из .env на случай, если понадобится вернуть.
        self.cooldown_seconds = _get_int_env("SCANNER_ALERT_COOLDOWN_SECONDS", 1800)
        # Предохранитель от "мусорных" совпадений: одинаковый тикер на двух
        # биржах иногда означает СОВЕРШЕННО РАЗНЫЕ активы (мемкоины,
        # токенизированные акции вроде "XAU"/"*STOCK" на MEXC и т.п.) — это
        # даёт нереально огромный "спред", который на самом деле не спред,
        # а ошибка сопоставления. Выше этого порога — молча пропускаем и
        # логируем, а не шлём вводящий в заблуждение алерт.
        self.max_sane_spread_pct = _get_float_env("SCANNER_MAX_SANE_SPREAD_PERCENT", 50.0)

        # Монеты, которые заведомо не торгуются (биржевые ограничения на
        # testnet/демо-счетах вроде "max contracts 0 at this leverage" для
        # BNB на MEXC) — пропускаем их ещё на этапе поиска связок, чтобы не
        # тратить цикл на заведомо провальную сделку и не засорять лог.
        self.excluded_coins = {
            c.upper() for c in _get_list_env("SCANNER_EXCLUDED_COINS", "BNB")
        }

        # ТОЧЕЧНОЕ исключение "монета+биржа" (см. blocked_coins_store.py) —
        # по прямой просьбе пользователя 2026-09-09: "убери дельту из
        # исключений общих и добавь в исключение только для биржи мекс".
        # Кэшируется здесь (не через blocked_coins_store.is_coin_exchange_
        # excluded() на каждый вызов) — _find_opportunities сравнивает
        # КАЖДУЮ пару бирж КАЖДОЙ монеты каждые 0.5с, парсить строку из
        # .env в этом цикле было бы лишней тратой времени.
        self.excluded_coin_exchange_pairs = blocked_coins_store.parse_coin_exchange_pairs(
            os.getenv("SCANNER_EXCLUDED_COIN_EXCHANGE_PAIRS", "")
        )

        # Монеты, для которых закрытие срабатывает СРАЗУ при net_pnl >= 0,
        # БЕЗ ожидания схождения спреда до close_spread_max_pct — по явной
        # просьбе пользователя 2026-09-07 ("закрой BPUSDT когда она будет
        # в 0") для позиций вроде BP, чей спред застрял на 13-22% и, по
        # данным _check_spread_has_converged_recently, не сходится вообще
        # (см. её комментарий) — обычное условие (r[1] <= close_spread_max_pct
        # AND profitable) для такой монеты может не выполниться НИКОГДА,
        # даже если PnL уже положителен. Пустой список по умолчанию —
        # поведение для остальных монет не меняется.
        self.breakeven_close_coins = {
            c.upper() for c in _get_list_env("SCANNER_BREAKEVEN_CLOSE_COINS", "")
        }

        # COOLDOWN НА ПОВТОРНУЮ ПОПЫТКУ ВХОДА — добавлено 2026-09-07: при
        # интервале сканера 0.5с (см. self.interval_seconds выше) монета,
        # чей last-спред держится выше порога, но по факту (VWAP/bid-ask)
        # всегда отклоняется, повторно пытается открыться КАЖДЫЙ цикл —
        # реальный случай, FONE пытался войти десятки раз подряд за
        # несколько минут, ни разу не пройдя финальную проверку в
        # trade_tool.py. Это НЕ то же самое, что cooldown на АЛЕРТЫ (тот
        # убрали по просьбе пользователя 2026-09-06) — здесь речь только о
        # том, как часто ПОВТОРНО пытаться реально открыть ордера по одной
        # и той же монете после отказа; сама монета не исключается и не
        # перестаёт быть кандидатом — просто не дёргается на каждом цикле.
        # coin -> time.monotonic() до которого пропускаем попытки.
        self._entry_retry_cooldown_seconds = _get_float_env("SCANNER_ENTRY_RETRY_COOLDOWN_SECONDS", 20.0)
        self._entry_cooldown_until: dict[str, float] = {}

        # Короткий кэш результата _check_spread_has_converged_recently (см.
        # там же) — (coin, long_ex, short_ex) -> (time.monotonic() записи,
        # (passed, min_spread_seen)).
        self._convergence_cache: dict = {}

        # "Кандидаты в орфаны" для _reconcile_orphaned_positions (см. там
        # же) — (exchange, coin) -> time.monotonic() первого обнаружения.
        # Реальный инцидент 2026-09-10 (CATE, mexc/aster): реальный ордер
        # на aster УЖЕ исполнился, а position_store ЕЩЁ не успел записать
        # позицию (сама сделка открывается в отдельном потоке — гонка
        # между "ордер отправлен" и "запись в учёт" неизбежна архитектурно,
        # не полностью убирается одиночной свежей проверкой) — sweep
        # застал этот момент, счёл ногу орфаном и закрыл её РЕАЛЬНО, хотя
        # сделка была абсолютно легитимной и завершилась мгновением позже.
        self._pending_orphans: dict = {}
        # Обратная сторона той же проверки — see _reconcile_missing_legs:
        # (coin, exchange_that_is_missing) -> time.monotonic() первого
        # обнаружения. Реальный повод: тот же инцидент CATE — после того,
        # как aster-нога была ошибочно закрыта, mexc-нога осталась
        # непокрытой, а position_store всё ещё думал, что обе ноги открыты.
        self._pending_missing_legs: dict = {}

        # ГРЕЙС-ПЕРИОД ПОСЛЕ ОТКРЫТИЯ (добавлено 2026-09-11 — реальный
        # случай STORJ: реконсиляция закрыла ЧЕТЫРЕ раза подряд абсолютно
        # легитимно открытую позицию, приняв её за орфан/пропавшую ногу,
        # чистый убыток ~$0.17+ на комиссиях/проскальзывании). КОРЕНЬ
        # ПРОБЛЕМЫ: "два подтверждения подряд" (см. _pending_orphans выше)
        # защищает от гонки, только если между двумя прогонами sweep
        # реально проходит достаточно времени, чтобы сделка успела
        # дописаться в position_store. После того как сегодня же убрали
        # искусственную паузу реконсиляции (RECONCILE_INTERVAL_SECONDS=0)
        # сама проверка 8 бирж стала занимать МЕНЬШЕ времени, чем реальное
        # открытие обеих ног (~1.3-1.7с, см. "Задержка между ногами" в
        # логах) — оба подтверждения подряд успевали произойти ДО того,
        # как position_store вообще узнавал о сделке, и "защита от гонки"
        # переставала защищать. Фикс — НЕЗАВИСИМЫЙ от скорости самих
        # sweep'ов: помечаем момент СТАРТА попытки открытия (до отправки
        # ордеров) и полностью игнорируем эту монету в reconcile, пока не
        # прошёл SCANNER_RECONCILE_OPEN_GRACE_SECONDS — реальному открытию
        # с огромным запасом хватает этого времени дописаться в учёт,
        # сколько бы раз sweep ни прошёл за этот период.
        self._recent_open_attempts: dict = {}

        # Когда по каждой бирже в последний раз делали ПОЛНЫЙ REST-снимок
        # позиций (см. _positions_for_reconcile). Приватный поток
        # (private_stream.py) избавляет сверку от постоянного REST, но
        # опираться на него бессрочно нельзя: незамеченное расхождение
        # (пропущенная дельта, рассинхрон после переподключения) иначе жило
        # бы сколь угодно долго. Поэтому REST принудительно вызывается не
        # реже RECONCILE_REST_VERIFY_SECONDS — верхняя граница на возраст
        # любой ошибки потока.
        self._last_rest_verify: dict = {}

        # СОСТОЯНИЕ СИСТЕМНЫХ ЧАСОВ (см. clock_guard.py и _clock_guard_loop).
        # False = часы расходятся с биржами сильнее допустимого, НОВЫЕ
        # ВХОДЫ ЗАПРЕЩЕНЫ (см. _handle_test_batch_opportunity). Стартуем с
        # True, чтобы не блокировать торговлю до первого замера — первый
        # замер делается сразу при запуске цикла, ещё до первого скана.
        self._clock_ok: bool = True
        self._clock_last_alert_at: float = 0.0

        # РАЗОВОЕ ИСКЛЮЧЕНИЕ из правила "1 нога на биржу" (см.
        # busy_exchanges выше) — по явной просьбе пользователя 2026-09-08:
        # "разрешаю сделать разовое исключение для mexc и gate, но только 1
        # раз сейчас". В отличие от остальных настроек .env, это НЕ
        # постоянное правило — потребляется (переключается в False) сразу
        # после ПЕРВОГО УСПЕШНОГО открытия сделки, использовавшей его (см.
        # _handle_test_batch_opportunity), а не при каждом рестарте заново
        # читается как True. Ставится в True вручную в .env непосредственно
        # перед рестартом, когда пользователь просит именно такое разовое
        # исключение — не путать со стандартными постоянными gate-переменными.
        self._busy_exchange_override_available = _get_bool_env("SCANNER_BUSY_EXCHANGE_ONE_TIME_OVERRIDE", False)

        # Конфигурация авто-торговли (AUTO_TRADE_ENABLED/_AMOUNT_USDT/
        # _MIN_SPREAD, MAX_OPEN_POSITIONS) — см. config.py. Отдельный объект,
        # а не разбросанные self.* поля, чтобы было видно, что это ОДНА
        # логическая группа настроек, читаемая и валидируемая в одном месте.
        self.auto_trade = load_auto_trade_config()

        # Режим "тестовой серии" (TEST_BATCH_MODE=True) — открывает РОВНО
        # `size` сделок фиксированным объёмом, затем переключается на
        # мониторинг/закрытие ТОЛЬКО этих позиций (новые связки не ищет),
        # пока не закроет все — тогда шлёт сводный отчёт. См. test_batch.py.
        # ВАЖНО: если включён, он ИСКЛЮЧИТЕЛЬНО управляет открытием сделок —
        # общий AUTO_TRADE_ENABLED в этом режиме игнорируется (см.
        # _handle_opportunity), чтобы не смешивать два независимых счётчика
        # решений "открыть сделку".
        test_batch_cfg = load_test_batch_config()
        self.test_batch_monitor_interval = test_batch_cfg.monitor_interval_seconds
        self.test_batch_safe_exchanges = test_batch_cfg.safe_exchanges
        self.test_batch: "TestBatchTracker | None" = (
            TestBatchTracker(
                size=test_batch_cfg.size,
                amount_usdt=test_batch_cfg.amount_usdt,
                min_spread_pct=test_batch_cfg.min_spread_pct,
                close_spread_pct=test_batch_cfg.close_spread_pct,
                close_spread_max_pct=test_batch_cfg.close_spread_max_pct,
            )
            if test_batch_cfg.enabled
            else None
        )
        if self.test_batch is not None:
            self._bootstrap_test_batch_from_position_store()

        # Постоянные (переиспользуемые между циклами) ПУБЛИЧНЫЕ клиенты
        # CCXT — без API-ключей, т.к. цены/фандинг это открытые данные.
        # Пересоздавать клиент на каждый цикл незачем (лишние TLS-хендшейки,
        # лишняя нагрузка на rate limit) — держим по одному на биржу.
        self._clients: dict[str, "ccxt_async.Exchange"] = {}
        self._last_alert_at: dict[tuple, float] = {}

        print(
            "[scanner] Инициализация: биржи="
            + ", ".join(self.exchanges)
            + f"; интервал={self.interval_seconds}с; порог спреда={self.spread_threshold_pct}%;"
            + f" порог фандинга={self.funding_threshold_pct}%; AUTO_TRADE_ENABLED={self.auto_trade.enabled}"
            + (
                f" (amount=${self.auto_trade.amount_usdt}/нога,"
                f" min_spread={self.auto_trade.min_spread_percent}%,"
                f" max_positions={self.auto_trade.max_open_positions})"
                if self.auto_trade.enabled else ""
            )
        )
        if self.test_batch is not None:
            print(
                f"[test_batch] ВКЛЮЧЁН: "
                f"{'НЕПРЕРЫВНЫЙ РЕЖИМ (без лимита сделок)' if self.test_batch.unlimited else f'серия из {self.test_batch.size} сделок'} по "
                f"${self.test_batch.amount_usdt}, вход при спреде >= {self.test_batch.min_spread_pct}%, "
                f"закрытие при спреде {self.test_batch.close_spread_pct}%-"
                f"{self.test_batch.close_spread_max_pct}% и прибыли, "
                f"интервал мониторинга {self.test_batch_monitor_interval}с, "
                f"безопасные биржи: {', '.join(sorted(self.test_batch_safe_exchanges))}. "
                f"AUTO_TRADE_ENABLED игнорируется, пока идёт серия."
            )

    # -------------------------------------------------------------------
    # _bootstrap_test_batch_from_position_store — восстанавливает
    # TestBatchTracker.open_positions (живёт ТОЛЬКО в памяти процесса, см.
    # test_batch.py) из position_store.json (переживает рестарт) при
    # старте бота. Без этого перезапуск бота во время активной серии
    # "теряет" из виду реально открытые позиции — они остаются висеть на
    # биржах, но серия думает, что их не было, и открывает новые связки
    # поверх (проверено 2026-09-06: реальная позиция LTC, открытая ДО
    # рестарта, иначе выпала бы из мониторинга/счётчика "N/15").
    # entry_spread_pct не хранится в position_store — пересчитываем его из
    # сохранённых цен входа (та же формула, что и у живого сравнения
    # бирж), это даёт то же значение с точностью до микроскопических
    # различий округления.
    # -------------------------------------------------------------------
    def _bootstrap_test_batch_from_position_store(self) -> None:
        for coin, pos in position_store.list_positions().items():
            # В непрерывном режиме (size<=0) лимита нет — восстанавливаем
            # ВСЕ найденные в учёте позиции, иначе часть осталась бы без
            # мониторинга после рестарта.
            if not self.test_batch.unlimited and self.test_batch.opened_count >= self.test_batch.size:
                break
            try:
                long_exchange = pos["long_exchange"]
                short_exchange = pos["short_exchange"]
                long_price = pos["long_entry_price"]
                short_price = pos["short_entry_price"]
                entry_spread_pct = (short_price - long_price) / long_price * 100
                self.test_batch.record_open(
                    coin,
                    long_exchange=long_exchange,
                    short_exchange=short_exchange,
                    long_symbol=_build_symbol(long_exchange, coin),
                    short_symbol=_build_symbol(short_exchange, coin),
                    entry_spread_pct=entry_spread_pct,
                    long_entry_price=long_price,
                    short_entry_price=short_price,
                    amount_usdt=pos.get("amount_usdt", self.test_batch.amount_usdt),
                    entry_fee_total=(
                        # БАГ 2026-09-08: .get(key, 0.0) подставляет дефолт
                        # ТОЛЬКО когда ключа нет вообще — если ключ есть, но
                        # его значение явно null/None (реальный случай: у
                        # FONE long_entry_fee_usdt=null, т.к. lookup ставки
                        # комиссии для одной из ног не удался при открытии),
                        # .get() всё равно вернул None, и None + 0.0016
                        # падал с TypeError, полностью срывая восстановление
                        # позиции после рестарта (бот переставал её
                        # отслеживать вообще, хотя она реально открыта на
                        # бирже). "or 0.0" безопасно подставляет 0.0 и для
                        # отсутствующего ключа, и для явного None.
                        (pos.get("long_entry_fee_usdt") or 0.0) + (pos.get("short_entry_fee_usdt") or 0.0)
                        if pos.get("long_entry_fee_usdt") is not None or pos.get("short_entry_fee_usdt") is not None
                        else None
                    ),
                    # ИСПРАВЛЕНО 2026-09-12 (реальный случай ANTHROPIC) —
                    # раньше wide_spread тут не передавался вообще, и
                    # ЛЮБАЯ "широкая" позиция (правило 2%-тейк-профит без
                    # лимита) при каждом рестарте бота тихо превращалась в
                    # обычную (правило: спред <=1% и прибыль >=$0.15) —
                    # пометка хранилась ТОЛЬКО в памяти, а не в файле.
                    # Теперь _finalize_open пишет её в position_store, и
                    # здесь она читается обратно.
                    wide_spread=bool(pos.get("wide_spread")),
                )
                print(
                    f"[test_batch] Восстановлена позиция {coin} из position_store после рестарта: "
                    f"LONG {long_exchange}/{short_exchange} SHORT, спред входа {entry_spread_pct:.2f}%."
                )
            except Exception as exc:
                print(f"[test_batch] Не удалось восстановить позицию {coin} из position_store: {exc}")

    # -------------------------------------------------------------------
    # run() — основной бесконечный цикл. Каждая итерация обёрнута в
    # try/except — сбой ОДНОГО цикла (сетевой сбой биржи и т.п.) не должен
    # останавливать сканер навсегда, только пропустить этот цикл.
    # -------------------------------------------------------------------
    async def run(self) -> None:
        print("[scanner] Запущен.")
        # НЕЗАВИСИМЫЙ фоновый цикл мониторинга открытых позиций серии (см.
        # _position_monitor_loop) — запущен ОТДЕЛЬНОЙ задачей, параллельно
        # основному циклу поиска новых связок ниже. КРИТИЧНО (фикс
        # 2026-09-06): раньше проверка цен уже открытых позиций (LTC и
        # т.п.) была частью ТОГО ЖЕ последовательного цикла, что и поиск
        # новых связок — если поиск находил МНОГО связок за один проход
        # (например, XRP с устойчиво неверной ценой на Bitget даёт связку
        # против каждой из 3 остальных бирж КАЖДЫЙ цикл, и каждая попытка
        # открыть/откатить ногу — это несколько секунд реальных сетевых
        # вызовов), проверка уже открытой прибыльной позиции откладывалась
        # вместе с этим и могла не срабатывать быстрее, чем раз в 45-60с,
        # несмотря на настроенный "быстрый" интервал в 3с. Отдельная
        # задача гарантирует, что открытые позиции проверяются на
        # схождение спреда СВОИМ темпом, независимо от того, сколько
        # времени занимает поиск новых связок.
        self._monitor_task = asyncio.create_task(self._position_monitor_loop())
        # См. _reconcile_positions_loop — независимая защитная сверка
        # реальных позиций на биржах против position_store, по прямой
        # просьбе пользователя 2026-09-10 после реального инцидента с
        # неучтённой позицией LAB на MEXC (-$2.57).
        self._reconcile_task = asyncio.create_task(self._reconcile_positions_loop())
        # См. _daily_report_loop — раз в сутки шлёт сводку (задержки на
        # ноги + PnL за сутки) в @Depositik/@G_Pobedonosec, по прямой
        # просьбе пользователя 2026-09-12.
        self._daily_report_task = asyncio.create_task(self._daily_report_loop())
        # См. _clock_guard_loop / clock_guard.py — контроль системных часов
        # против времени бирж, добавлен 2026-09-14 после реального случая,
        # когда часы ПК отставали на 3ч42м и ни один подписанный запрос не
        # проходил, а бот при этом выглядел исправным.
        self._clock_guard_task = asyncio.create_task(self._clock_guard_loop())
        try:
            while True:
                cycle_start = time.monotonic()
                # ОБНОВЛЕНИЕ SCANNER_EXCLUDED_COIN_EXCHANGE_PAIRS КАЖДЫЙ
                # ЦИКЛ (добавлено 2026-09-12 — реальный случай MICRODUCK):
                # auto_exclude_coin_on_exchange() пишет новую пару в .env
                # и os.environ МГНОВЕННО в момент отката, но self.excluded_
                # coin_exchange_pairs раньше парсился ОДИН РАЗ в __init__ —
                # монета, которую только что исключили, продолжала
                # находиться и пытаться открыться СОТНИ раз за тот же
                # запуск бота, пока кто-то не перезапустит процесс вручную.
                # Разбор короткой строки из os.environ — не сетевой вызов,
                # микросекунды, поэтому делать это КАЖДЫЙ цикл (а не только
                # при старте) ничего не стоит, зато новое исключение
                # подхватывается сразу же, без рестарта.
                self.excluded_coin_exchange_pairs = blocked_coins_store.parse_coin_exchange_pairs(
                    os.getenv("SCANNER_EXCLUDED_COIN_EXCHANGE_PAIRS", "")
                )
                try:
                    # ЗАЩИТА ОТ ЗАВИСАНИЯ: без явного таймаута один
                    # зависший сетевой вызов (биржа не отвечает, но и не
                    # рвёт соединение) блокирует ВЕСЬ цикл сканирования
                    # НАВСЕГДА — проверено 2026-09-05: процесс оставался
                    # "живым" (отвечал ОС), но не проходил ни одного нового
                    # цикла ~20 минут. asyncio.wait_for гарантирует, что
                    # ПОИСК связок максимум "зависнет" на CYCLE_TIMEOUT
                    # секунд, после чего сработает TimeoutError, попадёт в
                    # except ниже, и цикл продолжится дальше как обычно.
                    #
                    # ВАЖНО (фикс 2026-09-06): этот таймаут оборачивает
                    # ТОЛЬКО поиск (_find_opportunities_once) — САМО
                    # исполнение сделки (_handle_opportunities) сюда
                    # намеренно НЕ входит. Проверено на реальном ордере:
                    # если завернуть сюда и исполнение, при долгой попытке
                    # (лестница сумм, откат) таймаут обрывает ОЖИДАНИЕ
                    # результата, но не сам фоновый поток (run_in_executor
                    # не отменяется отменой awaiting-корутины) — тот
                    # продолжает работать НЕВИДИМО для нас, а следующий
                    # цикл в это время стартует и, видя open_positions ещё
                    # пустым, открывает ВТОРУЮ сделку параллельно с первой
                    # (реальный случай: ALL и BP открылись одновременно,
                    # хотя лимит — не более 1 позиции).
                    opportunities = await asyncio.wait_for(
                        self._find_opportunities_once(), timeout=self._cycle_timeout_seconds
                    )
                    # Обработка (открытие сделок) — БЕЗ строгого лимита:
                    # собственные таймауты внутри trade_tool.py (15с на
                    # сетевой запрос, retry с задержками) уже защищают от
                    # истинного зависания, а искусственно обрывать честно
                    # выполняющуюся попытку открытия — как раз то, что
                    # вызвало гонку выше.
                    await self._handle_opportunities(opportunities)

                    # ФОНОВЫЙ ПРОГРЕВ ПЛЕЧА (добавлено 2026-09-14 по прямой
                    # просьбе пользователя "сделать так, чтобы везде стояло
                    # 5 плечо и тратить на открытие ещё меньше времени").
                    # Запускаем ПОСЛЕ обработки связок и НЕ ждём результата:
                    # это чистая подготовка на будущее, она не должна ни на
                    # миллисекунду задерживать текущий цикл или сделку.
                    asyncio.create_task(self._warm_leverage_for_candidates(opportunities))
                except asyncio.TimeoutError:
                    print(
                        f"[scanner] Цикл сканирования не уложился в "
                        f"{self._cycle_timeout_seconds}с — прерываю и продолжаю "
                        f"со следующего цикла (см. комментарий про защиту от зависания)."
                    )
                except Exception as exc:
                    print(f"[scanner] Ошибка в цикле сканирования: {type(exc).__name__}: {exc}")

                # HEARTBEAT: печатаем КАЖДЫЙ цикл, даже если ничего не
                # найдено (обычно _scan_once молчит в этом случае) — иначе
                # "нет новых строк в логе" не отличить от настоящего
                # зависания (внешний watchdog, см. watchdog.ps1, ориентируется
                # именно на свежесть listen.log).
                print(
                    f"[heartbeat] {time.strftime('%Y-%m-%d %H:%M:%S')} цикл завершён "
                    f"за {time.monotonic() - cycle_start:.1f}с, состояние серии="
                    f"{self.test_batch.state if self.test_batch else 'n/a'}"
                )

                # Скорость проверки УЖЕ ОТКРЫТЫХ позиций больше не зависит
                # от этого цикла — см. _position_monitor_loop (отдельная
                # задача, свой темп test_batch_monitor_interval). Этот цикл
                # — чисто поиск НОВЫХ связок, обычный интервал.
                interval = self.interval_seconds

                # Учитываем время, потраченное на сам цикл, чтобы интервал
                # между СТАРТАМИ циклов был примерно постоянным, а не
                # "interval + время цикла" каждый раз. Пол снижен с 1.0 до
                # 0.05с (2026-09-07), затем до 0.02с (2026-09-09, по прямой
                # просьбе пользователя "сделай мониторинг вообще без
                # задержки... круговое движение всего") — сам цикл (запрос
                # данных с 8 бирж) в любом случае занимает секунды, так что
                # "interval - elapsed" почти всегда < 0 и пол — единственное,
                # что реально применяется. Малый пол (не 0) оставлен НЕ как
                # "время ожидания", а как техническая защита от чистого
                # busy-loop (100% CPU впустую) при ошибке/пустом цикле.
                elapsed = time.monotonic() - cycle_start
                await asyncio.sleep(max(0.02, interval - elapsed))
        finally:
            await self._close_clients()

    async def _close_clients(self) -> None:
        for exchange in self._clients.values():
            try:
                await exchange.close()
            except Exception:
                pass

    # -------------------------------------------------------------------
    # _get_client — публичный (без ключей) клиент CCXT для биржи,
    # создаётся один раз и переиспользуется. load_markets() тоже
    # выполняется один раз при создании (при очень долгой работе бота
    # список рынков теоретически может устареть — это сознательное
    # упрощение первой версии сканера).
    # -------------------------------------------------------------------
    async def _get_client(self, exchange_id: str):
        if exchange_id in self._clients:
            return self._clients[exchange_id]
        if not hasattr(ccxt_async, exchange_id):
            print(f"[scanner] Биржа '{exchange_id}' из SCANNER_EXCHANGES не поддерживается CCXT — пропускаю.")
            return None
        exchange_class = getattr(ccxt_async, exchange_id)
        # timeout — жёсткий предел на любой сетевой запрос: без него один
        # зависший (не оборвавшийся, просто "молчащий") вызов может
        # заблокировать цикл сканера намного дольше, чем спасает
        # asyncio.wait_for в run() (та защита работает, только если сама
        # корутина умеет реагировать на отмену — зависший запрос без
        # timeout иногда не реагирует вовсе). Проверено 2026-09-05.
        exchange = exchange_class({"enableRateLimit": True, "options": {"defaultType": "swap"}, "timeout": 15000})

        # ВАЖНО: Gate's demo-режим (см. trade_tool.py) — это НАСТОЯЩИЙ
        # отдельный testnet (api-testnet.gateapi.io), а не тот же прод с
        # доп. заголовком (как у bybit/bitget) — и у него МЕНЬШЕ рынков,
        # чем на проде (проверено 2026-09-05: 6033 против 6579 — многие
        # мелкие/новые монеты вроде ASTEROID там просто не листингованы).
        # Если сканер ищет связки по ПРОДУ, а торговать потом придётся на
        # testnet — получаем "market not found" на ровном месте для целой
        # категории монет. Поэтому сканер тоже смотрит на testnet-список
        # Gate, пока идёт demo/тестовая торговля — видит ровно ту
        # вселенную монет, которую реально можно будет открыть.
        if exchange_id == "gate" and os.getenv("DEMO_TRADING", "False").lower() == "true":
            exchange.set_sandbox_mode(True)
        elif exchange_id == "mexc" and os.getenv("DEMO_TRADING", "False").lower() == "true":
            # Тот же случай, что и с Gate выше: MEXC testnet — ОТДЕЛЬНАЯ
            # вселенная рынков (проверено 2026-09-05: 2215 против 3235 на
            # проде) — переключаем URL руками (см. тот же приём в
            # trade_tool.py:_build_exchange_client), иначе сканер находит
            # связки, которые потом физически не открыть через testnet-ключ.
            exchange.urls["api"]["contract"] = {
                "public": "https://futures.testnet.mexc.com/api/v1/contract",
                "private": "https://futures.testnet.mexc.com/api/v1/private",
            }
        elif exchange_id == "bitget" and os.getenv("DEMO_TRADING", "False").lower() == "true":
            # Тот же случай, что и с Gate/MEXC выше, только СИЛЬНЕЕ:
            # Bitget paper trading (PAPTRADING=1) поддерживает лишь ЧАСТЬ
            # контрактов продакшна (проверено 2026-09-06: 83 рынка в demo
            # против 2150 на проде) — без этого сканер массово находит
            # связки (например, WOO спред 5.56%), которые физически нельзя
            # открыть демо-ключом ("does not have market symbol").
            exchange.enable_demo_trading(True)

        try:
            await exchange.load_markets()
        except Exception as exc:
            print(f"[scanner] Не удалось загрузить рынки {exchange_id}: {exc}")
            await exchange.close()
            return None
        self._clients[exchange_id] = exchange
        return exchange

    # -------------------------------------------------------------------
    # _position_monitor_loop — НЕЗАВИСИМАЯ фоновая задача (см. run()),
    # проверяющая открытые позиции серии на схождение спреда СВОИМ
    # СОБСТВЕННЫМ темпом (test_batch_monitor_interval, по умолчанию 3с),
    # НЕ дожидаясь основного цикла поиска новых связок. КРИТИЧНО (фикс
    # 2026-09-06): раньше эта проверка была частью ТОГО ЖЕ
    # последовательного цикла, что и поиск — если поиск находил много
    # связок за один проход (реальные попытки открыть/откатить ногу — это
    # секунды сетевых вызовов на каждую), проверка уже открытой позиции
    # откладывалась вместе с этим и могла срабатывать не быстрее, чем раз
    # в 45-60с, несмотря на настроенный "быстрый" интервал. Работает,
    # пока серия не завершена (DONE) — ЛЮБОЕ состояние (и PROSPECTING, и
    # MONITORING), если есть хотя бы одна открытая позиция.
    # -------------------------------------------------------------------
    async def _position_monitor_loop(self) -> None:
        while True:
            if self.test_batch is None or self.test_batch.state == test_batch_mod.DONE:
                return
            try:
                # Синхронизация потоковых подписок — БЕЗУСЛОВНО, вне
                # проверки на наличие позиций: когда закрывается последняя,
                # _monitor_test_batch() уже не вызывается, и висящие
                # подписки надо снять именно здесь.
                await self._sync_book_streams()
                if self.test_batch.open_positions:
                    await asyncio.wait_for(self._monitor_test_batch(), timeout=self._cycle_timeout_seconds)
            except asyncio.TimeoutError:
                print("[test_batch] Проверка открытых позиций не уложилась в таймаут — пробую снова следующим тиком.")
            except Exception as exc:
                print(f"[test_batch] Ошибка при мониторинге открытых позиций: {type(exc).__name__}: {exc}")
            # По прямой просьбе пользователя 2026-09-09 ("сделай мониторинг
            # вообще без задержки") — TEST_BATCH_MONITOR_INTERVAL_SECONDS
            # теперь может быть 0. Небольшой пол (0.02с) оставлен НЕ как
            # "время ожидания" в смысле пользователя, а как техническая
            # защита от чистого busy-loop (100% CPU впустую), когда
            # открытых позиций нет вообще — при НАЛИЧИИ позиций реальная
            # скорость всё равно определяется сетевыми запросами внутри
            # _monitor_test_batch, а не этой паузой.
            await asyncio.sleep(max(0.02, self.test_batch_monitor_interval))

    # -------------------------------------------------------------------
    # _reconcile_positions_loop / _reconcile_orphaned_positions — ЗАЩИТНАЯ
    # СВЕРКА: по прямой просьбе пользователя 2026-09-10, после РЕАЛЬНОГО
    # инцидента (LAB на MEXC, убыток -$2.57): бот посчитал ордер на MEXC
    # "не исполненным" (status=1, filled=0 после всех попыток проверки) и
    # откатил вторую ногу (gate) — а MEXC-ордер РЕАЛЬНО исполнился чуть
    # позже, оставив голую незахеджированную позицию, которую position_
    # store вообще не видел (нашли только ручной проверкой). Причина
    # могла быть разной (гонка состояний на бирже, наш баг, что угодно
    # ещё) — вместо того чтобы гоняться за каждой конкретной причиной,
    # это НЕЗАВИСИМАЯ защита: периодически сверяет РЕАЛЬНЫЕ открытые
    # позиции на каждой торгуемой бирже с тем, что знает position_store,
    # и если находит "лишнюю" (биржа думает, что позиция открыта, а мы —
    # нет) — сразу алертит И закрывает автоматически, не дожидаясь, пока
    # кто-то заметит вручную.
    # -------------------------------------------------------------------
    async def _reconcile_positions_loop(self) -> None:
        # По прямой просьбе пользователя 2026-09-10 — без искусственной
        # паузы (RECONCILE_INTERVAL_SECONDS=0), тот же принцип, что и у
        # остальных двух циклов (сканирование, мониторинг закрытия): пол
        # 0.02с — не "время ожидания", а техническая защита от чистого
        # busy-loop, если сам прогон вдруг завершится мгновенно. Сама
        # сверка (8 бирж, реальные сетевые запросы) и так занимает
        # секунды — ДВА ПОДТВЕРЖДЕНИЯ ПОДРЯД (см. _reconcile_orphaned_
        # positions) от этого не страдают: легитимная сделка успевает
        # записаться в position_store (обычно 1.5-5с на открытие) задолго
        # до того, как следующий прогон дойдёт до той же биржи повторно.
        interval = _get_float_env("RECONCILE_INTERVAL_SECONDS", 60.0)
        while True:
            try:
                await self._reconcile_orphaned_positions()
            except Exception as exc:
                print(f"[reconcile] Ошибка сверки позиций: {type(exc).__name__}: {exc}")
            await asyncio.sleep(max(0.02, interval))

    # -------------------------------------------------------------------
    # _daily_report_loop — раз в сутки (вскоре после полуночи UTC) шлёт
    # сводку за ПРОШЕДШИЕ сутки (открытия/закрытия, средняя задержка между
    # ногами на вход и на выход, итоговый PnL) в @Depositik/@G_Pobedonosec
    # — по прямой просьбе пользователя 2026-09-12. Дата последней отправки
    # хранится в маленьком JSON-файле (тот же принцип, что и у
    # position_store/blocked_coins_store) — переживает рестарт бота, не
    # шлёт отчёт повторно за уже отправленные сутки, даже если бот
    # перезапускали несколько раз в течение дня.
    # -------------------------------------------------------------------
    async def _clock_guard_loop(self) -> None:
        """Периодически сверяет системные часы с биржами (см. clock_guard.py).

        Первый замер — НЕМЕДЛЕННО при старте, чтобы бот с кривыми часами
        не успел даже начать искать входы. Дальше — раз в
        CLOCK_CHECK_INTERVAL_SECONDS: часы могут уехать и посреди работы
        (сон/пробуждение ноутбука, смена часового пояса, ручная правка).

        Алерт при проблеме — владельцу и обоим получателям отчётов
        (@Depositik/@G_Pobedonosec): это не техническая мелочь, а полная
        остановка торговли. Повторяем не чаще раза в CLOCK_ALERT_EVERY_
        SECONDS, пока проблема держится, и отдельно сообщаем о
        восстановлении — иначе непонятно, можно ли уже перестать волноваться."""
        interval = _get_float_env("CLOCK_CHECK_INTERVAL_SECONDS", 120.0)
        realert_every = _get_float_env("CLOCK_ALERT_EVERY_SECONDS", 600.0)
        # ДВА ПЛОХИХ ЗАМЕРА ПОДРЯД, прежде чем блокировать торговлю — тот же
        # принцип "перепроверь, не спеши", что и у сверки позиций. Первый
        # боевой запуск 2026-09-14 дал ложную тревогу с одного замера под
        # стартовой нагрузкой; блокировать входы по единичному шумному
        # числу нельзя. Подозрительный замер перепроверяем быстро (через
        # CLOCK_RECHECK_SECONDS), а не через полный интервал.
        recheck_after = _get_float_env("CLOCK_RECHECK_SECONDS", 20.0)
        suspicious = False
        while True:
            sleep_for = max(10.0, interval)
            try:
                offset = await clock_guard.measure_offset()
                if offset is None:
                    print("[clock] не удалось получить время ни одной опорной биржи — пропускаю проверку (сеть?).")
                elif abs(offset) > clock_guard.max_offset_seconds() and not suspicious and self._clock_ok:
                    # Первое подозрение — только запоминаем и быстро перепроверяем.
                    suspicious = True
                    sleep_for = recheck_after
                    print(f"[clock] подозрение: {clock_guard.describe(offset)} — перепроверю через {recheck_after:.0f}с.")
                elif abs(offset) > clock_guard.max_offset_seconds():
                    suspicious = False
                    was_ok = self._clock_ok
                    self._clock_ok = False
                    now = time.monotonic()
                    if was_ok or now - self._clock_last_alert_at >= realert_every:
                        self._clock_last_alert_at = now
                        msg = (
                            f"🕒 {clock_guard.describe(offset)}. Биржи отвергают ВСЕ подписанные "
                            f"запросы (recvWindow/REQUEST_EXPIRED) — бот НЕ МОЖЕТ ни открывать, "
                            f"ни закрывать позиции. НОВЫЕ ВХОДЫ ЗАПРЕЩЕНЫ до исправления. "
                            f"Починить: Параметры → Время и язык → «Синхронизировать сейчас», "
                            f"либо от администратора: net start w32time && w32tm /resync /force."
                        )
                        print(f"[clock] {msg}")
                        try:
                            self.notifier.notify_error(symbol="СИСТЕМНЫЕ ЧАСЫ", error_message=msg)
                        except Exception:
                            pass
                else:
                    suspicious = False
                    if not self._clock_ok:
                        self._clock_ok = True
                        msg = f"✅ Часы выровнены ({clock_guard.describe(offset)}) — торговля разрешена снова."
                        print(f"[clock] {msg}")
                        try:
                            self.notifier.notify_error(symbol="СИСТЕМНЫЕ ЧАСЫ", error_message=msg)
                        except Exception:
                            pass
                    else:
                        print(f"[clock] ок: расхождение с биржами {offset:+.2f} с.")
            except Exception as exc:
                print(f"[clock] ошибка проверки часов: {type(exc).__name__}: {exc}")
            await asyncio.sleep(sleep_for)

    async def _daily_report_loop(self) -> None:
        state_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            "daily_report_state.json",
        )

        def _load_last_sent():
            try:
                with open(state_path, "r", encoding="utf-8") as f:
                    return json.loads(f.read()).get("last_sent_date")
            except (OSError, json.JSONDecodeError):
                return None

        def _save_last_sent(date_str: str):
            try:
                with open(state_path, "w", encoding="utf-8") as f:
                    json.dump({"last_sent_date": date_str}, f)
            except OSError as exc:
                print(f"[daily-report] не удалось сохранить состояние: {exc}")

        last_sent = _load_last_sent()
        # При самом первом запуске (файла ещё нет) НЕ шлём отчёт немедленно
        # за "вчера" — незачем присылать сводку в момент случайного
        # рестарта посреди дня. Просто запоминаем текущую дату как
        # "последнюю отправленную" и ждём следующей смены суток.
        if last_sent is None:
            last_sent = datetime.now(timezone.utc).date().isoformat()
            _save_last_sent(last_sent)

        while True:
            await asyncio.sleep(600)  # проверка раз в 10 минут — не нужна ежесекундная точность
            today_str = datetime.now(timezone.utc).date().isoformat()
            if today_str == last_sent:
                continue
            # Дата сменилась — отчёт за ПРОШЕДШИЕ сутки (last_sent, ещё не
            # отправленные), не за today_str (те только начались).
            try:
                stats = trade_ledger.daily_stats(target_date=datetime.fromisoformat(last_sent).date())
                self.notifier.notify_daily_report(
                    date_str=stats["date"],
                    opens_count=stats["opens_count"],
                    closes_count=stats["closes_count"],
                    avg_open_latency_ms=stats["avg_open_latency_ms"],
                    avg_close_latency_ms=stats["avg_close_latency_ms"],
                    total_net_pnl=stats["total_net_pnl"],
                    wins=stats["wins"],
                    losses=stats["losses"],
                )
                print(f"[daily-report] Отправлен отчёт за {stats['date']}.")
            except Exception as exc:
                print(f"[daily-report] Ошибка формирования/отправки отчёта: {exc}")
            last_sent = today_str
            _save_last_sent(last_sent)

    def _in_open_grace_period(self, coin: str) -> bool:
        """True, если попытка открыть эту монету стартовала недавно (см.
        self._recent_open_attempts в __init__) — reconcile должен ПОЛНОСТЬЮ
        пропустить её на этом прогоне, не начиная даже первое обнаружение
        орфана/пропавшей ноги, пока не прошёл грейс-период."""
        started_at = self._recent_open_attempts.get(coin.upper())
        if started_at is None:
            return False
        grace_seconds = _get_float_env("SCANNER_RECONCILE_OPEN_GRACE_SECONDS", 20.0)
        elapsed = time.monotonic() - started_at
        if elapsed >= grace_seconds:
            # Грейс-период истёк — можно забыть отметку (заодно не даёт
            # словарю расти бесконечно за время жизни бота).
            self._recent_open_attempts.pop(coin.upper(), None)
            return False
        return True

    # -------------------------------------------------------------------
    # _positions_for_reconcile — ОТКУДА сверка берёт список позиций биржи.
    #
    # Добавлено 2026-09-14 по прямой просьбе пользователя ("нельзя ли
    # использовать такой же метод как со стаканом, чтобы обновлять данные
    # по открытым ордерам без задержки"). Сверка крутится непрерывно и
    # раньше на КАЖДОМ прогоне дёргала fetch_positions по всем семи
    # биржам — постоянная нагрузка ради данных, которые почти всегда не
    # меняются.
    #
    # ПРАВИЛО БЕЗОПАСНОСТИ (подробно — в шапке private_stream.py): поток
    # принимается ТОЛЬКО как подтверждение того, что всё совпадает с
    # учётом. Стоит картинке разойтись хоть в одну сторону — немедленно
    # идём по REST и дальше работаем ИСКЛЮЧИТЕЛЬНО с ответом REST.
    #
    # Это принципиально, потому что расхождение ведёт к ДЕЙСТВИЯМ С
    # ДЕНЬГАМИ: автозакрытию «неучтённой» позиции и закрытию оставшейся
    # ноги при «пропавшей». Ошибись поток в пустую сторону (молча умерло
    # соединение, пропущенная дельта) — бот закрыл бы живую позицию. При
    # таком порядке цена ошибки потока равна нулю: он способен лишь
    # сэкономить REST-запрос там, где и так всё в порядке, и не способен
    # ничего решить там, где что-то не так.
    #
    # Возвращает (позиции, источник) — источник попадает в лог, чтобы
    # всегда было видно, чем именно бот сейчас пользуется.
    # -------------------------------------------------------------------
    async def _positions_for_reconcile(self, tool, exchange_name: str, expected_coins: set):
        async def _rest():
            client = await tool._get_ready_client(exchange_name)
            params = {"settle": "usdt"} if exchange_name == "gate" else {}
            positions = await client.fetch_positions(params=params)
            self._last_rest_verify[exchange_name] = time.monotonic()
            return positions

        streamed = private_stream.get_positions(exchange_name)
        if streamed is None:
            return await _rest(), "REST"

        # Принудительная периодическая сверка по REST — верхняя граница на
        # то, сколько может прожить незамеченное расхождение в потоке.
        #
        # ПОЧЕМУ ДВА РАЗНЫХ ИНТЕРВАЛА. Есть один сценарий, который
        # сравнение "поток против учёта" поймать не может в принципе: если
        # нога РЕАЛЬНО закрылась на бирже, а поток потерял это обновление
        # (сообщение не дошло, но соединение не оборвалось — переподключения,
        # а значит и пересева, не произошло), то поток и учёт согласованно
        # врут одно и то же, расхождения нет, и REST не вызывается. Живём с
        # этим ровно до следующей плановой сверки.
        #
        # Цена такой задержки совершенно разная в двух состояниях. Когда на
        # бирже НЕТ наших ног, ошибка не стоит ничего — терять нечего, и
        # редкий REST оправдан. Когда нога ЕСТЬ, это потенциально
        # незахеджированная позиция, а незамеченный голый риск —
        # исторически самая дорогая поломка этого бота (LAB -$2.57, CATE,
        # STORJ). Поэтому при открытых ногах сверяемся заметно чаще: один
        # REST-запрос в минуту на биржу — ничтожная цена против этого риска.
        if expected_coins:
            verify_after = _get_float_env("RECONCILE_REST_VERIFY_OPEN_SECONDS", 60.0)
        else:
            verify_after = _get_float_env("RECONCILE_REST_VERIFY_SECONDS", 300.0)
        last = self._last_rest_verify.get(exchange_name)
        if last is None or time.monotonic() - last > verify_after:
            return await _rest(), "REST (плановая сверка потока)"

        live_coins = set()
        for p in streamed:
            if abs(p.get("contracts") or 0) <= 0:
                continue
            symbol = p.get("symbol") or ""
            coin = symbol.split("/")[0].upper() if "/" in symbol else symbol.upper()
            if coin:
                live_coins.add(coin)

        if live_coins != expected_coins:
            # РАСХОЖДЕНИЕ — решает только REST, поток здесь лишь повод
            # посмотреть внимательнее (и посмотреть НЕМЕДЛЕННО, а не на
            # следующем прогоне: в этом и состоит выигрыш в отклике).
            print(
                f"[reconcile] {exchange_name}: поток показывает расхождение с учётом "
                f"(поток={sorted(live_coins) or '—'}, учёт={sorted(expected_coins) or '—'}) "
                f"— перепроверяю по REST."
            )
            return await _rest(), "REST (расхождение в потоке)"

        return streamed, "поток"

    async def _reconcile_orphaned_positions(self) -> None:
        tool = TradeExecutionTool()
        tracked = position_store.list_positions()
        # Множество (биржа, монета), которые МЫ считаем открытыми сейчас —
        # по ОБЕИМ ногам каждой известной позиции.
        tracked_pairs = set()
        for coin, pos in tracked.items():
            for key in ("long_exchange", "short_exchange"):
                ex = (pos.get(key) or "").lower()
                if ex:
                    tracked_pairs.add((ex, coin.upper()))

        # Приватные потоки поднимаются лениво и идемпотентно — здесь, а не
        # при старте бота: сверка и так первый цикл, который ходит по всем
        # биржам, а любая неудача подписки не мешает ей работать по REST.
        try:
            await tool.ensure_private_streams(self.test_batch_safe_exchanges)
        except Exception as exc:
            print(f"[private-stream] не удалось поднять потоки: {type(exc).__name__}: {exc}")

        orphans_found = 0
        from_stream = 0
        checked_exchanges = 0
        checked_exchange_names = set()
        real_pairs = set()  # (exchange, coin) — реально найдены на бирже (для обратной проверки ниже)
        for exchange_name in self.test_batch_safe_exchanges:
            # Монеты, которые МЫ считаем открытыми именно на этой бирже —
            # эталон, с которым сравнивается поток (см.
            # _positions_for_reconcile).
            expected_coins = {c for (e, c) in tracked_pairs if e == exchange_name}
            try:
                client = await tool._get_ready_client(exchange_name)
                positions, source = await self._positions_for_reconcile(
                    tool, exchange_name, expected_coins
                )
            except Exception as exc:
                print(f"[reconcile] {exchange_name}: не удалось проверить позиции ({type(exc).__name__}: {exc}).")
                continue
            checked_exchanges += 1
            if source == "поток":
                from_stream += 1

            # ФОНОВЫЙ ПРОГРЕВ КЭША БАЛАНСА (добавлено 2026-09-14 по прямой
            # просьбе пользователя: "оставь как лучше для безопасности, но
            # чтобы не ждать по 2-3 сек на вход"). Запрос баланса стоит
            # 657-2015мс (замер по биржам), и раньше эту цену платил САМ
            # ВХОД, когда кэш успевал протухнуть. Сверка позиций и так
            # ходит по всем биржам непрерывно и находится ВНЕ критического
            # пути — обновляем баланс здесь, и к моменту реальной сделки он
            # почти всегда свежий (0мс на входе).
            #
            # Обновляем не на каждом прогоне, а только когда кэш реально
            # постарел (см. BALANCE_WARMUP_AFTER_SECONDS): sweep крутится
            # без пауз, и запрашивать баланс каждый круг — лишняя нагрузка
            # на API, которая сама же и замедлит биржу.
            try:
                await tool._refresh_balance_if_stale(
                    exchange_name, _get_float_env("BALANCE_WARMUP_AFTER_SECONDS", 25.0)
                )
            except Exception:
                pass  # прогрев — вспомогательный, сбой не должен ломать сверку
            checked_exchange_names.add(exchange_name)

            for p in positions:
                contracts = p.get("contracts") or 0
                if abs(contracts) <= 0:
                    continue
                symbol = p.get("symbol") or ""
                coin = symbol.split("/")[0].upper() if "/" in symbol else symbol.upper()
                if not coin:
                    continue
                real_pairs.add((exchange_name, coin))
                if (exchange_name, coin) in tracked_pairs:
                    continue

                # ГРЕЙС-ПЕРИОД (см. _in_open_grace_period, реальный случай
                # STORJ 2026-09-11) — эта монета, возможно, ПРЯМО СЕЙЧАС
                # легитимно открывается, но ещё не попала в position_store.
                # Пропускаем полностью, даже не начиная отсчёт "два
                # подтверждения подряд" — независимо от того, сколько раз
                # sweep пройдёт за это время.
                if self._in_open_grace_period(coin):
                    continue

                # ВАЖНО (реальные инциденты 2026-09-10, IOST и потом CATE):
                # даже СВЕЖАЯ проверка position_store прямо перед действием
                # не убирает гонку полностью — сама сделка открывается в
                # ОТДЕЛЬНОМ потоке (run_in_executor), и между "реальный
                # ордер на бирже уже исполнился" и "position_store записал
                # обе ноги" есть неизбежный, пусть и короткий, зазор.
                # Sweep может застать РОВНО этот зазор — реальный случай:
                # CATE на aster был закрыт как "орфан" через секунды после
                # абсолютно легитимного открытия, оставив непокрытую ногу
                # на mexc (реальный убыток на самой этой ошибке).
                #
                # Поэтому теперь требуется ДВА ПОДТВЕРЖДЕНИЯ ПОДРЯД (на
                # соседних прогонах sweep, с паузой RECONCILE_INTERVAL_
                # SECONDS между ними) прежде чем реально закрывать — та же
                # логика "перепроверь, не спеши", что уже применялась для
                # похожих гонок в trade_tool.py. Ложную тревогу (гонка с
                # ещё не дописанной сделкой) это гасит уже на ПЕРВОМ
                # прогоне — ко второму позиция уже будет в position_store и
                # больше не попадёт сюда вовсе.
                fresh_pos = position_store.get_position(coin)
                key = (exchange_name, coin)
                if fresh_pos and exchange_name in (
                    (fresh_pos.get("long_exchange") or "").lower(),
                    (fresh_pos.get("short_exchange") or "").lower(),
                ):
                    self._pending_orphans.pop(key, None)
                    continue  # легитимно, в т.ч. если раньше была кандидатом

                if key not in self._pending_orphans:
                    # ПЕРВОЕ обнаружение — ТОЛЬКО запоминаем, без громкого
                    # алерта и без действия. Если это гонка — на следующем
                    # прогоне position_store уже будет знать про сделку, и
                    # выше сработает ветка "легитимно" (fresh_pos совпал).
                    self._pending_orphans[key] = time.monotonic()
                    print(
                        f"[reconcile] {coin} на {exchange_name}: похоже на неучтённую "
                        f"позицию (side={p.get('side')}, contracts={contracts}) — "
                        f"жду подтверждения на следующем прогоне, прежде чем закрывать."
                    )
                    continue

                # ВТОРОЕ подряд обнаружение — теперь уверенно закрываем.
                del self._pending_orphans[key]
                orphans_found += 1
                # ОРФАН — реальная позиция на бирже, о которой position_
                # store ничего не знает. Алертим НЕМЕДЛЕННО, до попытки
                # закрытия (если закрытие само упадёт — пользователь всё
                # равно уже в курсе и может вмешаться вручную).
                side = p.get("side")
                entry_price = p.get("entryPrice")
                unrealized = p.get("unrealizedPnl")
                alert_msg = (
                    f"🚨 НЕУЧТЁННАЯ ПОЗИЦИЯ: {coin} на {exchange_name} "
                    f"(side={side}, contracts={contracts}, entry={entry_price}, "
                    f"unrealizedPnl={unrealized}) — бот об этой ноге не знал. "
                    f"Пытаюсь закрыть автоматически."
                )
                print(f"[reconcile] {alert_msg}")
                try:
                    self.notifier.notify_error(symbol=coin, error_message=alert_msg)
                except Exception:
                    pass

                try:
                    market = client.market(symbol)
                    contract_size = market.get("contractSize") or 1
                    amount_in_coin = abs(contracts) * contract_size
                    original_side = "long" if (side or "").lower() == "long" else "short"
                    result = await tool._close_single_order(
                        exchange_name, coin, original_side, amount_in_coin
                    )
                    done_msg = f"Неучтённая позиция {coin} на {exchange_name} закрыта автоматически: {result}"
                    print(f"[reconcile] {done_msg}")
                    try:
                        self.notifier.notify_error(symbol=coin, error_message=done_msg)
                    except Exception:
                        pass
                except Exception as exc:
                    fail_msg = (
                        f"НЕ УДАЛОСЬ автоматически закрыть неучтённую позицию {coin} "
                        f"на {exchange_name}: {type(exc).__name__}: {exc} — ТРЕБУЕТСЯ РУЧНАЯ ПРОВЕРКА!"
                    )
                    print(f"[reconcile] {fail_msg}")
                    try:
                        self.notifier.notify_error(symbol=coin, error_message=fail_msg)
                    except Exception:
                        pass

        # ОБРАТНАЯ ПРОВЕРКА (добавлено 2026-09-10, реальный инцидент CATE):
        # учёт (position_store) думает, что ОБЕ ноги открыты, но на бирже
        # ОДНОЙ из них физически нет — например, потому что её ошибочно
        # закрыл сам reconcile (см. фикс с двумя подтверждениями выше) или
        # по любой другой причине. Проверяем ТОЛЬКО биржи, которые реально
        # опросили в этом прогоне (checked_exchange_names) — если запрос к
        # бирже упал, molчим про неё, а не делаем вывод по отсутствующим
        # данным. Та же защита "два подтверждения подряд", что и у прямой
        # проверки — не действуем по одному кадру.
        missing_found = 0
        for coin, pos in list(tracked.items()):
            coin_upper = coin.upper()
            # Тот же грейс-период, что и у прямой проверки выше — на
            # случай если одна из ног ещё не успела отразиться на бирже
            # (eventual consistency у самой биржи) сразу после открытия.
            if self._in_open_grace_period(coin_upper):
                continue
            for leg_key, other_key, other_amount_key in (
                ("long_exchange", "short_exchange", "short_amount_coin"),
                ("short_exchange", "long_exchange", "long_amount_coin"),
            ):
                ex = (pos.get(leg_key) or "").lower()
                if not ex or ex not in checked_exchange_names:
                    continue
                if (ex, coin_upper) in real_pairs:
                    self._pending_missing_legs.pop((coin_upper, ex), None)
                    continue

                key = (coin_upper, ex)
                if key not in self._pending_missing_legs:
                    self._pending_missing_legs[key] = time.monotonic()
                    print(
                        f"[reconcile] {coin_upper}: ожидаемая нога на {ex} НЕ найдена на "
                        f"бирже — жду подтверждения на следующем прогоне, прежде чем действовать."
                    )
                    continue

                # ВТОРОЕ подряд обнаружение — нога реально пропала.
                del self._pending_missing_legs[key]
                missing_found += 1
                other_ex = (pos.get(other_key) or "").lower()
                alert_msg = (
                    f"🚨 ПРОПАВШАЯ НОГА: {coin_upper} должна быть открыта на {ex}, но её "
                    f"там нет (учёт думал, что позиция полная) — закрываю оставшуюся ногу "
                    f"на {other_ex}, чтобы убрать незахеджированный риск."
                )
                print(f"[reconcile] {alert_msg}")
                try:
                    self.notifier.notify_error(symbol=coin_upper, error_message=alert_msg)
                except Exception:
                    pass

                if other_ex:
                    amount = pos.get(other_amount_key)
                    other_side = "long" if other_key == "long_exchange" else "short"
                    try:
                        if amount:
                            result = await tool._close_single_order(other_ex, coin_upper, other_side, amount)
                            done_msg = f"Оставшаяся нога {coin_upper} на {other_ex} закрыта: {result}"
                        else:
                            result = None
                            done_msg = (
                                f"{coin_upper}: не удалось определить объём для закрытия "
                                f"оставшейся ноги на {other_ex} — ТРЕБУЕТСЯ РУЧНАЯ ПРОВЕРКА!"
                            )
                        print(f"[reconcile] {done_msg}")
                        try:
                            self.notifier.notify_error(symbol=coin_upper, error_message=done_msg)
                        except Exception:
                            pass

                        # ЗАПИСЬ В ЖУРНАЛ (добавлено 2026-09-11 по прямой
                        # просьбе пользователя — запрос "посчитай точный
                        # win-rate" выявил дыру: reconcile-закрытия вообще
                        # не попадали в trade_ledger.jsonl, статистика была
                        # неполной). Известна ТОЛЬКО судьба ноги, которую мы
                        # только что закрыли реальным ордером (entry — из
                        # pos, exit — из result); судьба ПРОПАВШЕЙ ноги (на
                        # ex) неизвестна — она уже отсутствовала на бирже к
                        # моменту этой проверки (скорее всего, закрыта тем
                        # же reconcile отдельным более ранним событием
                        # "орфан", со своей ценой, которую этот код не
                        # видит). Пишем честно то, что знаем — с
                        # partial_data=True, а не выдумываем вторую цену.
                        if result and result.get("status") == "OK" and result.get("price") is not None:
                            try:
                                entry_price_key = "long_entry_price" if other_side == "long" else "short_entry_price"
                                entry_fee_key = "long_entry_fee_usdt" if other_side == "long" else "short_entry_fee_usdt"
                                entry_price = pos.get(entry_price_key)
                                exit_price = result.get("price")
                                entry_fee = pos.get(entry_fee_key)
                                known_leg_pnl = None
                                if entry_price is not None and exit_price is not None:
                                    if other_side == "long":
                                        known_leg_pnl = (exit_price - entry_price) * amount
                                    else:
                                        known_leg_pnl = (entry_price - exit_price) * amount
                                net_pnl = None
                                if known_leg_pnl is not None and entry_fee is not None:
                                    # Комиссию за выход оцениваем той же
                                    # ставкой, что и за вход — тот же приём,
                                    # что и в trade_executor._finalize_close.
                                    net_pnl = known_leg_pnl - entry_fee * 2
                                trade_ledger.record_trade({
                                    "event": "close",  # см. trade_ledger.daily_stats
                                    "coin": coin_upper,
                                    "long_exchange": pos.get("long_exchange"),
                                    "short_exchange": pos.get("short_exchange"),
                                    "long_entry_price": pos.get("long_entry_price"),
                                    "short_entry_price": pos.get("short_entry_price"),
                                    "long_exit_price": exit_price if other_side == "long" else None,
                                    "short_exit_price": exit_price if other_side == "short" else None,
                                    "amount_usdt": pos.get("amount_usdt"),
                                    "gross_pnl": known_leg_pnl,
                                    "net_pnl": net_pnl,
                                    "close_reason": (
                                        f"reconcile: пропавшая нога на {ex} — закрыл "
                                        f"оставшуюся на {other_ex}"
                                    ),
                                    "opened_at": pos.get("opened_at"),
                                    "closed_at": datetime.now(timezone.utc).isoformat(),
                                    "partial_data": True,
                                    "partial_data_note": (
                                        f"судьба ноги на {ex} неизвестна — уже отсутствовала "
                                        f"на бирже к моменту этой проверки (вероятно закрыта "
                                        f"отдельным событием reconcile ранее); PnL посчитан "
                                        f"только по известной ноге на {other_ex}."
                                    ),
                                })
                            except Exception as exc:
                                print(f"[reconcile] {coin_upper}: не удалось записать в trade_ledger: {exc}")
                    except Exception as exc:
                        fail_msg = (
                            f"НЕ УДАЛОСЬ закрыть оставшуюся ногу {coin_upper} на {other_ex}: "
                            f"{type(exc).__name__}: {exc} — ТРЕБУЕТСЯ РУЧНАЯ ПРОВЕРКА!"
                        )
                        print(f"[reconcile] {fail_msg}")
                        try:
                            self.notifier.notify_error(symbol=coin_upper, error_message=fail_msg)
                        except Exception:
                            pass

                # Снимаем с учёта в любом случае — исходной, полностью
                # захеджированной позиции больше не существует, независимо
                # от того, удалось ли закрыть оставшуюся ногу.
                position_store.pop_position(coin_upper)
                break  # эта монета обработана — вторую ногу того же coin уже не проверяем

        # Короткая строка на каждый прогон — чтобы в логе было ВИДНО, что
        # защита реально работает, а не просто "тихо всё хорошо" (что
        # неотличимо от "тихо сломалась"). Не через notifier — не хотим
        # спамить в Telegram каждую минуту, только когда РЕАЛЬНО что-то нашли.
        # Источник данных пишем явно: иначе по логу невозможно отличить
        # "поток работает и экономит REST" от "поток молча отвалился, и мы
        # незаметно вернулись на REST" — а это разные состояния системы.
        print(
            f"[reconcile] проверено бирж: {checked_exchanges} (из них по потоку: {from_stream}), "
            f"найдено неучтённых позиций: {orphans_found}, найдено пропавших ног: {missing_found}."
        )

    # -------------------------------------------------------------------
    # _scan_once — один цикл ПОИСКА НОВЫХ связок. Мониторинг уже открытых
    # позиций серии полностью вынесен в независимую _position_monitor_loop
    # (см. run()) — здесь его больше нет, чтобы медленный проход по многим
    # найденным связкам не задерживал проверку уже открытых позиций.
    # Если тестовая серия ЗАПОЛНЕНА (MONITORING) или ЗАВЕРШЕНА (DONE) —
    # поиск новых связок приостановлен (ни алертов, ни авто-торговли по
    # новым монетам), как и требовалось. Иначе — как раньше: собрать
    # данные со всех бирж параллельно, найти связки, разослать алерты (с
    # учётом cooldown), при AUTO_TRADE/тестовой серии передать в исполнение.
    # -------------------------------------------------------------------
    async def _find_opportunities_once(self) -> list[dict]:
        """Только ПОИСК данных/связок (сетевые запросы тикеров по всем
        биржам) — вынесено из _scan_once ОТДЕЛЬНО (фикс 2026-09-06), чтобы
        строгий таймаут цикла (SCANNER_CYCLE_TIMEOUT_SECONDS) оборачивал
        ИМЕННО эту, потенциально зависающую сетевую часть, а не реальное
        исполнение сделки (см. run() и комментарий там же — почему)."""
        if self.test_batch is not None and self.test_batch.state in (test_batch_mod.DONE, test_batch_mod.MONITORING):
            return []

        # Прогреваем/обновляем кэш курса USDC/USDT (см. _get_usdc_usdt_rate
        # в trade_tool.py) ПАРАЛЛЕЛЬНО со сбором тикеров — используется
        # ниже в _evaluate_pair (через синхронный _to_usdt_sync) для
        # сравнения цен Hyperliquid (USDC) с остальными биржами (USDT).
        _, per_exchange_data = await asyncio.gather(
            _get_usdc_usdt_rate(),
            asyncio.gather(*(self._fetch_exchange_data(ex_id) for ex_id in self.exchanges)),
        )

        # market_data[COIN][exchange_id] = {bid, ask, last, funding_rate, funding_timestamp}
        market_data: dict[str, dict[str, dict]] = {}
        for ex_id, coins in zip(self.exchanges, per_exchange_data):
            for coin, data in coins.items():
                market_data.setdefault(coin, {})[ex_id] = data

        return self._find_opportunities(market_data)

    async def _handle_opportunities(self, opportunities: list[dict]) -> None:
        """Обработка уже найденных связок — ОТДЕЛЬНО от поиска (см.
        _find_opportunities_once) и БЕЗ строгого таймаута цикла (см.
        run()): реальное исполнение сделки (лестница сумм, откат,
        выравнивание ног) может занимать десятки секунд само по себе —
        обрывать его по таймауту цикла ОПАСНО, а не просто неэффективно.
        Проверено на реальном ордере 2026-09-06: строгий wait_for на весь
        _scan_once() обрывал ОЖИДАНИЕ результата попытки открытия (сама
        попытка при этом реально продолжалась в фоновом потоке —
        run_in_executor не отменяется отменой awaiting-корутины), и
        СЛЕДУЮЩИЙ цикл сканирования, стартуя немедленно, заставал
        test_batch.open_positions ЕЩЁ пустым (запись появляется только
        когда фоновый поток реально завершится) — из-за этого лимит "не
        более 1 позиции одновременно" не срабатывал: успели открыться СРАЗУ
        ДВЕ сделки (ALL и BP) вместо одной.

        ПОСЛЕДОВАТЕЛЬНО (возвращено 2026-09-06 по просьбе пользователя
        "не более 1 позиции одновременно" при переходе на реальные
        деньги) — параллельный asyncio.gather (был введён раньше для
        скорости) не годится с лимитом "максимум 1 открытая позиция":
        если за один цикл нашлось сразу несколько связок, ВСЕ они
        проверили бы "уже есть открытая позиция?" ОДНОВРЕМЕННО, до того
        как хоть одна реально успела открыться — лимит не сработал бы
        (race condition). Последовательная обработка гарантирует, что
        вторая связка проверяет лимит уже ПОСЛЕ того, как первая
        полностью завершила попытку открытия. Как только пользователь
        снимет ограничение на 1 позицию — можно будет вернуть gather."""
        for opp in opportunities:
            await self._handle_opportunity(opp)

    # -------------------------------------------------------------------
    # _fetch_exchange_data — забирает тикеры (bid/ask) и ставки фандинга
    # ОДНИМ (максимум двумя) bulk-запросом на биржу — НЕ по одному запросу
    # на символ, иначе на бирже с тысячей контрактов это моментально
    # упрётся в rate limit. Возвращает {coin: {...}} для этой биржи.
    # -------------------------------------------------------------------
    async def _fetch_exchange_data(self, exchange_id: str) -> dict[str, dict]:
        exchange = await self._get_client(exchange_id)
        if exchange is None:
            return {}

        try:
            # Валюта котировки зависит от биржи (см. EXCHANGE_QUOTE_CURRENCY
            # в trade_tool.py) — у подавляющего большинства это USDT, но у
            # Hyperliquid контракты котируются в USDC (2026-09-07, по явному
            # уточнению пользователя: "везде НАЗВАНИЕUSDT, а на хайперликвид
            # НАЗВАНИЕUSDC, весь баланс тоже в USDC"). Жёсткий фильтр
            # quote == "USDT" раньше исключил бы ВСЕ рынки Hyperliquid из
            # поиска связок целиком. USDC/USDT — оба стейблкоины с околодо-
            # ларовым перегом, так что сравнение спреда между ними не
            # искажается сильнее обычного шума между биржами.
            expected_quote = EXCHANGE_QUOTE_CURRENCY.get(exchange_id, "USDT")
            markets = [
                m for m in exchange.markets.values()
                if m.get("swap") and m.get("linear") and m.get("quote") == expected_quote
                and m.get("active", True) and m.get("base")
            ]
            symbols = [m["symbol"] for m in markets]
            if not symbols:
                return {}

            tickers = await exchange.fetch_tickers(symbols)

            # BULK bid/ask ПОДСТРАХОВКА (добавлено 2026-09-11, корень
            # проблемы найден по трём реальным случаям подряд — FUTU, 0G,
            # RAVE, все на aster): fetch_tickers у aster не отдаёт bid/ask
            # вообще (см. комментарий ниже, 2026-09-07), поэтому цена
            # берётся из ticker['last'] — а на негромких контрактах это
            # цена ПОСЛЕДНЕЙ СДЕЛКИ, которая могла пройти давно и совсем не
            # по рыночной цене (реально проверено: aster last по 0G был
            # 0.1868 при живом рынке ~0.181, по RAVE — 0.2094 при ~0.203,
            # по FUTU — 118.5 при ~113.5). early-spread-check перед входом
            # эти фантомные спреды и так ловил (деньги не тратились), но
            # монета попадала в "найдена связка" и тратила время сканера
            # впустую на биржах с этим ограничением. У aster (в отличие от
            # fetch_tickers) есть ОТДЕЛЬНЫЙ bulk-эндпоинт fetch_bids_asks,
            # который живые bid/ask ДАЁТ (проверено напрямую: для тех же
            # трёх монет отдал реальные ~0.1811/0.1814, ~0.2030/0.2034,
            # 111.18/115.73 — совпадает с fetch_order_book). Используем его
            # как источник цены НА БИРЖАХ, где сам fetch_tickers bid/ask не
            # даёт — с приоритетом НАД last (last для таких бирж заведомо
            # ненадёжен), а не просто как fallback при отсутствии last.
            # Лишний bulk-запрос делаем ТОЛЬКО если это реально нужно — если
            # fetch_tickers этой биржи и так отдаёт bid/ask хотя бы по части
            # контрактов (обычная ситуация — gate/mexc/bybit/kucoin/
            # binance/bitget/hyperliquid), fetch_bids_asks не нужен вообще.
            tickers_have_bidask = any(t.get("bid") or t.get("ask") for t in tickers.values())
            bids_asks_by_symbol: dict[str, dict] = {}
            if not tickers_have_bidask and exchange.has.get("fetchBidsAsks"):
                try:
                    bids_asks_by_symbol = await exchange.fetch_bids_asks(symbols)
                except Exception as exc:
                    print(f"[scanner] {exchange_id}: fetch_bids_asks не сработал ({exc}), использую только last-цену тикеров.")

            funding_by_symbol: dict[str, dict] = {}
            if exchange.has.get("fetchFundingRates"):
                try:
                    funding_by_symbol = await exchange.fetch_funding_rates(symbols)
                except Exception as exc:
                    print(f"[scanner] {exchange_id}: fetch_funding_rates не сработал ({exc}), продолжаю без фандинга.")

            result: dict[str, dict] = {}
            for symbol, ticker in tickers.items():
                market = exchange.markets.get(symbol)
                if not market:
                    continue
                coin = market["base"].upper()
                bid, ask = ticker.get("bid"), ticker.get("ask")
                # Раньше отсутствие bid/ask считалось "неликвидный контракт
                # без реального стакана" и монета исключалась целиком — но
                # реальный случай 2026-09-07 (Aster): fetch_tickers/
                # fetch_ticker для ВСЕХ ~555 контрактов НЕ отдают bid/ask
                # вообще (только last), хотя fetch_order_book по факту
                # показывает нормальный, живой стакан — это ограничение
                # именно тикер-эндпоинта Aster/CCXT, а не признак низкой
                # ликвидности. Из-за строгого требования Aster НИКОГДА не
                # попадал в связки, хотя рынков там сотни. Теперь монета
                # исключается, только если совсем НЕТ ни bid/ask, ни last —
                # bid/ask=None просто передаётся дальше как None (используется
                # ниже в crossable-спред фильтре, который сам умеет мягко
                # пропускать проверку при отсутствии данных — см. _evaluate_pair).
                used_bidask_fallback = False
                if not bid and not ask and symbol in bids_asks_by_symbol:
                    ba = bids_asks_by_symbol[symbol]
                    bid, ask = ba.get("bid"), ba.get("ask")
                    used_bidask_fallback = bool(bid and ask)
                if not bid and not ask and not ticker.get("last"):
                    continue  # действительно нет вообще никакой цены

                # prefer_bidask=True ТОЛЬКО когда bid/ask реально пришли из
                # bids_asks_by_symbol (см. выше) — для таких бирж мы уже
                # знаем, что their ticker['last'] ненадёжен (устаревшая
                # цена последней редкой сделки), поэтому здесь СОЗНАТЕЛЬНО
                # доверяем живому bid/ask больше, чем last.
                price = self._extract_price(ticker, bid, ask, prefer_bidask=used_bidask_fallback)
                if not price:
                    continue

                funding_rate, funding_ts = self._extract_funding(
                    exchange_id, symbol, ticker, funding_by_symbol
                )

                # ОБЪЁМ (в валюте котировки, т.е. в USDT) — по явной просьбе
                # пользователя 2026-09-06: приоритет более "релевантным"
                # (активно торгуемым, ликвидным) монетам — у таких спред
                # обычно сходится быстрее и надёжнее, чем у тонких/мусорных
                # контрактов вроде ALL/BP (спред застрял на 13-18% часами).
                # quoteVolume — уже в USDT; если бирже его не даёт (не все
                # отдают), оцениваем через baseVolume*price.
                volume_usdt = ticker.get("quoteVolume")
                if not volume_usdt and ticker.get("baseVolume") and price:
                    volume_usdt = ticker["baseVolume"] * price

                result[coin] = {
                    "exchange": exchange_id,
                    "symbol": symbol,
                    "price": price,
                    "bid": bid,
                    "ask": ask,
                    "funding_rate": funding_rate,
                    "funding_timestamp": funding_ts,
                    "volume_usdt": volume_usdt,
                }
            return result
        except Exception as exc:
            print(f"[scanner] {exchange_id}: сбой сбора данных ({type(exc).__name__}: {exc}) — пропускаю биржу в этом цикле.")
            return {}

    @staticmethod
    def _extract_price(ticker: dict, bid=None, ask=None, prefer_bidask: bool = False):
        """ЕДИНАЯ референсная цена биржи, используется при поиске связок
        (_fetch_exchange_data). 'last' (цена последней сделки) — как и
        показывает сам канал сигналов ("Long BITGET: $0.026120000" — тоже
        одно число на биржу, не bid/ask по отдельности). Если 'last' не
        пришёл — подстраховываемся серединой bid/ask (если они переданы).

        prefer_bidask=True (добавлено 2026-09-11, см. _fetch_exchange_data)
        — переворачивает приоритет: используется ТОЛЬКО когда переданные
        bid/ask пришли из отдельного bulk-запроса fetch_bids_asks на бирже,
        чей fetch_tickers bid/ask вообще не отдаёт (сейчас — aster). Для
        таких бирж ticker['last'] на негромких контрактах доказанно
        ненадёжен (реальные случаи: FUTU/0G/RAVE — last расходился с живым
        рынком на 2-4%), а bid/ask из fetch_bids_asks — живой и точный."""
        if prefer_bidask and bid and ask:
            return (bid + ask) / 2
        price = ticker.get("last")
        if price:
            return price
        bid = bid if bid is not None else ticker.get("bid")
        ask = ask if ask is not None else ticker.get("ask")
        if bid and ask:
            return (bid + ask) / 2
        return None

    @staticmethod
    def _extract_funding(exchange_id: str, symbol: str, ticker: dict, funding_by_symbol: dict):
        """Возвращает (funding_rate, funding_timestamp_ms) — сперва из
        bulk fetch_funding_rates (унифицированный формат CCXT), а если для
        биржи это не поддерживается (например, MEXC — см. комментарий в
        DEMO_TRADING_SUPPORTED и has['fetchFundingRates'] в trade_tool.py
        для похожего случая), пробуем достать напрямую из "info" тикера,
        где многие биржи всё равно отдают fundingRate внутри обычного
        ответа по тикеру."""
        entry = funding_by_symbol.get(symbol)
        if entry:
            return entry.get("fundingRate"), entry.get("fundingTimestamp")

        info = ticker.get("info") or {}
        raw_rate = info.get("fundingRate")
        if raw_rate is not None:
            try:
                return float(raw_rate), None
            except (TypeError, ValueError):
                pass
        return None, None

    # -------------------------------------------------------------------
    # _find_opportunities — сравнивает КАЖДУЮ пару бирж для каждой монеты,
    # присутствующей минимум на двух биржах, и отбирает те, что проходят
    # порог спреда ИЛИ порог фандинга. По явной просьбе пользователя
    # 2026-09-06: заходим ТОЛЬКО на 2 ноги (одну пару бирж) на одну
    # монету за раз — если у монеты несколько пар выше порога (например,
    # DOT против и Gate, и Bybit одновременно из-за отклонения цены на
    # Bitget), берём ТОЛЬКО пару с САМЫМ БОЛЬШИМ спредом, а не пытаемся
    # открыть их все параллельно (раньше это тратило попытки/rate limit
    # впустую — вторая-третья попытка по той же монете почти всегда либо
    # дублирует первую, либо сразу ловит "уже есть открытая позиция").
    # -------------------------------------------------------------------
    def _find_opportunities(self, market_data: dict[str, dict[str, dict]]) -> list[dict]:
        opportunities = []
        for coin, per_exchange in market_data.items():
            if coin.upper() in self.excluded_coins:
                continue
            if len(per_exchange) < 2:
                continue
            exchange_ids = list(per_exchange.keys())
            best_opp = None
            coin_upper = coin.upper()
            for i in range(len(exchange_ids)):
                for j in range(i + 1, len(exchange_ids)):
                    ex_a, ex_b = exchange_ids[i], exchange_ids[j]
                    if (
                        (coin_upper, ex_a) in self.excluded_coin_exchange_pairs
                        or (coin_upper, ex_b) in self.excluded_coin_exchange_pairs
                    ):
                        continue
                    opp = self._evaluate_pair(coin, ex_a, per_exchange[ex_a], ex_b, per_exchange[ex_b])
                    if opp and (best_opp is None or opp["spread_pct"] > best_opp["spread_pct"]):
                        best_opp = opp
            if best_opp is not None:
                # Полный снимок по ВСЕМ биржам, где есть эта монета —
                # нужен для блока "All Exchanges Overview" в алерте
                # (см. _format_alert_html), не только для пары long/short.
                best_opp["all_exchanges"] = per_exchange
                opportunities.append(best_opp)

        # ПРИОРИТЕТ ДИАПАЗОНУ 4-6% + ЛИКВИДНОСТИ (по явной просьбе
        # пользователя 2026-09-06, вторым уточнением: "сравнивай монеты по
        # релевантности — которые больше торгуются и с которых можно
        # побыстрее снять прибыль"): при последовательной обработке связок
        # (см. _handle_opportunities) и лимите "не более 1 позиции
        # одновременно" порядок этого списка НАПРЯМУЮ определяет, что
        # реально пробуется открыть первым. Сортируем в три уровня:
        #   1) связки со спредом В ДИАПАЗОНЕ 4-6% — раньше остальных;
        #   2) внутри каждой группы — более ЛИКВИДНЫЕ (больше объём торгов
        #      за 24ч на менее ликвидной из двух бирж связки) — раньше
        #      менее ликвидных: у активно торгуемых монет спред обычно
        #      сходится быстрее и надёжнее, чем у тонких/мусорных контрактов
        #      (реальный случай: ALL/BP простояли на спреде 13-18% часами);
        #      связки без данных об объёме (liquidity_usdt=None) — в самый
        #      конец своей группы, а не считаются "нулевой" ликвидностью
        #      наравне с реально тонкими рынками;
        #   3) при равной ликвидности — по убыванию самого спреда, как
        #      раньше.
        # Ни один порог входа (TEST_BATCH_MIN_SPREAD) при этом не меняется —
        # это только ПОРЯДОК попытки, а не дополнительный фильтр.
        def _priority_key(opp: dict) -> tuple:
            in_band = self.PRIORITY_SPREAD_MIN <= opp["spread_pct"] <= self.PRIORITY_SPREAD_MAX
            liquidity = opp.get("liquidity_usdt")
            no_liquidity_data = liquidity is None
            return (
                0 if in_band else 1,
                1 if no_liquidity_data else 0,
                -(liquidity or 0),
                -opp["spread_pct"],
            )

        opportunities.sort(key=_priority_key)
        return opportunities

    def _evaluate_pair(self, coin: str, ex_a: str, data_a: dict, ex_b: str, data_b: dict):
        """Сравнивает ЕДИНУЮ референсную цену (data['price'], см.
        _fetch_exchange_data) двух бирж по прямому ТЗ:
          spread = ((price_short - price_long) / price_long) * 100
        Биржа с БОЛЕЕ НИЗКОЙ ценой автоматически становится Long (её и
        покупаем), с БОЛЕЕ ВЫСОКОЙ — Short (её и продаём) — так спред
        всегда получается положительным для реальной возможности."""
        price_a, price_b = data_a.get("price"), data_b.get("price")
        if not price_a or not price_b:
            return None  # нет цены — спреда нет

        # Нормализация USDC->USDT (см. _to_usdt_sync в trade_tool.py) — ТОЛЬКО
        # для сравнения/расчёта спреда между биржами; long_price/short_price
        # ниже остаются в ЛОКАЛЬНОЙ валюте каждой биржи (Hyperliquid — USDC),
        # т.к. именно они уходят в opp-словарь и дальше используются для
        # размера ордера и учёта позиции — там нужна реальная цена биржи,
        # а не приведённая к USDT.
        price_a_norm = _to_usdt_sync(price_a, ex_a)
        price_b_norm = _to_usdt_sync(price_b, ex_b)
        if price_a_norm == price_b_norm:
            return None  # цены совпадают — спреда нет

        if price_a_norm < price_b_norm:
            long_ex, long_data, long_price, long_price_norm = ex_a, data_a, price_a, price_a_norm
            short_ex, short_data, short_price, short_price_norm = ex_b, data_b, price_b, price_b_norm
        else:
            long_ex, long_data, long_price, long_price_norm = ex_b, data_b, price_b, price_b_norm
            short_ex, short_data, short_price, short_price_norm = ex_a, data_a, price_a, price_a_norm

        spread_pct = (short_price_norm - long_price_norm) / long_price_norm * 100
        if spread_pct > self.max_sane_spread_pct:
            return None  # явно "мусорное" совпадение тикеров разных активов

        funding_hit = (
            abs(long_data.get("funding_rate") or 0) * 100 >= self.funding_threshold_pct
            or abs(short_data.get("funding_rate") or 0) * 100 >= self.funding_threshold_pct
        )
        if spread_pct >= self.spread_threshold_pct or funding_hit:
            # "Ликвидность" связки — МИНИМУМ объёма из двух ног (узкое
            # место для быстрого схождения — это МЕНЕЕ ликвидная из
            # бирж, а не более), в USDT за 24ч.
            long_vol = long_data.get("volume_usdt")
            short_vol = short_data.get("volume_usdt")
            liquidity_usdt = min(long_vol, short_vol) if long_vol and short_vol else None

            # ЖЁСТКИЙ ФИЛЬТР ПО МИНИМАЛЬНОЙ ЛИКВИДНОСТИ (по явной просьбе
            # пользователя 2026-09-06, после реальных случаев ANSEM/BONER:
            # исполнение рыночным ордером на монете с малым объёмом торгов
            # проскальзывает мгновенно — заявленный спред "съедается" ценой
            # исполнения на тонком стакане). Раньше низкая ликвидность
            # только СНИЖАЛА приоритет (см. _find_opportunities) — теперь
            # монета с объёмом ниже порога (или вовсе без данных об объёме
            # — биржа его не отдала) ИСКЛЮЧАЕТСЯ из связок целиком, а не
            # просто идёт последней в очереди.
            if liquidity_usdt is None or liquidity_usdt < self.min_liquidity_usdt:
                return None

            # ФИЛЬТР ПО РЕАЛЬНО ИСПОЛНИМОМУ (crossable) СПРЕДУ — добавлено
            # 2026-09-07 после серии реальных случаев (ANSEM/BREW/SHROOM/
            # STONK/ZCAT), когда спред по 'last'-цене показывал 3%+, а
            # факт исполнения (см. _estimate_execution_price в
            # trade_tool.py, VWAP по стакану на реальный объём сделки)
            # оказывался от -25% до +1.7% — 'last' это цена ПОСЛЕДНЕЙ
            # сделки (могла пройти давно и по другой цене), а не то, что
            # реально можно купить/продать ПРЯМО СЕЙЧАС. bid/ask УЖЕ есть
            # в data_a/data_b (без дополнительных сетевых запросов) —
            # грубая (только первый уровень стакана, не VWAP на весь
            # объём), но бесплатная и мгновенная проверка ДО того, как
            # связка вообще попадёт в алерт/попытку входа. Не заменяет
            # финальный VWAP-чек в trade_tool.py (тот точнее — учитывает
            # глубину на весь объём сделки, но требует отдельного
            # fetch_order_book прямо перед отправкой ордеров) — служит
            # первым, дешёвым фильтром, чтобы не гонять заведомо мёртвые
            # связки через весь пайплайн (алерт -> попытка открытия ->
            # отмена) на каждом цикле.
            #
            # Применяется ТОЛЬКО когда связка найдена именно по ценовому
            # спреду (spread_pct >= порог) — funding-only связки (низкий
            # spread_pct, но funding_hit) торгуются по другой логике и не
            # должны отсеиваться по crossable-спреду цены.
            if spread_pct >= self.spread_threshold_pct:
                # Та же нормализация USDC->USDT, что и выше для last-цены —
                # bid/ask тоже приходят в ЛОКАЛЬНОЙ валюте биржи.
                long_ask = _to_usdt_sync(long_data.get("ask"), long_ex)
                short_bid = _to_usdt_sync(short_data.get("bid"), short_ex)
                crossable_spread_pct = (
                    (short_bid - long_ask) / long_ask * 100
                    if long_ask and short_bid and long_ask > 0 else None
                )
                # MISSING (None) != ПЛОХОЙ (отрицательный/заниженный) — если
                # bid/ask вообще не пришли (см. комментарий про Aster в
                # _fetch_exchange_data выше, тикер-эндпоинт этой биржи их не
                # отдаёт вообще ни для одной монеты), это НЕ повод считать
                # связку заведомо мёртвой — пропускаем фильтр (fail-open,
                # тот же принцип, что и в _check_spread_has_converged_recently
                # при сбое сети). Блокируем ТОЛЬКО когда bid/ask ЕСТЬ, но
                # показывают реально плохой (ниже порога) исполнимый спред.
                if crossable_spread_pct is not None and crossable_spread_pct < self.spread_threshold_pct:
                    return None

            return {
                "coin": coin,
                "long_exchange": long_ex,
                "short_exchange": short_ex,
                "long_price": long_price,
                "short_price": short_price,
                "long_symbol": long_data.get("symbol"),
                "short_symbol": short_data.get("symbol"),
                "spread_pct": spread_pct,
                "long_funding_rate": long_data.get("funding_rate"),
                "long_funding_ts": long_data.get("funding_timestamp"),
                "short_funding_rate": short_data.get("funding_rate"),
                "short_funding_ts": short_data.get("funding_timestamp"),
                "liquidity_usdt": liquidity_usdt,
            }
        return None

    # -------------------------------------------------------------------
    # _handle_opportunity — алерт (без cooldown по времени, см. ниже),
    # (опционально) авто-трейд.
    # -------------------------------------------------------------------
    async def _handle_opportunity(self, opp: dict) -> None:
        # По явной просьбе пользователя 2026-09-06: убрали временной
        # cooldown (раньше — 30 минут молчания по одной и той же связке).
        # Теперь ЕДИНСТВЕННАЯ причина не слать алерт повторно — по этой
        # монете УЖЕ есть открытая позиция (свежая проверка position_store,
        # а не устаревший таймер) — как только она закроется, алерты по
        # этой монете возобновляются немедленно, без ожидания.
        if position_store.get_position(opp["coin"]):
            return

        print(
            f"[scanner] Найдена связка: {opp['coin']} LONG {opp['long_exchange']}"
            f" / SHORT {opp['short_exchange']}, спред {opp['spread_pct']:.2f}%"
        )

        try:
            self.notifier.notify_scan_alert(self._format_alert_html(opp))
        except Exception as exc:
            print(f"[scanner] Не удалось сформировать/отправить алерт: {exc}")

        # Режим тестовой серии ИСКЛЮЧИТЕЛЬНО управляет открытием, пока
        # включён (см. комментарий в __init__) — общий AUTO_TRADE_ENABLED
        # ниже в этом случае не рассматривается вовсе.
        if self.test_batch is not None:
            await self._handle_test_batch_opportunity(opp)
            return

        if not self.auto_trade.enabled:
            return

        # a) Порог для АВТО-ТОРГОВЛИ — отдельный от порога алерта
        # (SCANNER_SPREAD_THRESHOLD_PERCENT): можно видеть в Telegram
        # больше находок, чем реально торговать (обычно min_spread выше).
        if opp["spread_pct"] < self.auto_trade.min_spread_percent:
            return

        # b) Лимит на общее число ОДНОВРЕМЕННО открытых позиций (не важно,
        # чьих — канала или сканера) — простой предохранитель от того,
        # чтобы сканер разом открыл десятки сделок за один цикл.
        open_count = len(position_store.list_positions())
        if open_count >= self.auto_trade.max_open_positions:
            print(
                f"[scanner] {opp['coin']}: достигнут лимит MAX_OPEN_POSITIONS="
                f"{self.auto_trade.max_open_positions} (сейчас открыто {open_count}) — "
                f"AUTO_TRADE пропускает вход."
            )
            return

        # c) Не открываем повторно, если по этой монете уже есть открытая
        # позиция (не важно, откуда она взялась — из канала или из
        # сканера) — иначе на каждый цикл, пока связка ещё актуальна,
        # уходил бы новый ордер поверх уже открытого.
        if position_store.get_position(opp["coin"]):
            print(f"[scanner] {opp['coin']}: позиция уже открыта — AUTO_TRADE пропускает повторный вход.")
            return

        await self._trigger_trade(opp)

    async def _trigger_trade(self, opp: dict) -> None:
        amount_usdt = self.auto_trade.amount_usdt

        # Уведомление о ПОПЫТКЕ входа — отправляется СРАЗУ, ДО фактического
        # исполнения ордеров (само исполнение — asyncio.run() внутри
        # trade_tool.py, поэтому уходит в отдельный поток ниже и может
        # занять заметное время) — чтобы вы видели факт триггера сразу же,
        # а не только итоговый результат через несколько секунд.
        try:
            self.notifier.notify_auto_trade_trigger(
                symbol=opp["coin"], spread_percent=opp["spread_pct"], amount_usdt=amount_usdt,
            )
        except Exception as exc:
            print(f"[scanner] Не удалось отправить уведомление о старте авто-сделки: {exc}")

        print(
            f"[scanner][AUTO-TRADE] Открываю сделку по #{opp['coin']} "
            f"| Spread: {opp['spread_pct']:.2f}% | Объём: ${amount_usdt:.2f} "
            f"(LONG {opp['long_exchange']} / SHORT {opp['short_exchange']})"
        )
        # Отмечаем СТАРТ попытки открытия ДО отправки ордеров — см.
        # _in_open_grace_period/_recent_open_attempts в __init__ (реальный
        # случай STORJ) — reconcile должен знать, что эта монета сейчас,
        # возможно, легитимно открывается, и не спутать её с орфаном.
        self._recent_open_attempts[opp["coin"].upper()] = time.monotonic()

        # ПРЯМОЙ await (без run_in_executor) — обновлено 2026-09-11 по
        # прямой просьбе пользователя ("как ускорить открытие ног"). Раньше
        # здесь execute_arbitrage_trade() (внутри — asyncio.run()/
        # run_coroutine_threadsafe через TradeExecutionTool.open_spread)
        # выполнялась в отдельном потоке через run_in_executor, потому что
        # её нельзя было звать напрямую из корутины, уже работающей внутри
        # event loop бота (а мы именно там — см. run() этого класса).
        # execute_arbitrage_trade_async() await'ит _open_both_legs_async()
        # НАПРЯМУЮ на этом же loop — экономит создание потока и лишнее
        # переключение контекста между вызовом и реальной отправкой
        # ордеров (тот же приём, что уже применён к закрытию, см.
        # close_structured_signal_detailed_async).
        try:
            report = await trade_executor.execute_arbitrage_trade_async(
                opp["coin"], opp["long_exchange"], opp["short_exchange"],
                amount_usdt, opp["spread_pct"], self.notifier,
            )
        except Exception as exc:
            # Непредвиденное исключение (не штатный ERROR-статус ноги,
            # который уже обработан внутри execute_arbitrage_trade_async) —
            # тоже логируем и уведомляем, а не теряем молча.
            print(f"[scanner][AUTO-TRADE] КРИТИЧЕСКАЯ ОШИБКА исполнения {opp['coin']}: {exc}")
            try:
                self.notifier.notify_error(
                    symbol=opp["coin"],
                    error_message=f"Необработанное исключение при авто-исполнении: {exc}",
                )
            except Exception:
                pass
            return

        print("-" * 70)
        print(f"[scanner][AUTO-TRADE] Результат по #{opp['coin']}:")
        print(report)
        print("-" * 70)

    # =====================================================================
    # ТЕСТОВАЯ СЕРИЯ (TEST_BATCH_MODE) — см. test_batch.py за состоянием
    # (PROSPECTING/MONITORING/DONE) и агрегацией статистики.
    # =====================================================================

    async def _handle_test_batch_opportunity(self, opp: dict) -> None:
        """Гейты входа для тестовой серии — вызывается ТОЛЬКО пока серия
        в PROSPECTING (см. _scan_once: в MONITORING/DONE сюда вообще не
        доходим, новые связки не ищутся)."""
        if not self.test_batch.can_open_more():
            return  # серия уже набрана — ждём переключения состояния на следующем цикле

        # СИСТЕМНЫЕ ЧАСЫ (см. _clock_guard_loop / clock_guard.py): при
        # расхождении с биржами вход бессмыслен — подписи не пройдут, — но
        # главное, он ОПАСЕН: если одна биржа примет запрос с чуть более
        # широким окном, а вторая нет, останется голая нога, которую бот
        # потом не сможет закрыть по той же причине. Тихий return —
        # громкий алерт уже отправлен из _clock_guard_loop.
        if not self._clock_ok:
            return

        # АВТОМАТИЧЕСКАЯ БЛОКИРОВКА (см. blocked_coins_store.py) — по явной
        # просьбе пользователя 2026-09-09: после нескольких РЕАЛЬНЫХ откатов
        # подряд (не пустых VWAP-отмен) монета перестаёт пытаться открыться
        # вообще, пока пользователь не разберётся с причиной вручную.
        # Печатаем ТОЛЬКО РАЗ за цикл (через cooldown-подобный принцип не
        # нужен — is_blocked дешёвая проверка файла, без сети).
        if blocked_coins_store.is_blocked(opp["coin"]):
            return

        # См. self._entry_cooldown_until — не пытаемся ПОВТОРНО открыть
        # эту же монету, если её недавно уже отклонили (VWAP/пост-фактум
        # проверка в trade_tool.py).
        cooldown_until = self._entry_cooldown_until.get(opp["coin"].upper())
        if cooldown_until is not None and time.monotonic() < cooldown_until:
            return

        # ВРЕМЕННЫЙ ЛИМИТ (по явной просьбе пользователя 2026-09-06 при
        # переходе на реальные деньги): "пока проверка не пройдёт успешно"
        # — не открываем больше ОДНОЙ позиции одновременно, даже если за
        # цикл нашлось сразу несколько связок. Снимается по отдельной
        # просьбе пользователя, когда бот докажет себя на реальных сделках.
        #
        # РАЗОВОЕ ИСКЛЮЧЕНИЕ "+1" (по явной просьбе пользователя 2026-09-06,
        # после случая с SHROOM — та связка 3.79% была пропущена из-за
        # этого самого лимита, пока BP уже был открыт): лимит поднят до 2
        # ОДИН РАЗ. Та же логика, что и для сигналов канала (см.
        # main.py:open_signal) — на будущее оба лимита стоит свести к
        # одному общему правилу, если пользователь попросит.
        if len(self.test_batch.open_positions) >= 2:
            return

        # НЕ БОЛЕЕ 1 ОТКРЫТОЙ НОГИ НА БИРЖУ ОДНОВРЕМЕННО (по явной просьбе
        # пользователя 2026-09-07: "допустим на mexc и gate открыты по
        # ноге — не открываем новую ногу пока эта не закроется, но можно
        # открывать на других биржах"). Снижает риск каскада под cross
        # margin — см. position_store.busy_exchanges() и реальный
        # инцидент BONER/BP 2026-09-06 (одна плохая сделка на бирже утащила
        # за собой другую, никак не связанную, открытую позицию на ТОЙ ЖЕ
        # бирже). position_store — единый источник правды (та же проверка
        # и для сигналов канала, см. main.py:open_signal).
        # ВАЖНО (найдено 2026-09-09, реальный случай: ONE kucoin/mexc
        # 2.9%+ спред НИ РАЗУ не пытался открыться, без единой строки в
        # логе): здесь НЕ должно быть EXCHANGE_ALIASES.get(...) — этот
        # словарь в trade_tool.py смешивает ДВЕ разные вещи: алиасы
        # альтернативных ИМЁН (hliquid->hyperliquid) и перевод НАШЕГО
        # канонического имени в имя КЛАССА CCXT, когда они расходятся
        # (kucoin->kucoinfutures). busy_exchanges()/test_batch_safe_
        # exchanges используют НАШЕ каноническое имя ("kucoin"), а
        # EXCHANGE_ALIASES.get("kucoin") отдавал "kucoinfutures" —
        # сравнение никогда не совпадало, проверка тихо не проходила.
        # opp["long_exchange"]/short_exchange и так уже наши канонические
        # имена (пришли из SCANNER_EXCHANGES через _fetch_exchange_data) —
        # никакого перевода имени здесь не требуется вообще.
        busy = position_store.busy_exchanges()
        long_id_check = opp["long_exchange"].lower()
        short_id_check = opp["short_exchange"].lower()
        bypass_busy_check = False
        if long_id_check in busy or short_id_check in busy:
            # РАЗОВОЕ ИСКЛЮЧЕНИЕ (по явной просьбе пользователя 2026-09-08:
            # "разрешаю сделать разовое исключение для mexc и gate, но
            # только 1 раз сейчас") — self._busy_exchange_override_available
            # потребляется ТОЛЬКО после реально УСПЕШНОГО открытия (см. ниже
            # после _trigger_test_batch_trade), а не на каждую попытку — если
            # эта конкретная связка не пройдёт дальнейшие проверки (VWAP и
            # т.п.), исключение остаётся доступным для следующей попытки.
            if not self._busy_exchange_override_available:
                return
            bypass_busy_check = True
            print(
                f"[test_batch] {opp['coin'].upper()}: РАЗОВОЕ исключение из правила "
                f"'1 нога на биржу' (по явной просьбе пользователя) — пробую открыть, "
                f"несмотря на занятость {long_id_check if long_id_check in busy else short_id_check}."
            )

        # Порог входа тестовой серии — TEST_BATCH_MIN_SPREAD (по ТЗ: 5.0%),
        # отдельный от порога алерта.
        if opp["spread_pct"] < self.test_batch.min_spread_pct:
            return

        # Тестовая серия торгует ТОЛЬКО на биржах из TEST_BATCH_SAFE_EXCHANGES
        # (по умолчанию — то же, что DEMO_TRADING_SUPPORTED, bybit/bitget/gate)
        # — даже если DEMO_ALLOW_LIVE_EXCHANGES разрешает какой-то бирже
        # (например, Aster) торговать реальными деньгами во время
        # DEMO_TRADING, эту связку сюда всё равно не пускаем, если её нет
        # в safe_exchanges: тестовая серия не должна случайно тронуть
        # реальные деньги. MEXC можно добавить в TEST_BATCH_SAFE_EXCHANGES
        # ОТДЕЛЬНО, если ваш MEXC_API_KEY привязан именно к demo-счёту биржи
        # (CCXT не умеет переключать MEXC в demo-режим кодом — но если ключ
        # уже demo, переключать и не нужно).
        # См. комментарий выше у long_id_check/short_id_check — та же
        # причина, никакого EXCHANGE_ALIASES здесь не нужно.
        long_id = opp["long_exchange"].lower()
        short_id = opp["short_exchange"].lower()
        if long_id not in self.test_batch_safe_exchanges or short_id not in self.test_batch_safe_exchanges:
            return

        # Защита от дублирования ордеров: не открываем повторно, если по
        # этой монете уже есть открытая позиция (сканера, серии или
        # канала — не важно чья).
        if position_store.get_position(opp["coin"]):
            return

        # long_symbol/short_symbol обязательны для последующего мониторинга
        # цены (см. _monitor_test_batch) — без них не сможем понять, какой
        # именно контракт на бирже проверять на закрытие.
        if not opp.get("long_symbol") or not opp.get("short_symbol"):
            print(f"[test_batch] {opp['coin']}: нет символов бирж в связке — пропускаю (не должно случаться).")
            return

        # РАННЯЯ ПРОВЕРКА ПО СТАКАНУ (добавлено 2026-09-10, по прямой
        # просьбе пользователя — улучшение №1 из списка "как улучшить
        # систему"): сканер находит связки по ТИКЕРУ (last-цена), а
        # реальный спред по глубине стакана регулярно оказывается ниже
        # порога или вовсе отрицательным — раньше это отсеивалось ТОЛЬКО
        # на VWAP-проверке прямо перед отправкой ордера (внутри
        # _trigger_test_batch_trade), уже ПОСЛЕ дорогого convergence-check
        # (сетевой запрос за 24ч свечей). Проверяем здесь, пока дешевле
        # отбросить шум, чем тратить время на последующие проверки.
        # Fail-open при сетевой ошибке/пустом стакане — как и остальные
        # soft-check в этом файле, не блокируем сделку из-за ЭТОЙ
        # конкретной проверки, если данных просто не удалось получить.
        book_tool = TradeExecutionTool()
        long_snapshot, short_snapshot = await asyncio.gather(
            book_tool._get_book_snapshot(opp["long_exchange"], opp["coin"], self.test_batch.amount_usdt),
            book_tool._get_book_snapshot(opp["short_exchange"], opp["coin"], self.test_batch.amount_usdt),
        )
        # ПЕРЕДАЁМ СНИМКИ ДАЛЬШЕ (добавлено 2026-09-14): ровно эти же стаканы
        # нужны потом внутри _open_both_legs_async (направление входа +
        # финальная проверка спреда), и раньше они там запрашивались ЗАНОВО —
        # лишние ~560мс (медиана по [timing]) на каждый вход. Между этими
        # двумя точками обычно проходят миллисекунды (проверка схождения
        # чаще всего берётся из кэша), поэтому повторный запрос ничего не
        # уточнял. Возраст снимка проверяется на той стороне: если он вдруг
        # успел устареть (сработала НЕкэшированная проверка схождения — это
        # реальные секунды на запрос свечей), стаканы там перезапросятся.
        opp["_book_snapshots"] = (long_snapshot, short_snapshot)
        opp["_book_snapshots_at"] = time.monotonic()
        if long_snapshot is not None and short_snapshot is not None:
            long_vwap = long_snapshot.get("buy_vwap") or long_snapshot.get("reference_price")
            short_vwap = short_snapshot.get("sell_vwap") or short_snapshot.get("reference_price")
            long_norm = _to_usdt_sync(long_vwap, opp["long_exchange"]) if long_vwap else None
            short_norm = _to_usdt_sync(short_vwap, opp["short_exchange"]) if short_vwap else None
            if long_norm is not None and short_norm is not None and long_norm > 0:
                real_spread_pct = (short_norm - long_norm) / long_norm * 100
                if real_spread_pct < self.test_batch.min_spread_pct:
                    print(
                        f"[early-spread-check] {opp['coin'].upper()}: реальный спред по стакану "
                        f"{real_spread_pct:.2f}% (порог {self.test_batch.min_spread_pct}%, тикер "
                        f"показывал {opp['spread_pct']:.2f}%) — пропускаю до дорогой проверки "
                        f"схождения."
                    )
                    return

        # ИСТОРИЯ СХОЖДЕНИЯ СПРЕДА (добавлено 2026-09-07 по явной просьбе
        # пользователя после реального случая с BP: спред между mexc/gate
        # держался 13-22% ПОЧТИ 30 ЧАСОВ ПОДРЯД, ни разу не сойдясь — это
        # не временная арбитражная возможность, а, похоже, постоянный
        # разрыв в котировках между биржами по этому тикеру). Проверяем ПО
        # СВЕЧАМ, сходился ли спред этой пары бирж хотя бы раз за последние
        # SCANNER_CONVERGENCE_LOOKBACK_HOURS часов до разумного уровня
        # (SCANNER_CONVERGENCE_MAX_SPREAD_PCT) — если НИ РАЗУ, монету
        # пропускаем: ждать схождения тут можно бесконечно.
        converged_recently, min_spread_seen = await self._check_spread_has_converged_recently(
            opp["coin"], opp["long_exchange"], opp["short_exchange"]
        )
        # ШИРОКИЙ ВХОД БЕЗ ИСТОРИИ СХОЖДЕНИЯ (добавлено 2026-09-11 по явной
        # просьбе пользователя после реального случая STORJ: спред
        # gate/bybit ни разу не опускался ниже 1% за 12ч, но сам спред
        # входа — 3%+ — РЕАЛЬНЫЙ, не фантомный тикер (прошёл early-spread-
        # check по стакану). Раньше такую монету просто пропускали
        # целиком — теперь входим, но ТОЛЬКО если разрешено
        # (SCANNER_ALLOW_WIDE_SPREAD_ENTRY) и помечаем позицию как
        # "wide_spread": для неё в _monitor_test_batch действует
        # ДОПОЛНИТЕЛЬНАЯ защита — максимальное время удержания и
        # стоп-лосс (см. SCANNER_WIDE_SPREAD_MAX_HOLD_HOURS/
        # SCANNER_WIDE_SPREAD_STOP_LOSS_USDT), которой нет у обычных,
        # исторически сходящихся монет — история уже сказала нам, что
        # схождения до 0.2-1% можно не дождаться вообще, поэтому нельзя
        # просто ждать спред бесконечно, как для обычной связки.
        opp["wide_spread"] = not converged_recently
        opp["convergence_min_spread_seen_pct"] = min_spread_seen
        if opp["wide_spread"]:
            if not _get_bool_env("SCANNER_ALLOW_WIDE_SPREAD_ENTRY", True):
                return
            # min_spread_seen тут ВСЕГДА реальное число (не None) — если бы
            # истории не было вовсе, _check_spread_has_converged_recently
            # вернула бы (True, None), т.е. wide_spread было бы False.
            print(
                f"[wide-spread-entry] {opp['coin'].upper()}: история НЕ показала схождение "
                f"туже {_get_float_env('SCANNER_CONVERGENCE_MAX_SPREAD_PCT', 1.0)}% за "
                f"последние {_get_int_env('SCANNER_CONVERGENCE_LOOKBACK_HOURS', 12)}ч "
                f"(минимум был {min_spread_seen:.2f}%) — вхожу всё равно (спред "
                f"{opp['spread_pct']:.2f}% реальный); закроется при чистой прибыли > "
                f"${_get_float_env('SCANNER_CLOSE_MIN_PROFIT_USDT', 0.10):.2f} (в минус не закрываемся, лимита времени нет)."
            )

        await self._trigger_test_batch_trade(opp)

        if bypass_busy_check and position_store.get_position(opp["coin"]):
            # Разовое исключение реально ИСПОЛЬЗОВАНО (обе ноги успешно
            # открылись) — потребляем его: следующая попытка снова
            # подчиняется обычному правилу "1 нога на биржу".
            self._busy_exchange_override_available = False
            print(
                f"[test_batch] {opp['coin'].upper()}: разовое исключение из правила "
                f"'1 нога на биржу' использовано — дальше правило снова действует как обычно."
            )

    # -------------------------------------------------------------------
    # _check_spread_has_converged_recently — по часовым свечам ОБЕИХ бирж
    # проверяет, опускался ли спред этой пары хотя бы раз за последние N
    # часов до "сходящегося" уровня. Возвращает True, если ДА (монета
    # проходит проверку) или если данные получить не удалось (не блокируем
    # сделку из-за сетевой ошибки самой проверки — тот же принцип, что и у
    # _get_reference_price). Возвращает False, только если ИСТОРИЯ ЕСТЬ и
    # она ЧЁТКО показывает: спред ни разу не сходился — тогда монета
    # реально "залипшая" (как BP), а не просто новая/без истории.
    # -------------------------------------------------------------------
    async def _check_spread_has_converged_recently(
        self, coin: str, long_exchange: str, short_exchange: str
    ) -> tuple[bool, "float | None"]:
        """Обёртка с КОРОТКИМ кэшем (см. SCANNER_CONVERGENCE_CACHE_TTL_SECONDS)
        над _check_spread_has_converged_recently_impl — по явной просьбе
        пользователя 2026-09-08 ("можем ли мы уменьшить задержку"): это
        САМЫЙ ТЯЖЁЛЫЙ сетевой этап пайплайна входа (~5с на реальном замере
        — тянет часовые свечи за 24ч с ДВУХ бирж), а одну и ту же связку
        (монета+пара бирж) часто проверяем повторно на соседних циклах
        (реальный пример: ACE/FONE встречаются раз за разом). Характер
        схождения спреда не меняется от цикла к циклу за секунды — кэш на
        несколько минут убирает ПОВТОРНЫЙ сетевой запрос почти полностью,
        не снижая строгость самой проверки (TTL короче, чем реально нужно
        монете, чтобы сменить "характер" поведения)."""
        ttl_seconds = _get_float_env("SCANNER_CONVERGENCE_CACHE_TTL_SECONDS", 180.0)
        cache_key = (coin.upper(), long_exchange.lower(), short_exchange.lower())
        now = time.monotonic()
        cached = self._convergence_cache.get(cache_key)
        if cached is not None and now - cached[0] < ttl_seconds:
            return cached[1]
        result = await self._check_spread_has_converged_recently_impl(coin, long_exchange, short_exchange)
        self._convergence_cache[cache_key] = (now, result)
        return result

    async def _check_spread_has_converged_recently_impl(
        self, coin: str, long_exchange: str, short_exchange: str
    ) -> tuple[bool, "float | None"]:
        """Возвращает (прошла_ли_проверка, min_spread_seen) — второе
        значение (минимальный спред за lookback_hours, None если история
        недоступна) используется вызывающим кодом ДОПОЛНИТЕЛЬНО — см.
        SCANNER_TIGHT_CONVERGENCE_PCT в _handle_test_batch_opportunity."""
        lookback_hours = _get_int_env("SCANNER_CONVERGENCE_LOOKBACK_HOURS", 24)
        max_spread_pct = _get_float_env("SCANNER_CONVERGENCE_MAX_SPREAD_PCT", 2.0)
        symbol = _build_symbol(long_exchange, coin)  # тот же тикер на обеих биржах (USDT-пары)
        try:
            long_client = await self._get_client(long_exchange)
            short_client = await self._get_client(short_exchange)
            if long_client is None or short_client is None:
                return True, None
            long_ohlcv, short_ohlcv = await asyncio.gather(
                long_client.fetch_ohlcv(symbol, timeframe="1h", limit=lookback_hours),
                short_client.fetch_ohlcv(symbol, timeframe="1h", limit=lookback_hours),
            )
        except Exception as exc:
            # Нет истории (новый листинг, биржа не отдаёт OHLCV и т.п.) —
            # не блокируем сделку из-за ЭТОЙ отдельной проверки.
            print(f"[convergence-check] {coin}: не удалось получить историю свечей ({exc}) — пропускаю проверку.")
            return True, None

        long_by_ts = {int(c[0]): c[4] for c in long_ohlcv if c[4]}  # timestamp -> close
        short_by_ts = {int(c[0]): c[4] for c in short_ohlcv if c[4]}
        common_ts = set(long_by_ts) & set(short_by_ts)
        if len(common_ts) < 3:
            # Слишком мало общих точек, чтобы судить (новый листинг на
            # одной из бирж и т.п.) — не блокируем.
            return True, None

        spreads_by_ts = {
            ts: abs(long_by_ts[ts] - short_by_ts[ts]) / min(long_by_ts[ts], short_by_ts[ts]) * 100
            for ts in common_ts
        }
        min_spread_seen = min(spreads_by_ts.values())
        if min_spread_seen > max_spread_pct:
            print(
                f"[convergence-check] {coin.upper()}: спред между {long_exchange}/{short_exchange} "
                f"НИ РАЗУ не опускался ниже {min_spread_seen:.2f}% за последние {lookback_hours}ч "
                f"(порог {max_spread_pct}%) — похоже на постоянный разрыв в котировках, а не "
                f"временную возможность. Пропускаю."
            )
            return False, min_spread_seen

        # ПРОВЕРКА СВЕЖЕСТИ СХОЖДЕНИЯ (добавлено 2026-09-08 по явной просьбе
        # пользователя: "старайся заходить в монеты, где спред быстрее
        # сходится") — раньше проверялось только "сходился ли спред ХОТЬ
        # РАЗ за 24ч", без учёта, КОГДА именно. Реальный случай: FONE
        # честно сходился ~25 часов подряд, а затем ~20 часов ПОДРЯД
        # только расходился (с 4% до 25%+) — старая проверка это пропускала
        # (спред ведь опускался ниже порога где-то в начале 24-часового
        # окна), и мы зашли ровно в разгар разъезда, а не в здоровую фазу.
        # Теперь дополнительно требуем, чтобы спред опускался ДО порога не
        # только когда-то, а НЕДАВНО — иначе монета, скорее всего, вышла из
        # режима быстрого схождения в затяжной тренд.
        recency_hours = _get_float_env("SCANNER_CONVERGENCE_RECENCY_HOURS", 8.0)
        last_convergence_ts = max(ts for ts, s in spreads_by_ts.items() if s <= max_spread_pct)
        latest_ts = max(common_ts)
        hours_since_convergence = (latest_ts - last_convergence_ts) / (1000 * 3600)
        if hours_since_convergence > recency_hours:
            print(
                f"[convergence-check] {coin.upper()}: спред {long_exchange}/{short_exchange} последний раз "
                f"опускался ниже {max_spread_pct}% {hours_since_convergence:.1f}ч назад (порог свежести "
                f"{recency_hours}ч) — похоже, монета сейчас в затяжном тренде расхождения, а не в режиме "
                f"быстрого схождения. Пропускаю."
            )
            return False, min_spread_seen
        return True, min_spread_seen

    async def _trigger_test_batch_trade(self, opp: dict) -> None:
        amount_usdt = self.test_batch.amount_usdt
        next_number = self.test_batch.opened_count + 1

        try:
            self.notifier.notify_auto_trade_trigger(
                symbol=opp["coin"], spread_percent=opp["spread_pct"], amount_usdt=amount_usdt,
            )
        except Exception as exc:
            print(f"[test_batch] Не удалось отправить уведомление о старте сделки: {exc}")

        print(
            f"[test_batch] Открываю сделку "
            f"{f'#{next_number} (непрерывный режим)' if self.test_batch.unlimited else f'{next_number}/{self.test_batch.size}'}: "
            f"#{opp['coin']} | Spread: {opp['spread_pct']:.2f}% | Объём: ${amount_usdt:.2f} "
            f"(LONG {opp['long_exchange']} / SHORT {opp['short_exchange']})"
        )
        # Отмечаем СТАРТ попытки открытия ДО отправки ордеров — см.
        # _in_open_grace_period/_recent_open_attempts в __init__ (реальный
        # случай STORJ, 4 ложных закрытия подряд реконсиляцией).
        self._recent_open_attempts[opp["coin"].upper()] = time.monotonic()

        # См. комментарий в _trigger_trade — тот же прямой await напрямую
        # на loop бота, без run_in_executor (обновлено 2026-09-11).
        # extra_position_fields={"wide_spread": ...} (добавлено 2026-09-12,
        # реальный случай ANTHROPIC) — без этого пометка "широкая монета"
        # (правило закрытия 2%-тейк-профит без лимита времени/спреда)
        # хранилась ТОЛЬКО в памяти и терялась при любом рестарте бота —
        # позиция тихо переключалась на обычные правила закрытия.
        try:
            report = await trade_executor.execute_arbitrage_trade_async(
                opp["coin"], opp["long_exchange"], opp["short_exchange"],
                amount_usdt, opp["spread_pct"], self.notifier,
                extra_position_fields={"wide_spread": bool(opp.get("wide_spread"))},
                # Готовые стаканы из early-spread-check (см. там же) — чтобы
                # не запрашивать их второй раз. Возраст проверяется внутри.
                prefetched_books=opp.get("_book_snapshots"),
                prefetched_books_at=opp.get("_book_snapshots_at"),
            )
        except Exception as exc:
            print(f"[test_batch] КРИТИЧЕСКАЯ ОШИБКА открытия {opp['coin']}: {exc}")
            try:
                self.notifier.notify_error(
                    symbol=opp["coin"],
                    error_message=f"Необработанное исключение при открытии тестовой сделки: {exc}",
                )
            except Exception:
                pass
            return

        print(report)

        # Подтверждаем успех через position_store (надёжнее, чем парсить
        # текстовый report) — обе ноги реально открылись и позиция
        # поставлена на учёт ТОЛЬКО если open_structured_signal() сочла
        # обе ноги успешными (see trade_executor.py). Заодно вытаскиваем
        # РЕАЛЬНЫЕ цену входа и комиссию за вход (уже посчитаны и записаны
        # туда самим trade_executor) — нужны для оценки суммарного PnL
        # ДО фактического закрытия, см. _monitor_test_batch().
        stored_position = position_store.get_position(opp["coin"])
        if stored_position is None:
            print(f"[test_batch] {opp['coin']}: сделка не открылась (не обе ноги) — НЕ засчитываю в серию.")
            # См. self._entry_cooldown_until — не долбим ту же монету
            # каждый цикл (0.5с), даём ей время либо реально измениться,
            # либо просто перестать засорять лог повторами.
            self._entry_cooldown_until[opp["coin"].upper()] = (
                time.monotonic() + self._entry_retry_cooldown_seconds
            )
            return

        entry_fee_total = trade_executor._sum_fees_values(
            stored_position.get("long_entry_fee_usdt"), stored_position.get("short_entry_fee_usdt")
        )
        self.test_batch.record_open(
            opp["coin"], opp["long_exchange"], opp["short_exchange"],
            opp["long_symbol"], opp["short_symbol"], opp["spread_pct"],
            long_entry_price=stored_position.get("long_entry_price"),
            short_entry_price=stored_position.get("short_entry_price"),
            amount_usdt=stored_position.get("amount_usdt") or amount_usdt,
            entry_fee_total=entry_fee_total,
            wide_spread=bool(opp.get("wide_spread")),
        )
        if self.test_batch.unlimited:
            print(
                f"[test_batch] Всего открыто сделок: {self.test_batch.opened_count} "
                f"(сейчас в работе: {len(self.test_batch.open_positions)})."
            )
        else:
            print(f"[test_batch] Серия: {self.test_batch.opened_count}/{self.test_batch.size} открыто.")
        if self.test_batch.state == test_batch_mod.MONITORING:
            print(
                f"[test_batch] Серия заполнена ({self.test_batch.size}/{self.test_batch.size}) — "
                f"поиск новых связок ПРИОСТАНОВЛЕН, перехожу в режим мониторинга/закрытия."
            )

    # -------------------------------------------------------------------
    # _monitor_test_batch — вызывается КАЖДЫЙ цикл, пока серия в
    # MONITORING (интервал короче обычного, см. run()). Для ВСЕХ открытых
    # позиций серии ОДНОВРЕМЕННО (asyncio.gather, без последовательных
    # задержек) проверяет текущий спред и закрывает те, что достигли цели.
    # -------------------------------------------------------------------
    async def _warm_leverage_for_candidates(self, opportunities: list) -> None:
        """Заранее выставляет плечо на биржах для монет, в которые мы РЕАЛЬНО
        можем зайти в ближайшее время — чтобы вход не платил за set_leverage
        0.5-1.9с (замер 2026-09-14: aster 1875мс, gate 1172, bitget 610,
        mexc 609, binance 594, bybit 500).

        Осторожность здесь важнее скорости, поэтому:
          * берём ТОЛЬКО связки, прошедшие порог входа по спреду и
            состоящие из разрешённых бирж — греть тысячи рынков "на всякий
            случай" бессмысленно и упрёмся в rate limit;
          * ограничиваем число пар за цикл (LEVERAGE_WARM_MAX_PER_CYCLE);
          * сторона (long/short) берётся из самой связки — у MEXC параметр
            positionType зависит от стороны, прогрев "не той" стороны был бы
            бесполезен;
          * всё это в отдельной задаче, результат никто не ждёт, ошибки
            гасятся внутри warm_leverage.
        """
        if not opportunities or self.test_batch is None:
            return
        limit = _get_int_env("LEVERAGE_WARM_MAX_PER_CYCLE", 6)
        leverage = _get_int_env("TRADE_LEVERAGE", 5)
        tool = TradeExecutionTool()
        done = 0
        seen = set()
        for opp in opportunities:
            if done >= limit:
                break
            if opp.get("spread_pct", 0) < self.test_batch.min_spread_pct:
                continue
            coin = opp.get("coin")
            for exchange_name, side in ((opp.get("long_exchange"), "long"), (opp.get("short_exchange"), "short")):
                if not exchange_name or exchange_name.lower() not in self.test_batch_safe_exchanges:
                    continue
                key = (exchange_name.lower(), coin, side)
                if key in seen:
                    continue
                seen.add(key)
                if await tool.warm_leverage(exchange_name, coin, side, leverage):
                    done += 1
                    print(f"[leverage-warm] {exchange_name}/{coin} {side}: плечо {leverage}x выставлено заранее.")
                if done >= limit:
                    break

    async def _sync_book_streams(self) -> None:
        """Держит набор потоковых подписок в точности равным набору ног
        ОТКРЫТЫХ позиций серии: новая позиция — подписались, закрылась —
        отписались. Вызывается из цикла мониторинга (дёшево: сравнение
        двух множеств, реальные действия только при изменениях).

        Публичный стакан не требует ключей, поэтому клиенту потока хватает
        минимального конфига — ключи сюда СОЗНАТЕЛЬНО не передаются."""
        if not self.test_batch:
            return
        want = set()
        for pos in self.test_batch.open_positions.values():
            want.add((EXCHANGE_ALIASES.get(pos.long_exchange.lower(), pos.long_exchange.lower()), pos.long_symbol))
            want.add((EXCHANGE_ALIASES.get(pos.short_exchange.lower(), pos.short_exchange.lower()), pos.short_symbol))
        have = set()
        for item in book_stream.active_streams():
            ex_id, _, sym = item.partition("/")
            have.add((ex_id, sym))
        config = {"enableRateLimit": True, "options": {"defaultType": "swap"}, "timeout": 15000}
        for ex_id, sym in want - have:
            await book_stream.subscribe(ex_id, sym, config)
        for ex_id, sym in have - want:
            await book_stream.unsubscribe(ex_id, sym)

    async def _monitor_test_batch(self) -> None:
        positions = list(self.test_batch.open_positions.values())
        if not positions:
            return  # не должно происходить (state стал бы DONE), но на всякий случай

        # ОБЪЕДИНЕНО В ОДИН ПАРАЛЛЕЛЬНЫЙ ПРОХОД (2026-09-10, по прямой
        # просьбе пользователя "ускорь всё это в один параллельный цикл")
        # — раньше здесь было ДВА ПОСЛЕДОВАТЕЛЬНЫХ этапа: дешёвый check_price
        # по last-цене ТИКЕРА (для отбора кандидатов), а ПОТОМ отдельно, для
        # кандидатов, _verify_close_still_profitable по стакану (VWAP).
        # Раз решение о закрытии и так снова принимается по VWAP (см.
        # комментарий у _estimate_total_pnl про 2026-09-10/IOST), второй
        # отдельный проход избыточен — fetch_order_book даёт ОДНИМ запросом
        # и референсную цену для спреда, и VWAP для PnL, без разделения на
        # "дешёвый прикидочный" и "дорогой финальный" этапы.
        async def check_close_vwap(position: "test_batch_mod.TestBatchPosition"):
            long_client = await self._get_client(position.long_exchange)
            short_client = await self._get_client(position.short_exchange)
            if long_client is None or short_client is None:
                return None
            # СНАЧАЛА пробуем потоковый стакан (WebSocket, см. book_stream.py):
            # он лежит в памяти и читается мгновенно, вместо ~560мс REST-запроса
            # на КАЖДЫЙ цикл мониторинга. Если потока нет или он молчит —
            # честно идём по REST, как раньше (fail-open, торговая логика
            # от этого не зависит).
            long_ex_id = EXCHANGE_ALIASES.get(position.long_exchange.lower(), position.long_exchange.lower())
            short_ex_id = EXCHANGE_ALIASES.get(position.short_exchange.lower(), position.short_exchange.lower())
            long_book = book_stream.get_book(long_ex_id, position.long_symbol)
            short_book = book_stream.get_book(short_ex_id, position.short_symbol)
            try:
                if long_book is None or short_book is None:
                    fetched = await asyncio.gather(
                        long_client.fetch_order_book(position.long_symbol, limit=50) if long_book is None else _already(long_book),
                        short_client.fetch_order_book(position.short_symbol, limit=50) if short_book is None else _already(short_book),
                    )
                    long_book, short_book = fetched
            except Exception as exc:
                print(f"[test_batch] {position.coin}: не удалось получить стакан ({type(exc).__name__}: {exc})")
                return None

            long_bids, long_asks = long_book.get("bids"), long_book.get("asks")
            short_bids, short_asks = short_book.get("bids"), short_book.get("asks")
            if not long_bids or not long_asks or not short_bids or not short_asks:
                return None

            # ДЕЛЬТА-НЕЙТРАЛЬНАЯ МАТЕМАТИКА (по вашему уточнению 2026-09-06):
            # рынок может пойти в любую сторону — одна нога будет в плюсе,
            # другая в минусе, и решение "закрывать или нет" нельзя принимать
            # ТОЛЬКО по текущему спреду. ГЛАВНОЕ условие — суммарный
            # (Long+Short) PnL уже РЕАЛЬНО положительный (после оценочных
            # комиссий, если они известны). Требование к спреду смягчено —
            # достаточно, чтобы спред опустился ДО close_spread_max_pct или
            # ниже (верхняя граница диапазона, см. TestBatchConfig).
            #
            # РЕАЛЬНАЯ цена закрытия (не last/mid, а VWAP walk по объёму
            # позиции): закрытие LONG — это ПРОДАЖА (walk по bids), закрытие
            # SHORT — это ПОКУПКА (walk по asks). См. комментарий у
            # _estimate_total_pnl про 2026-09-10/IOST — решение по markPrice
            # биржи однажды разошлось с реальным исполнением, вернулись на
            # цену/VWAP как более надёжный вариант.
            long_exit_price = TradeExecutionTool._vwap_from_levels(long_bids, position.amount_usdt)
            short_exit_price = TradeExecutionTool._vwap_from_levels(short_asks, position.amount_usdt)
            if not long_exit_price or not short_exit_price:
                return None

            # СПРЕД СЧИТАЕМ ПО ЦЕНАМ ИСПОЛНЕНИЯ, А НЕ ПО СЕРЕДИНЕ СТАКАНА
            # (исправлено 2026-09-13 по разбору ночной статистики). РАНЬШЕ
            # здесь бралась середина книги (bid+ask)/2 на каждой бирже — и
            # это СИСТЕМАТИЧЕСКИ врало в нашу невыгоду: закрываясь, мы
            # ПЕРЕСЕКАЕМ обе книги (длинную ногу продаём по bid, короткую
            # откупаем по ask), поэтому реально исполнимый спред всегда
            # ШИРЕ среднего — на сумму полуспредов обеих бирж.
            #
            # Замер по 11 реальным сделкам за ночь 13.09: расхождение между
            # спредом, который видел монитор, и спредом по ФАКТИЧЕСКИМ ценам
            # исполнения — в среднем +1.01 процентного пункта, в 9 случаях
            # из 11 в худшую сторону (до +2.87пп на ZCAT). Из-за этого
            # триггер "спред сошёлся до close_spread_max_pct" срабатывал
            # РАНО: монитор видел 1%, а исполниться можно было только по 2%,
            # и мы фиксировали ~0.2% схождения при комиссиях, требующих
            # 0.24% только для выхода в ноль — отсюда 5 из 6 ночных убытков
            # с ПОЛОЖИТЕЛЬНЫМ gross и отрицательным net.
            #
            # Теперь спред считается ровно по тем ценам, по которым мы
            # реально закроемся — "спред 1%" означает настоящий 1%.
            long_exit_norm = _to_usdt_sync(long_exit_price, position.long_exchange)
            short_exit_norm = _to_usdt_sync(short_exit_price, position.short_exchange)
            current_spread = (short_exit_norm - long_exit_norm) / long_exit_norm * 100

            gross_pnl, net_pnl = self._estimate_total_pnl(position, long_exit_price, short_exit_price)
            if gross_pnl is None:
                # Нет реальной цены входа (например, DRY_RUN) — оценить PnL
                # нечем. ИСПРАВЛЕНО 2026-09-12 по явной просьбе пользователя
                # ("нас интересует только +, не закрывай в минус") — раньше
                # здесь стояло profitable=True (fail-open: закрывали по
                # одному только спреду, без реального подтверждения
                # прибыли) — единственная реальная дыра в правиле "не
                # закрываем в минус": если по какой-то причине цену входа
                # не удалось определить, эта ветка могла закрыть позицию
                # БЕЗ проверки PnL вообще. Теперь fail-CLOSED: раз прибыль
                # подтвердить нечем — не закрываем (кроме DRY_RUN, где
                # реальных денег и так нет — там ждать нечего, спред
                # достаточен).
                dry_run = os.getenv("DRY_RUN", "True").lower() == "true"
                profitable = dry_run
                pnl_estimate = None
            else:
                pnl_estimate = net_pnl if net_pnl is not None else gross_pnl
                # ЗАПАС ПРОЧНОСТИ (добавлено 2026-09-13 по прямой просьбе
                # пользователя, реальный случай FONE: закрылась на VWAP-
                # оценке +$0.0054 gross, но между решением и реальным
                # исполнением цена сдвинулась на волосок, и после комиссий
                # итог ушёл в -$0.0033) — раньше "прибыльно" означало
                # ЛЮБОЕ положительное число, хоть +$0.0001, без запаса на
                # микро-проскальзывание за секунды между решением и
                # реальной отправкой ордеров закрытия. Теперь требуем
                # положительный результат ЗАМЕТНО больше нуля — не только
                # для min_profit_usdt-ветки ниже (у неё свой, более
                # высокий порог), а для САМОГО факта "мы в плюсе" в
                # принципе, включая путь "спред сошёлся до close_spread_
                # max_pct".
                close_profit_buffer = _get_float_env("SCANNER_CLOSE_PROFIT_BUFFER_USDT", 0.02)
                profitable = pnl_estimate > close_profit_buffer

            return position, current_spread, profitable, pnl_estimate

        results = await asyncio.gather(*(check_close_vwap(p) for p in positions))

        # Абсолютный порог прибыли (в USDT) — по явной просьбе пользователя
        # 2026-09-08: "как только выигрышная нога перекрывает минусовую и
        # комиссию, и получает более 0.15 usdt прибыли — может так же
        # закрываться с успехом". В ОТЛИЧИЕ от self.breakeven_close_coins
        # (только явно перечисленные монеты, PnL >= 0) — это ОБЩЕЕ правило
        # для ЛЮБОЙ монеты: не нужно вручную добавлять в список каждую
        # "застрявшую" монету (как раньше делали для BP/FONE) — если
        # прибыль уже заметная (не копейки на грани нуля), закрываем сразу,
        # не дожидаясь схождения спреда до close_spread_max_pct.
        min_profit_usdt = _get_float_env("SCANNER_CLOSE_MIN_PROFIT_USDT", 0.15)

        # ЗАЩИТА ДЛЯ "ШИРОКИХ" ПОЗИЦИЙ БЕЗ ИСТОРИИ СХОЖДЕНИЯ (добавлено
        # 2026-09-11, см. подробный комментарий в
        # _handle_test_batch_opportunity про opp["wide_spread"] и реальный
        # случай STORJ) — такие позиции открыты, ЗАРАНЕЕ зная, что спред
        # исторически мог вообще не сходиться до close_spread_max_pct.
        # ПРАВИЛА (по явной просьбе пользователя, ОБНОВЛЕНО 2026-09-12):
        # "не закрывай сделку в -... ждём только +пнл... нас интересует
        # только +" — лимит времени, который раньше закрывал такую позицию
        # ПРИНУДИТЕЛЬНО даже в убытке (единственная защита от бесконечного
        # удержания, которую сам же пользователь просил раньше), ОТМЕНЁН
        # по его явному подтверждению ("убрать лимит, ждать только +"),
        # несмотря на озвученный риск — позиция теперь МОЖЕТ занимать
        # маржу/слот на бирже сколь угодно долго, если прибыль так и не
        # придёт. Единственное условие закрытия, ТОЛЬКО для
        # position.wide_spread — тейк-профит В ПРОЦЕНТАХ от вложенной
        # суммы (не в USDT — по просьбе "если мы уходим в 2% плюса ...
        # это тоже прибыль для нас"), заметно выше общего
        # SCANNER_CLOSE_MIN_PROFIT_USDT. Для обычных (не wide_spread)
        # позиций поведение НЕ меняется.

        def _should_close(r) -> "tuple[bool, str | None]":
            """Возвращает (закрывать_ли, принудительная_причина). Причина
            None означает обычное закрытие по достижении цели — вызывающий
            код сам сформирует стандартный текст ('спред сошёлся до X%')."""
            if r is None:
                return False, None
            position, current_spread, profitable, pnl_estimate = r

            if position.wide_spread:
                # ОБНОВЛЕНО 2026-09-13 по прямой просьбе пользователя ("закрывай
                # когда будет +0.10 с учётом комиссии и минусовой ноги"): для
                # широких позиций та же планка, что и для всех остальных —
                # чистый PnL обеих ног после комиссий > min_profit_usdt. Прежний
                # отдельный тейк-профит в процентах (2% ~ $1.00 на $50) отменён
                # — он заставлял бы держать позицию много дольше ради большего
                # куша, пользователь предпочёл забирать стабильные +$0.10.
                # Единственное отличие от обычных позиций сохраняется: путь
                # "спред сошёлся до close_spread_max_pct" здесь не используется
                # (у широкой монеты он по истории не сходится — иначе она не
                # была бы широкой); в минус не закрываемся, лимита времени нет.
                if pnl_estimate is not None and pnl_estimate > min_profit_usdt:
                    return True, (
                        f"широкий спред: чистая прибыль {pnl_estimate:+.4f} USDT "
                        f"(обе ноги, после комиссий) превысила ${min_profit_usdt:.2f} — закрываю"
                    )
                return False, None

            if not profitable:
                return False, None
            if position.coin.upper() in self.breakeven_close_coins:
                # См. self.breakeven_close_coins — не ждём схождения спреда,
                # достаточно net_pnl >= 0.
                return True, None
            if pnl_estimate is not None and pnl_estimate >= min_profit_usdt:
                # См. min_profit_usdt выше — заметная прибыль сама по себе
                # достаточна, спред можно не ждать.
                return True, None
            return current_spread <= self.test_batch.close_spread_max_pct, None

        # candidates уже посчитаны по VWAP (см. check_close_vwap выше) — то
        # есть это уже и есть "финальная проверка по стакану", отдельный
        # второй проход (_verify_close_still_profitable) больше не нужен,
        # см. комментарий у check_close_vwap. Закрываем ВСЕ достигшие цели
        # ОДНОВРЕМЕННО — без последовательных задержек между позициями.
        candidates = []
        for r in results:
            should_close, forced_reason = _should_close(r)
            if should_close:
                candidates.append((r[0], r[1], forced_reason))
        if candidates:
            await asyncio.gather(
                *(self._close_test_batch_position(pos, spread, reason=reason) for pos, spread, reason in candidates)
            )

    async def _verify_close_still_profitable(self, position: "test_batch_mod.TestBatchPosition") -> bool:
        """СЕЙЧАС НЕ ИСПОЛЬЗУЕТСЯ (объединена с check_price в один
        параллельный проход внутри _monitor_test_batch, 2026-09-10, по
        прямой просьбе пользователя "ускорь всё это в один параллельный
        цикл" — раз дешёвый первый проход и так стал VWAP-based, отдельный
        второй проход этой же проверкой избыточен). Оставлена как есть —
        пригодится, если понадобится вернуть двухэтапную схему.

        Финальная проверка ПЕРЕД фактическим закрытием — по РЕАЛЬНОМУ
        стакану (VWAP на объём позиции), а не по last-цене. Закрытие LONG —
        это ПРОДАЖА (walk по bids), закрытие SHORT — это ПОКУПКА (walk по
        asks); переиспользует TradeExecutionTool._vwap_from_levels (та же
        логика, что и для входа в trade_tool.py — общий static-метод, без
        дублирования кода).

        В ОТЛИЧИЕ от большинства soft-fail проверок в этом файле (которые
        при сбое сети "пропускают" проверку, чтобы не блокировать сделку),
        здесь при ЛЮБОЙ ошибке возвращаем False — не закрываем в этом
        цикле, попробуем на следующем (через 0.5с): риск закрыть в
        РЕАЛЬНЫЙ убыток из-за неполных данных важнее небольшой задержки
        закрытия честно прибыльной позиции."""
        long_client = await self._get_client(position.long_exchange)
        short_client = await self._get_client(position.short_exchange)
        if long_client is None or short_client is None:
            return False
        try:
            long_book, short_book = await asyncio.gather(
                long_client.fetch_order_book(position.long_symbol, limit=50),
                short_client.fetch_order_book(position.short_symbol, limit=50),
            )
        except Exception as exc:
            print(
                f"[test_batch] {position.coin}: не удалось перепроверить закрытие "
                f"по стакану ({type(exc).__name__}: {exc}) — откладываю до следующего цикла."
            )
            return False

        long_exit_price = TradeExecutionTool._vwap_from_levels(long_book.get("bids"), position.amount_usdt)
        short_exit_price = TradeExecutionTool._vwap_from_levels(short_book.get("asks"), position.amount_usdt)
        if not long_exit_price or not short_exit_price:
            return False

        # ВАЖНО: передаём цены в ЛОКАЛЬНОЙ валюте бирж (без _to_usdt_sync) —
        # так же, как check_price делает для _estimate_total_pnl: entry_price
        # в position тоже хранится в локальной валюте, сравнение должно быть
        # той же линейкой с обеих сторон (нормализация нужна только для
        # СПРЕДА между биржами, не для PnL одной ноги относительно её же
        # входа).
        gross_pnl, net_pnl = self._estimate_total_pnl(position, long_exit_price, short_exit_price)
        if gross_pnl is None:
            return True  # DRY_RUN/нет цены входа — сравнивать нечего, не блокируем
        profitable = (net_pnl if net_pnl is not None else gross_pnl) > 0
        if not profitable:
            print(
                f"[test_batch] {position.coin}: last-цена показала прибыль, но VWAP "
                f"по стакану — нет (gross={gross_pnl:+.4f}$, net="
                f"{f'{net_pnl:+.4f}$' if net_pnl is not None else 'н/д'}) — откладываю закрытие."
            )
        return profitable

    @staticmethod
    async def _fetch_leg_unrealized_pnl(exchange_name: str, symbol: str) -> "float | None":
        """PnL этой НОГИ по версии САМОЙ БИРЖИ (markPrice) — по прямой
        просьбе пользователя 2026-09-09: "нам нужно ориентироваться по
        бирже, давай попробуем так" (реальный повод: BONER на Aster/
        MEXC — наша VWAP-оценка по стакану показывала -0.05$, биржа в
        интерфейсе показывала +0.06$; пользователь решил доверять
        цифрам биржи, а не нашему пересчёту).

        ВАЖНО, честно: markPrice — справочная цена биржи, НЕ обязательно
        цена, по которой реально исполнится закрытие рыночным ордером на
        тонком стакане (именно ради этой разницы раньше и был сделан
        VWAP-пересчёт — см. историю FONE в комментариях выше). Это
        сознательный компромисс по прямой просьбе пользователя, а не
        "более правильный" способ сам по себе.

        Требует АУТЕНТИФИЦИРОВАННОГО клиента (fetch_positions недоступен
        публичному API биржи) — используем TradeExecutionTool._get_ready_
        client (с реальными ключами, тот же прогретый клиент, что и для
        реальных ордеров), НЕ self._get_client (тот публичный, для
        сканирования, без ключей).

        Возвращает None при любой ошибке или если позиция на бирже не
        нашлась — вызывающий код должен считать это "нет данных", а не
        "ноль прибыли" (fail-closed)."""
        tool = TradeExecutionTool()
        try:
            exchange = await tool._get_ready_client(exchange_name)
            positions = await exchange.fetch_positions([symbol])
        except Exception as exc:
            print(
                f"[test_batch] {symbol} на {exchange_name}: не удалось получить "
                f"unrealizedPnl биржи ({type(exc).__name__}: {exc})."
            )
            return None
        for p in positions:
            if p.get("symbol") != symbol:
                continue
            pnl = p.get("unrealizedPnl")
            if pnl is not None:
                return float(pnl)
            # Некоторые биржи (замечено на MEXC) не заполняют унифицированное
            # поле CCXT — берём из "сырого" info с известными вариантами
            # названия поля.
            info = p.get("info") or {}
            for key in (
                "unRealizedPnl", "unrealizedPnl", "unRealizedProfit",
                "unrealised_pnl", "unrealisedPnl",
            ):
                if key in info and info[key] is not None:
                    try:
                        return float(info[key])
                    except (TypeError, ValueError):
                        continue
        return None

    async def _estimate_total_pnl_exchange(self, position: "test_batch_mod.TestBatchPosition"):
        """То же самое (gross_pnl, net_pnl), что и _estimate_total_pnl, но
        считает по PnL, который отдаёт САМА БИРЖА для каждой ноги (см.
        _fetch_leg_unrealized_pnl) — по прямой просьбе пользователя
        2026-09-09. Возвращает (None, None), если хотя бы одна нога
        недоступна (сетевая ошибка/позиция не нашлась) — fail-closed."""
        long_pnl, short_pnl = await asyncio.gather(
            self._fetch_leg_unrealized_pnl(position.long_exchange, position.long_symbol),
            self._fetch_leg_unrealized_pnl(position.short_exchange, position.short_symbol),
        )
        if long_pnl is None or short_pnl is None:
            return None, None
        gross_pnl = long_pnl + short_pnl
        net_pnl = None
        if position.entry_fee_total is not None:
            # Та же грубая оценка комиссии за выход, что и в
            # _estimate_total_pnl — считаем её равной комиссии за вход.
            net_pnl = gross_pnl - position.entry_fee_total * 2
        return gross_pnl, net_pnl

    @staticmethod
    def _estimate_total_pnl(position: "test_batch_mod.TestBatchPosition", long_price: float, short_price: float):
        """ОСНОВНОЙ способ оценки PnL для решения "закрывать или нет" — по
        ЦЕНЕ (тикер для дешёвого первого прохода, VWAP по стакану для
        финальной проверки перед реальным закрытием), а не по markPrice
        биржи. Был временно заменён на _estimate_total_pnl_exchange
        2026-09-09 ("ориентироваться по бирже"), но возвращён обратно
        2026-09-10 после реального случая с IOST — закрытие "по бирже"
        показало прибыль в момент решения, а реальное исполнение (с
        проскальзыванием) дало небольшой убыток. _estimate_total_pnl_
        exchange оставлена в коде — пригодится, если понадобится свериться
        глазами с интерфейсом биржи, но решение о закрытии теперь снова
        принимается по этой функции.

        Оценка ВАЛОВОГО (до комиссий) и ЧИСТОГО (после оценочной
        комиссии за выход) суммарного PnL связки (Long_PnL + Short_PnL) по
        ТЕКУЩИМ ценам — используется как гейт "прибыль уже положительна"
        перед фактическим закрытием (см. _monitor_test_batch). Возвращает
        (None, None), если цена входа неизвестна (DRY_RUN-открытие — не с
        чем сравнивать), иначе (gross_pnl, net_pnl); net_pnl — None, если
        комиссия за вход неизвестна (тогда сравниваем по gross_pnl).

        РЕАЛЬНЫЕ цифры для отчёта/статистики считаются ПОСЛЕ фактического
        закрытия в trade_executor.close_structured_signal_detailed() — это
        только предварительная оценка для решения "закрывать сейчас или
        подождать ещё цикл"."""
        if not position.long_entry_price or not position.short_entry_price:
            return None, None
        amount_coin_long = position.amount_usdt / position.long_entry_price
        amount_coin_short = position.amount_usdt / position.short_entry_price
        long_pnl = (long_price - position.long_entry_price) * amount_coin_long
        short_pnl = (position.short_entry_price - short_price) * amount_coin_short
        gross_pnl = long_pnl + short_pnl
        net_pnl = None
        if position.entry_fee_total is not None:
            # Комиссию за ВЫХОД оцениваем той же суммой, что и за вход (тот
            # же объём/биржи — ставка обычно не меняется) — грубая, но
            # достаточная оценка именно для этого гейта.
            net_pnl = gross_pnl - position.entry_fee_total * 2
        return gross_pnl, net_pnl

    async def _close_test_batch_position(
        self, position: "test_batch_mod.TestBatchPosition", current_spread: float, reason: str = None
    ) -> None:
        # ПРЯМОЙ await вместо run_in_executor (2026-09-10, по прямой
        # просьбе пользователя "нагружай максимально... сделай
        # параллельным, чтобы закрывались по нужной цене") — этот код и
        # так уже выполняется на event loop бота, поэтому лишний прыжок
        # через отдельный поток (который внутри всё равно планирует
        # корутину ОБРАТНО на этот же loop через run_coroutine_threadsafe
        # и блокирующе ждёт результата) только добавлял переключения
        # контекста между решением "закрываем" и реальной отправкой
        # ордеров на биржи. См. close_structured_signal_detailed_async
        # в trade_executor.py.
        #
        # reason (добавлено 2026-09-11) — позволяет вызывающему коду
        # (_monitor_test_batch: _should_close) подставить свою причину
        # закрытия вместо стандартной "спред сошёлся" — нужно для
        # принудительных закрытий wide_spread-позиций (лимит времени/
        # стоп-лосс), где реальная причина закрытия ДРУГАЯ (см. там же).
        try:
            result = await trade_executor.close_structured_signal_detailed_async(
                position.coin, self.notifier,
                reason or f'спред сошёлся до {current_spread:.2f}% (тестовая серия)',
                # ФИНАЛЬНАЯ проверка прибыли по живому стакану прямо перед
                # отправкой ордеров (добавлено 2026-09-13, реальный случай
                # STORJ: решение при спреде 0.99%, исполнение по факту при
                # 3.28%) — тот же порог, что и у самого решения о закрытии,
                # чтобы гейт не спорил сам с собой: раз PnL успел упасть
                # ниже — просто ждём следующего цикла, а не фиксируем
                # ухудшившийся результат.
                min_net_pnl=_get_float_env("SCANNER_CLOSE_PROFIT_BUFFER_USDT", 0.02),
            )
        except Exception as exc:
            print(f"[test_batch] КРИТИЧЕСКАЯ ОШИБКА закрытия {position.coin}: {exc}")
            try:
                self.notifier.notify_error(
                    symbol=position.coin,
                    error_message=f"Необработанное исключение при закрытии тестовой сделки: {exc}",
                )
            except Exception:
                pass
            return

        if result is None:
            # Позиции уже нет в учёте (закрыта извне/вручную) — снимаем из
            # трекера серии без статистики, чтобы не зависла в мониторинге.
            print(f"[test_batch] {position.coin}: позиция не найдена в учёте — снимаю из серии без статистики.")
            self.test_batch.open_positions.pop(position.coin, None)
            return

        if result.get("close_aborted"):
            # Закрытие ОТМЕНЕНО финальной проверкой прибыли по живому
            # стакану (см. _verify_close_still_worth_it) — не ошибка, а
            # штатное "ещё не время". Позиция остаётся в мониторинге,
            # следующий цикл проверит заново. Лог уже напечатан внутри.
            return

        if not result["both_ok"]:
            # Не обе ноги закрылись — ОСТАЁТСЯ в open_positions серии,
            # следующий цикл мониторинга попробует снова (ошибка уже
            # ушла отдельным notify_error изнутри close_structured_signal_detailed).
            print(f"[test_batch] {position.coin}: не обе ноги закрылись — остаётся в мониторинге.")
            return

        pnl_amount = result["net_pnl"] if result["net_pnl"] is not None else (result["gross_pnl"] or 0.0)
        pnl_percent = result["net_pnl_pct"] if result["net_pnl_pct"] is not None else (result["gross_pnl_pct"] or 0.0)
        # Результат КАЖДОЙ ноги отдельно (см. trade_executor.py) — именно
        # эти цифры показывают "плюс перекрыл минус" в алерте ниже.
        long_pnl = result.get("long_pnl")
        short_pnl = result.get("short_pnl")

        trade = self.test_batch.record_close(
            position.coin, current_spread, long_pnl, short_pnl, pnl_amount, pnl_percent
        )
        if trade is None:
            return  # не должно случиться (позиция была в open_positions), но на всякий случай

        try:
            self.notifier.notify_test_position_closed(
                symbol=position.coin,
                long_exchange=position.long_exchange,
                short_exchange=position.short_exchange,
                entry_spread_pct=trade.entry_spread_pct,
                exit_spread_pct=trade.exit_spread_pct,
                long_pnl=trade.long_pnl,
                short_pnl=trade.short_pnl,
                pnl_amount=trade.pnl_amount,
                pnl_percent=trade.pnl_percent,
                closed_count=len(self.test_batch.closed_trades),
                # В непрерывном режиме "всего" не существует — показываем
                # то же число, что и закрыто, чтобы в алерте было "N / N",
                # а не бессмысленное "N / 0".
                total_count=(
                    len(self.test_batch.closed_trades)
                    if self.test_batch.unlimited
                    else self.test_batch.size
                ),
            )
        except Exception as exc:
            print(f"[test_batch] Не удалось отправить алерт закрытия {position.coin}: {exc}")

        elapsed_ms = result.get("elapsed_ms")
        print(
            f"[test_batch] Закрыта #{position.coin}: PnL {pnl_amount:+.4f}$ ({pnl_percent:+.2f}%), "
            f"удержание {trade.holding_seconds:.0f}с"
            + (f", задержка между ногами закрытия {elapsed_ms:.0f}мс" if elapsed_ms is not None else "")
            + (
                f". Всего закрыто: {len(self.test_batch.closed_trades)}."
                if self.test_batch.unlimited
                else f". Серия: {len(self.test_batch.closed_trades)}/{self.test_batch.size} закрыто."
            )
        )

        if self.test_batch.state == test_batch_mod.DONE:
            await self._send_test_batch_summary()

    async def _send_test_batch_summary(self) -> None:
        stats = self.test_batch.summary()
        print(
            f"[test_batch] СЕРИЯ ЗАВЕРШЕНА: {stats['closed_count']}/{self.test_batch.size} закрыто, "
            f"общий PnL {stats['total_pnl']:+.4f}$, win-rate {stats['win_rate_pct']:.1f}% "
            f"({stats['wins']}W/{stats['losses']}L), среднее удержание "
            f"{stats['avg_holding_seconds']:.0f}с."
        )
        try:
            self.notifier.notify_test_batch_summary(
                total_count=self.test_batch.size,
                total_pnl=stats["total_pnl"],
                avg_holding_seconds=stats["avg_holding_seconds"],
                wins=stats["wins"],
                losses=stats["losses"],
                win_rate_pct=stats["win_rate_pct"],
            )
        except Exception as exc:
            print(f"[test_batch] Не удалось отправить итоговый отчёт: {exc}")

    # -------------------------------------------------------------------
    # Форматирование HTML-алерта (моноширинный блок <pre>) — см. ТЗ:
    # тикер, % спреда, цены на обеих биржах, фандинг + время до выплаты.
    # -------------------------------------------------------------------
    @staticmethod
    def _format_funding_line(label: str, rate, timestamp_ms) -> str:
        if rate is None:
            rate_str = "н/д"
        else:
            rate_str = f"{rate * 100:+.4f}%"
        if timestamp_ms:
            remaining = (timestamp_ms / 1000) - datetime.now(timezone.utc).timestamp()
            if remaining > 0:
                hours, rem = divmod(int(remaining), 3600)
                minutes = rem // 60
                time_str = f"через {hours}ч {minutes}м"
            else:
                time_str = "уже наступила"
        else:
            time_str = "н/д"
        return f"Funding {label:<8}: {rate_str:>10}  (след. выплата {time_str})"

    @staticmethod
    def _format_overview_block(all_exchanges: dict) -> str:
        """Блок "All Exchanges Overview" — цены на ВСЕХ биржах, где видна
        эта монета (включая выбранную long/short-пару), отсортированы по
        возрастанию цены — как в самом канале сигналов ("📝 All Exchanges
        Overview: 🟢HYPERLIQUID: $0.14193 ⚪️MEXC: $0.1464 ⚫️BYBIT: $14.082").
        Показывается только если монета торгуется на 3+ биржах — на двух
        это просто повторило бы строки LONG/SHORT выше."""
        if len(all_exchanges) < 3:
            return ""
        rows = sorted(all_exchanges.items(), key=lambda kv: kv[1].get("price") or 0)
        lines = [
            f"{ex_id.upper():<8}: {data['price']:.8g}"
            for ex_id, data in rows
            if data.get("price")
        ]
        return "\nAll Exchanges Overview:\n" + "\n".join(lines)

    def _format_alert_html(self, opp: dict) -> str:
        coin = html_escape(opp["coin"])
        long_ex = html_escape(opp["long_exchange"].upper())
        short_ex = html_escape(opp["short_exchange"].upper())
        # Показываем "будет исполнена" только если авто-торговля реально
        # сработает по ЭТОЙ находке — включена И спред проходит именно
        # порог авто-торговли (AUTO_TRADE_MIN_SPREAD), а не только порог
        # алерта (тот обычно ниже — иначе эта находка сюда бы не попала).
        will_auto_trade = (
            self.auto_trade.enabled and opp["spread_pct"] >= self.auto_trade.min_spread_percent
        )
        auto_trade_note = "\n🤖 AUTO_TRADE: сделка передана в исполнение." if will_auto_trade else ""
        overview = self._format_overview_block(opp.get("all_exchanges") or {})

        body = (
            f"#{coin}          Spread: {opp['spread_pct']:.2f}%\n"
            f"LONG  {long_ex:<8}: {opp['long_price']:.8g}\n"
            f"SHORT {short_ex:<8}: {opp['short_price']:.8g}\n"
            f"\n"
            f"{self._format_funding_line(long_ex, opp.get('long_funding_rate'), opp.get('long_funding_ts'))}\n"
            f"{self._format_funding_line(short_ex, opp.get('short_funding_rate'), opp.get('short_funding_ts'))}"
            f"{overview}"
        )
        return (
            f"🔍 <b>СКАНЕР: найдена связка</b>\n"
            f"<pre>{html_escape(body)}</pre>"
            f"{auto_trade_note}"
        )
