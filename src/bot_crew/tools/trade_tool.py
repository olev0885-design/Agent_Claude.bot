# =============================================================================
# trade_tool.py — КАСТОМНЫЙ ИНСТРУМЕНТ (Tool) для агента trade_executor.
# =============================================================================
# Здесь реализовано АСИНХРОННОЕ одновременное исполнение двух встречных
# ордеров (LONG на одной бирже, SHORT на другой) через библиотеку CCXT.
# Асинхронность (asyncio.gather) нужна, чтобы отправить оба ордера
# "одновременно" — с минимальной задержкой между ними, а не по очереди.
# =============================================================================

# --- Импорты стандартной библиотеки Python ----------------------------------
import asyncio  # Модуль для асинхронного программирования (корутины, gather)
import os       # Для чтения переменных окружения (ключи API бирж)
import re       # Для разбора текста ошибок биржи (см. _set_leverage_with_fallback)
import time     # Для замера реальной задержки между исполнением ордеров
from typing import Optional, Type  # Для аннотаций типов

# --- Импорты сторонних библиотек ---------------------------------------------
# ccxt.async_support — асинхронная версия CCXT (в отличие от обычного ccxt,
# методы здесь — корутины: их нужно вызывать через "await").
import ccxt.async_support as ccxt_async
from crewai.tools import BaseTool   # Базовый класс для всех инструментов CrewAI
from pydantic import BaseModel, Field  # Для строгой схемы входных аргументов

# --- Локальный импорт ---------------------------------------------------
# См. blocked_coins_store.py — автоматическая блокировка монеты после
# нескольких РЕАЛЬНЫХ откатов подряд (не путать с VWAP pre-check
# отменами, те денег не тратят и здесь не учитываются).
from bot_crew import blocked_coins_store
# См. private_stream.py — приватные WebSocket-потоки (позиции/ордера).
# Используется здесь ТОЛЬКО для мгновенного подтверждения исполнения
# ордера (см. _verify_order_filled); при любом сбое потока путь
# полностью откатывается на прежние REST-проверки.
from bot_crew import private_stream
# См. book_stream.py — потоковые стаканы; _get_book_snapshot сначала
# смотрит туда и лишь при отсутствии/устаревании идёт по REST.
from bot_crew import book_stream
# См. _persist_provisional_position — запись позиции в учёт СРАЗУ после
# исполнения обеих ног, до любой косметики (инцидент BONER 2026-09-15).
from bot_crew import position_store


# =============================================================================
# АЛИАСЫ БИРЖ: канал сигналов иногда называет биржу не так, как она
# называется в CCXT (например, "HLIQUID" вместо "hyperliquid"). Ключи —
# названия из канала В НИЖНЕМ РЕГИСТРЕ, значения — реальные ID классов CCXT.
# =============================================================================
EXCHANGE_ALIASES = {
    "hliquid": "hyperliquid",
    "hyperliquid": "hyperliquid",
    # KuCoin: CCXT держит спот ("kucoin") и фьючерсы ("kucoinfutures") как
    # РАЗНЫЕ классы — нам нужны перпетуалы, поэтому "kucoin" (как канал/
    # SkySpreads/.env его называют) транслируем в "kucoinfutures". Ключи в
    # .env всё равно называются KUCOIN_* (см. _get_exchange_credentials —
    # использует exchange_name, а не exchange_id, алиас его не трогает).
    "kucoin": "kucoinfutures",
    # Binance: аналогично — спот ("binance") и USDT-маржинальные перпетуалы
    # ("binanceusdm") РАЗНЫЕ классы CCXT, нам нужны фьючерсы.
    "binance": "binanceusdm",
}

# Валюта, в которой котируется перпетуал-контракт (unified CCXT symbol —
# "COIN/QUOTE:QUOTE"). У подавляющего большинства бирж канала это USDT, но
# у Hyperliquid контракты котируются в USDC — если строить символ как
# "COIN/USDT:USDT" на Hyperliquid, CCXT не найдёт такой рынок вообще
# (BadSymbol), и ЛЮБОЙ запрос (цена, комиссия, ордер) на этой бирже упадёт.
# Проверено 2026-08-28: load_markets() на всех остальных биржах канала
# (bybit/mexc/bitget/gate/bingx/aster) показывает именно "*/USDT:USDT".
EXCHANGE_QUOTE_CURRENCY = {
    "hyperliquid": "USDC",
}


def _build_symbol(exchange_name: str, coin: str) -> str:
    """CCXT unified symbol перпетуал-контракта для конкретной биржи (см.
    EXCHANGE_QUOTE_CURRENCY выше — у разных бирж разная валюта котировки)."""
    exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
    quote = EXCHANGE_QUOTE_CURRENCY.get(exchange_id, "USDT")
    return f"{coin.upper()}/{quote}:{quote}"


# =============================================================================
# КУРС USDC/USDT — по явной просьбе пользователя 2026-09-07: "запомни
# сейчас курс юсдс к юсдт и подтягивай его ко всем [расчётам спреда/PnL]".
# Hyperliquid котируется в USDC (см. EXCHANGE_QUOTE_CURRENCY), все
# остальные биржи канала — в USDT. USDC/USDT НЕ равны строго 1:1 (реально
# ~1.0001-1.0002 на споте gate/mexc/bitget 2026-09-07) — при сравнении цен
# между Hyperliquid и любой другой биржей (расчёт спреда, направление
# входа, VWAP-гейт перед ордером) это небольшое, но РЕАЛЬНОЕ искажение,
# если считать USDC=USDT напрямую. Кэшируем курс (а не один статичный
# снимок навсегда) — тонкий стейблкоин-спред медленно дрейфует, но за
# часы/дни работы бота может отличаться от значения на момент запроса
# этой фичи; TTL даёт актуальность без лишнего сетевого запроса на КАЖДЫЙ
# цикл сканера (тот и так уже бьёт по нескольким биржам).
# =============================================================================
_usdc_usdt_rate_cache: dict = {"rate": None, "fetched_at": 0.0}
_USDC_USDT_RATE_TTL_SECONDS = 300.0  # 5 минут — курс стейблкоина меняется медленно


async def _get_usdc_usdt_rate() -> float:
    """Текущий курс 1 USDC = X USDT (спот, среднее bid/ask с gate, с
    фолбэком на mexc/bitget при сбое) — кэшируется на
    _USDC_USDT_RATE_TTL_SECONDS. При любой ошибке сети возвращает
    ПОСЛЕДНЕЕ известное значение (если есть) или 1.0 как консервативный
    дефолт (лучше слегка неточный спред, чем упавшая проверка из-за сбоя
    третьестепенного запроса за курсом стейблкоина)."""
    now = time.monotonic()
    if (
        _usdc_usdt_rate_cache["rate"] is not None
        and now - _usdc_usdt_rate_cache["fetched_at"] < _USDC_USDT_RATE_TTL_SECONDS
    ):
        return _usdc_usdt_rate_cache["rate"]

    for exchange_id in ("gate", "mexc", "bitget"):
        exchange = None
        try:
            exchange_class = getattr(ccxt_async, exchange_id)
            exchange = exchange_class({"enableRateLimit": True, "timeout": 10000})
            ticker = await exchange.fetch_ticker("USDC/USDT")
            bid, ask = ticker.get("bid"), ticker.get("ask")
            rate = (bid + ask) / 2 if bid and ask else ticker.get("last")
            if rate:
                _usdc_usdt_rate_cache["rate"] = rate
                _usdc_usdt_rate_cache["fetched_at"] = now
                return rate
        except Exception:
            continue
        finally:
            if exchange is not None:
                await exchange.close()

    # Все три биржи не ответили — используем последнее известное значение
    # (даже устаревшее — лучше, чем ничего) или 1.0, если вообще ни разу
    # не удавалось получить курс.
    return _usdc_usdt_rate_cache["rate"] or 1.0


def _get_cached_usdc_usdt_rate_sync() -> float:
    """Версия _get_usdc_usdt_rate БЕЗ сетевого запроса — читает то, что уже
    в кэше (или 1.0, если кэш ещё ни разу не прогревался). Для мест,
    вызываемых ОЧЕНЬ часто (например, scanner.py:_evaluate_pair — на
    каждой паре бирж каждого цикла сканирования, или мониторинг открытых
    позиций каждые 0.5с) — сетевой запрос за курсом стейблкоина на КАЖДЫЙ
    такой вызов был бы явно избыточным. Кэш прогревается асинхронно в
    scanner.py:_find_opportunities_once (см. _get_usdc_usdt_rate)."""
    return _usdc_usdt_rate_cache["rate"] or 1.0


def _to_usdt_sync(price: Optional[float], exchange_name: str) -> Optional[float]:
    """Синхронная версия _to_usdt — см. _get_cached_usdc_usdt_rate_sync."""
    if price is None:
        return None
    exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
    if EXCHANGE_QUOTE_CURRENCY.get(exchange_id) != "USDC":
        return price
    return price * _get_cached_usdc_usdt_rate_sync()


async def _to_usdt(price: Optional[float], exchange_name: str) -> Optional[float]:
    """Переводит цену В ЕДИНУЮ валюту USDT, если она была в USDC (только
    Hyperliquid, см. EXCHANGE_QUOTE_CURRENCY) — иначе возвращает как есть.
    Используется ПОВСЮДУ, где сравниваются/суммируются цены/PnL С РАЗНЫХ
    бирж (расчёт спреда, направление входа, VWAP-гейт, PnL) — без этого
    прямое сравнение "цена в USDC" vs "цена в USDT" считало бы их
    эквивалентными 1:1, что не совсем так."""
    if price is None:
        return None
    exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
    if EXCHANGE_QUOTE_CURRENCY.get(exchange_id) != "USDC":
        return price
    rate = await _get_usdc_usdt_rate()
    return price * rate


def _extract_ticker_price(ticker: dict):
    """Референсная цена тикера с фолбэком — реальный случай (проверено
    2026-09-05): у MEXC testnet 'last' для BNB был None при вполне живых
    bid/ask, и код, бравший ticker['last'] напрямую, падал с
    "unsupported operand type(s) for /: 'float' and 'NoneType'" уже ПОСЛЕ
    того, как первая нога на другой бирже реально открылась — оставляя
    незахеджированную позицию. Та же логика, что и в scanner.py:
    _extract_price (единообразие важно для двух независимых источников
    сигналов, хотя дублируем — там свой async-контекст класса)."""
    price = ticker.get("last")
    if price:
        return price
    bid, ask = ticker.get("bid"), ticker.get("ask")
    if bid and ask:
        return (bid + ask) / 2
    return None


def _persist_provisional_position(
    coin: str, long_exchange: str, short_exchange: str,
    long_result: dict, short_result: dict, amount_usdt: float, leverage: int,
) -> None:
    """ПЕРВОЕ действие после того, как ОБЕ ноги реально исполнились: кладём
    позицию в position_store с тем минимумом, что уже известен (биржи,
    объёмы, цены исполнения, id ордеров). Всё, что бот делает дальше —
    комиссии, замеры, отчёт, Telegram — это обработка УЖЕ СЛУЧИВШЕГОСЯ, и
    ни одна ошибка там не должна оставить открытые ноги без учёта.

    Зачем (реальный инцидент BONER 2026-09-15, -0.82 USDT): раньше запись
    делалась в trade_executor._finalize_open ПОСЛЕ подсчёта задержек и
    сборки отчёта; исключение в подсчёте (int + str) вылетало до записи,
    позиция «терялась», сверка закрывала обе ноги как неучтённые, сканер
    заходил снова. С этой записью такой сценарий невозможен: даже если всё
    последующее упадёт, сверка увидит ноги как СВОИ, а монитор подхватит
    позицию из учёта (см. scanner.py:_adopt_positions_from_store).

    _finalize_open потом ПЕРЕЗАПИШЕТ запись полной версией (с комиссиями)
    — record_open перезаписывает по монете, это штатно. Только для
    боевых ордеров (status == "OK"): у DRY_RUN своя обработка в executor."""
    if long_result.get("status") != "OK" or short_result.get("status") != "OK":
        return
    try:
        position_store.record_open(coin.upper(), {
            "coin": coin.upper(),
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "long_amount_coin": long_result.get("amount_coin"),
            "short_amount_coin": short_result.get("amount_coin"),
            "long_entry_price": long_result.get("price"),
            "short_entry_price": short_result.get("price"),
            "amount_usdt": long_result.get("amount_usdt") or amount_usdt,
            "leverage": leverage,
            "long_order_id": long_result.get("order_id"),
            "short_order_id": short_result.get("order_id"),
            "long_taker_fee_rate": long_result.get("taker_fee_rate"),
            "short_taker_fee_rate": short_result.get("taker_fee_rate"),
            "provisional": True,  # снимается в _finalize_open полной записью
        })
    except Exception as exc:
        # Сама запись упасть почти не может (локальный JSON), но если
        # упала — кричим громко: это ровно тот случай, ради которого всё.
        print(f"[position-store] {coin.upper()}: НЕ УДАЛОСЬ записать предварительную позицию: {type(exc).__name__}: {exc}")


def _coin_min_spread_overrides() -> dict:
    """SCANNER_COIN_MIN_SPREAD_OVERRIDES — индивидуальный порог входа для
    отдельных монет, формат «МОНЕТА:процент,МОНЕТА:процент». Добавлено
    2026-09-15 по прямой просьбе пользователя после BONER ("добавим бонер в
    исключение, но можно зайти, если спред будет более 5%"): монета не
    запрещена совсем, но общий порог для неё заменяется своим, более
    высоким — чтобы на неликвидном рынке компенсация за пересечение тонкого
    стакана (~1–1.5% на BONER/aster) заранее сидела в спреде входа.
    Читается из окружения при каждом вызове — правка .env + рестарт, без
    отдельного кэша (вызовов единицы на сделку)."""
    raw = os.getenv("SCANNER_COIN_MIN_SPREAD_OVERRIDES", "") or ""
    out = {}
    for part in raw.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        coin, _, value = part.partition(":")
        try:
            out[coin.strip().upper()] = float(value)
        except ValueError:
            continue
    return out


def _min_spread_for_coin(coin: str, default: float) -> float:
    """Порог входа для конкретной монеты: индивидуальный (см.
    _coin_min_spread_overrides) либо общий default."""
    return _coin_min_spread_overrides().get((coin or "").upper(), default)


def _timings_total_ms(timings: dict) -> float:
    """Сумма ТОЛЬКО числовых полей *_ms. Раньше здесь было sum(timings.values())
    — и 2026-09-15 это стоило реальных денег: в словарь замеров добавили
    строковое поле book_source ("поток/поток"), sum() упал на int + str
    УЖЕ ПОСЛЕ того, как обе ноги BONER исполнились на биржах. Исключение
    вылетело из открытия, позиция не попала в учёт, reconcile закрыл обе
    ноги как «неучтённые», сканер зашёл снова — и так три круга комиссий
    подряд. Замер задержки не имеет права влиять на судьбу сделки, а
    подсчёт суммы — знать, какие ещё поля кто-то положит в словарь."""
    return round(
        sum(v for k, v in timings.items() if k.endswith("_ms") and isinstance(v, (int, float)) and not isinstance(v, bool)),
        1,
    )


async def _verify_order_filled(
    exchange, order: dict, symbol: str, exchange_id: str, exchange_name: str = None
) -> dict:
    """Перепроверяет, что ордер РЕАЛЬНО исполнен биржей, а не просто создан
    без исключения. Два независимых реальных случая, оба проверены
    2026-09-06 на боевых demo-ордерах:

    1) Bitget: create_order() вернул order_id, но биржа тут же отменила
       ордер (риск-движок/лимит ликвидности) — ответ на создание вообще не
       содержит status/filled у Bitget синхронно, поэтому без перепроверки
       код принимал отмену за успех.
    2) Bybit: fetch_order() без params={"acknowledged": True} кидает
       ArgumentsRequired ("can only access an order if it is in last 500
       orders... Set params['acknowledged']=True to hide this warning") —
       без этого параметра каждая попытка перепроверки на Bybit тихо
       падала (проглатывалась try/except), верификация НИКОГДА не получала
       реальный статус и ложно объявляла успешный ордер неисполненным
       (проверено на реальном ордере: create_order прошёл, позиция
       реально открылась, но код доложил ошибку и не закрыл её — только
       ручная проверка выявила расхождение).

    Поэтому: несколько коротких попыток с паузой на случай, если сама
    биржа ещё не разнесла исполнение по внутренним таблицам (без этого
    единичная мгновенная проверка была бы слишком нетерпеливой) — но
    статус "canceled" не пересматриваем повторными попытками (отменённый
    ордер сам по себе не станет исполненным от ожидания)."""
    filled = order.get("filled") or 0
    status = order.get("status")
    if status == "closed" or filled > 0:
        return order

    verify_params = {"uta": False} if exchange_id == "bitget" else {}
    if exchange_id == "bybit":
        verify_params["acknowledged"] = True

    # СНАЧАЛА — ПРИВАТНЫЙ ПОТОК (добавлено 2026-09-14, см. private_stream.py).
    # Биржа сама пушит факт исполнения по WebSocket, обычно за 20-80мс,
    # тогда как лестница ниже в лучшем случае узнаёт об этом через 200мс
    # плюс round-trip REST-запроса. Для Bitget это особенно важно: она
    # НИКОГДА не возвращает статус синхронно в ответе на создание (см.
    # случай 1 в докстринге), то есть до сих пор платила полную цену
    # лестницы на КАЖДОМ ордере.
    #
    # ПОЧЕМУ ЭТО НЕ МОЖЕТ ЗАМЕДЛИТЬ ВХОД: если поток не поднят или
    # недоступен, wait_for_fill возвращает None МГНОВЕННО (проверка
    # orders_ready внутри), и мы идём прежним путём без единой лишней
    # миллисекунды. А если поток есть, но промолчал — потраченное время
    # ВЫЧИТАЕТСЯ из первой паузы лестницы (waited ниже), поэтому суммарно
    # ожидание не превышает прежнего. Ускорение не имеет права
    # оборачиваться замедлением на пути реальных денег.
    waited = 0.0
    if status != "canceled" and order.get("id"):
        stream_key = exchange_name or exchange_id
        started = time.monotonic()
        try:
            streamed = await private_stream.wait_for_fill(stream_key, order.get("id"))
        except Exception as exc:
            # Поток — вспомогательный путь. Любой его сбой не должен
            # мешать верификации: молча уходим на REST-лестницу.
            print(f"[private-stream] {stream_key}: сбой ожидания исполнения ({type(exc).__name__}: {exc}).")
            streamed = None
        waited = time.monotonic() - started
        if streamed is not None:
            # Печатаем ЯВНО, каким путём получено подтверждение. Без этого
            # по логу невозможно отличить "поток работает" от "поток тихо
            # отвалился, и мы незаметно вернулись к лестнице" — а разница
            # между ними больше секунды на каждой ноге. Тот же принцип, что
            # и в строке [timing]: замер обязан сам говорить, откуда он.
            print(
                f"[private-stream] {stream_key}: исполнение ордера {order.get('id')} "
                f"подтверждено ПОТОКОМ за {waited * 1000:.0f}мс (без REST-лестницы)."
            )
            return streamed

    # Быстрые паузы (0.2, 0.4, 0.6) сохранены по прямой просьбе
    # пользователя 2026-09-10 — после реального инцидента (LAB на MEXC,
    # -$2.57, см. историю) решили НЕ жертвовать скоростью исполнения, а
    # вместо этого защищаться независимой сверкой позиций (см.
    # scanner.py: _reconcile_positions_loop) — она ловит именно такие
    # "голые" ноги, оставшиеся от гонки состояний, независимо от того,
    # что именно стало причиной (эта функция, или что-то ещё).
    for delay in (0.2, 0.4, 0.6):
        if status == "canceled":
            break
        # Вычитаем время, уже потраченное на ожидание потока, ТОЛЬКО из
        # первой ступени (waited обнуляется сразу после) — дальше лестница
        # работает ровно как раньше.
        sleep_for = max(0.0, delay - waited)
        waited = 0.0
        if sleep_for > 0:
            await asyncio.sleep(sleep_for)
        try:
            order = await exchange.fetch_order(order.get("id"), symbol, params=verify_params)
        except Exception as exc:
            # НЕ проглатываем тихо — именно такая тишина маскировала
            # реальную проблему с Bybit (недостающий params.acknowledged)
            # и не давала её вовремя заметить.
            print(f"[{exchange_id}] Не удалось перепроверить статус ордера {order.get('id')}: {exc}")
            continue
        filled = order.get("filled") or 0
        status = order.get("status")
        if status == "closed" or filled > 0:
            break
    return order


# =============================================================================
# КЭШ СТАВОК TAKER-КОМИССИИ (добавлено 2026-09-13 по прямой просьбе
# пользователя: "сделать один запрос на комиссию, запомнить и обновлять
# каждые 30 минут, а не запрашивать постоянно, повышая время выхода ноги").
#
# Что было (см. старую _get_taker_fee_rate): на КАЖДУЮ сделку — новый клиент
# биржи без ключей + load_markets() (у gate это 6500+ рынков) ради ОДНОГО
# поля market["taker"]. Реальные замеры по [timing] на закрытиях MTL/ZCAT:
# 11 984мс, 12 219мс, 16 157мс — при том, что сами ордера исполнялись за
# ~1.4с. То есть 90% времени "закрытия" уходило на справочный запрос,
# который ещё и регулярно упирался в таймаут 15с и возвращал None (net_pnl
# в журнале оставался пустым). И это ПОСЛЕ исполнения ордеров — окно, в
# котором position_store уже устарел, и reconcile успевал дважды
# "подтвердить" пропавшую ногу (реальный случай ZCAT 17:51 UTC).
#
# И второе: market["taker"] — это ПУБЛИЧНЫЙ базовый тариф из справочника
# CCXT, а не то, что реально списывает биржа с нашего аккаунта. Сверка с
# fetch_my_trades по реальным сделкам 2026-09-13: mexc 0.080% (в CCXT 0.020%
# — в 4 раза меньше!), bybit 0.100% (0.060%), bitget 0.100% (0.060%); gate и
# binance совпали (0.050%). Из-за этого net_pnl в журнале был завышен, а
# решение "мы уже в плюсе" принималось по заниженным комиссиям — прямая
# причина части "gross плюс / net минус" за ночь.
#
# Три источника ставки, по убыванию доверия:
#   1) "fill"     — реальная комиссия из ответа биржи на ИСПОЛНЕННЫЙ ордер
#                   (order["fee"]["cost"] / номинал). Ground truth, ноль
#                   дополнительных запросов, обновляется каждой сделкой.
#   2) "override" — явная ставка из EXCHANGE_TAKER_FEE_OVERRIDES в .env
#                   (посеяно замеренными реальными значениями, см. выше) —
#                   действует, пока по бирже нет ни одного своего исполнения.
#   3) "static"   — market["taker"] из УЖЕ загруженных рынков персистентного
#                   клиента (мгновенно, без нового клиента и load_markets),
#                   кэшируется на FEE_RATE_CACHE_TTL_SECONDS (30 мин).
# =============================================================================
_FEE_RATE_CACHE: dict = {}
_LEVERAGE_CONFIGURED: set = set()  # (exchange_id, symbol, leverage, side) — см. _place_single_order  # exchange_id -> {"rate": float, "ts": monotonic, "source": str}

# MEXC требует явно указывать openType в КАЖДОМ вызове, где есть margin-
# режим: и в set_leverage, и в самом create_order. 2 = cross (по явной
# просьбе пользователя 2026-09-06 "вернуть кросс на всех биржах").
#
# Вынесено на уровень модуля 2026-09-14 после реального сбоя: при выносе
# логики плеча в ensure_leverage_configured переменная осталась локальной
# внутри нового метода, а create_order в _place_single_order продолжал на
# неё ссылаться — нога на MEXC падала с NameError, вторая нога при этом
# успевала открыться и её приходилось откатывать (поймано тестовой сделкой
# MTL, реальная стоимость ошибки — комиссии за открытие и откат bybit).
# Константа уровня модуля гарантирует, что режим плеча и режим ордера
# физически не могут разойтись.
MEXC_OPEN_TYPE_CROSS = 2  # 1 = isolated, 2 = cross

# =============================================================================
# КЭШ СВОБОДНОГО БАЛАНСА (добавлено 2026-09-14 по замеру задержки входа CAP).
# Проверка маржи перед ордером (см. balance-guard в _open_both_legs_async)
# запрашивала баланс по обеим ногам КАЖДЫЙ раз. Замер 2026-09-14: gate
# 2015мс, aster 1812мс, bitget 1078мс, bybit 985мс, mexc 844мс, binance
# 657мс — после того как стаканы стали переиспользоваться, именно это
# осталось главным тормозом входа (781мс в [timing] по CAP).
#
# Баланс меняется предсказуемо: от НАШИХ же сделок (тогда сбрасываем кэш
# явно, см. _invalidate_balance) и от фандинга (мелкие суммы раз в 8 часов).
# Поэтому короткий TTL + сброс после каждого ордера дают точность, которой
# с запасом хватает для гейта с его 10%-м буфером. Если кэш пуст/протух —
# запрашиваем как раньше.
# =============================================================================
_BALANCE_CACHE: dict = {}  # exchange_id -> {"free": float, "ts": monotonic}


def _balance_cache_ttl() -> float:
    try:
        return float(os.getenv("BALANCE_CACHE_TTL_SECONDS", "60"))
    except (TypeError, ValueError):
        return 60.0


def _invalidate_balance(exchange_name: str) -> None:
    """Сбросить кэш баланса биржи — вызывается после КАЖДОГО реально
    отправленного ордера (открытие/закрытие): маржа изменилась, старое
    значение больше не годится."""
    _BALANCE_CACHE.pop(EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower()), None)


def _fee_cache_ttl_seconds() -> float:
    try:
        return float(os.getenv("FEE_RATE_CACHE_TTL_SECONDS", "1800"))
    except (TypeError, ValueError):
        return 1800.0


def _fee_overrides() -> dict:
    """EXCHANGE_TAKER_FEE_OVERRIDES="mexc:0.0008,bybit:0.001" -> {"mexc": 0.0008, ...}
    (ключ — каноническое имя биржи как в SCANNER_EXCHANGES, приводится к
    CCXT-id через EXCHANGE_ALIASES). Читается каждый раз (дёшево), чтобы
    правка .env подхватывалась без рестарта."""
    result = {}
    raw = os.getenv("EXCHANGE_TAKER_FEE_OVERRIDES", "")
    for item in raw.split(","):
        item = item.strip()
        if not item or ":" not in item:
            continue
        name, _, value = item.partition(":")
        try:
            rate = float(value)
        except ValueError:
            continue
        ex_id = EXCHANGE_ALIASES.get(name.strip().lower(), name.strip().lower())
        result[ex_id] = rate
    return result


def _learn_fee_from_fill(exchange_id: str, order: dict) -> None:
    """Достаёт РЕАЛЬНУЮ ставку из исполненного ордера и запоминает её в
    кэше как источник "fill" (высший приоритет). Никогда не бросает
    исключений и ничего не запрашивает у биржи — только читает то, что
    уже пришло в ответе. Если комиссии в ответе нет (часть бирж не отдаёт
    её в create_order — но отдаёт в fetch_order, см. _resolve_fill_price,
    который для этого сливает fee из повторного запроса обратно в order),
    просто молча ничего не делает."""
    try:
        fee = order.get("fee") or {}
        fee_cost = fee.get("cost")
        if fee_cost is None:
            fees = order.get("fees") or []
            fee_cost = sum((f.get("cost") or 0) for f in fees) if fees else None
        notional = order.get("cost")
        if not notional:
            filled = order.get("filled") or order.get("amount")
            avg = order.get("average") or order.get("price")
            if filled and avg:
                notional = filled * avg
        if not fee_cost or not notional or notional <= 0:
            return
        rate = abs(fee_cost) / notional
        # Санити: taker-комиссия на фьючерсах — доли процента. Всё, что вне
        # (0; 1%], — скорее всего комиссия в другой валюте/единицах или
        # мусор в ответе; такое не учим.
        if not (0 < rate <= 0.01):
            return
        prev = _FEE_RATE_CACHE.get(exchange_id)
        if prev and prev.get("rate") and abs(rate - prev["rate"]) / prev["rate"] > 0.25:
            print(
                f"[fee] {exchange_id}: реальная ставка по исполнению {rate*100:.3f}% заметно "
                f"отличается от прежней {prev['rate']*100:.3f}% ({prev.get('source')}) — обновляю."
            )
        _FEE_RATE_CACHE[exchange_id] = {"rate": rate, "ts": time.monotonic(), "source": "fill"}
    except Exception:
        pass  # обучение — вспомогательное, не должно мешать сделке


async def _resolve_fill_price(exchange, order: dict, symbol: str, exchange_id: str) -> Optional[float]:
    """Реальный повод (Binance, реальная сделка IOST 2026-09-09): рыночный
    ордер вернулся из create_order() СРАЗУ с status='closed' и полным
    filled — то есть _verify_order_filled() выше не перезапрашивает его
    вообще (у неё своя задача — убедиться, что ордер ИСПОЛНЕН, а не
    отменён, и она возвращается рано именно при filled>0/status=closed).
    Но order.get('average')/order.get('price') при этом были ПУСТЫ —
    Binance не всегда успевает посчитать среднюю цену синхронно с ответом
    на создание рыночного ордера. Старый код в этом случае падал на цену
    ТИКЕРА ДО отправки ордера (устаревшую, не реальную цену исполнения) —
    расхождение получилось ~2.5% ЦЕЛЫЙ размер спреда стратегии, отчёт
    показал +$0.25 прибыли вместо реального небольшого убытка.

    Уменьшено 2026-09-09 по прямой просьбе пользователя ("уменьши все
    задержки") — сначала ПЕРЕЗАПРАШИВАЕМ СРАЗУ, без паузы (часто этого
    уже достаточно — задержка не в "нужно подождать N секунд", а в том,
    что create_order() отвечает ДО того, как биржа успела посчитать
    среднюю), и только если это тоже не помогло — одна короткая пауза
    0.2с и ещё одна попытка. Возвращает None, если и это не помогло —
    вызывающий код падает на цену тикера как на самый крайний случай
    (лучше, чем ничего)."""
    price = order.get("average") or order.get("price")
    if price:
        return price
    order_id = order.get("id")
    if not order_id:
        return None
    verify_params = {"uta": False} if exchange_id == "bitget" else {}
    if exchange_id == "bybit":
        verify_params["acknowledged"] = True
    try:
        fresh = await exchange.fetch_order(order_id, symbol, params=verify_params)
        # Сливаем в исходный order всё полезное из повторного запроса — в
        # т.ч. fee/cost/filled, которых в ответе create_order часто нет, а
        # для _learn_fee_from_fill они нужны (см. кэш ставок выше).
        for key in ("fee", "fees", "cost", "filled", "average"):
            if fresh.get(key) is not None and order.get(key) is None:
                order[key] = fresh[key]
        price = fresh.get("average") or fresh.get("price")
        if price:
            return price
        await asyncio.sleep(0.2)
        fresh = await exchange.fetch_order(order_id, symbol, params=verify_params)
        for key in ("fee", "fees", "cost", "filled", "average"):
            if fresh.get(key) is not None and order.get(key) is None:
                order[key] = fresh[key]
        return fresh.get("average") or fresh.get("price")
    except Exception as exc:
        print(f"[fill-price] Не удалось перезапросить точную цену исполнения ордера {order_id} ({exchange_id}): {exc}")
        return None


def _bump_amount_to_minimum(market: dict, order_amount: float, price, contract_size: float) -> float:
    """Если посчитанное количество контрактов меньше минимального лота
    биржи для этого рынка — поднимает его до минимума, вместо того чтобы
    просто провалить ногу с ошибкой (по явной просьбе пользователя
    2026-09-06: "если не хватает количества, то просто повысь его до
    минимально проходимого"). Реальный объём сделки в USDT в этом случае
    получится НЕМНОГО БОЛЬШЕ исходного amount_usdt — это осознанный
    компромисс, озвученный пользователем, а не побочный эффект.

    Проверяет ДВА независимых лимита биржи (market['limits'], CCXT
    unified) — оба видели в реальных ошибках 2026-09-06:
    1) limits.amount.min — минимальное количество КОНТРАКТОВ (та самая
       "amount ... must be greater than minimum amount precision of X").
    2) limits.cost.min — минимальная НОМИНАЛЬНАЯ стоимость сделки в
       валюте котировки (amount * price * contractSize), отдельный лимит
       не у всех бирж, но там где есть — тоже реальная причина отказа."""
    limits = market.get("limits") or {}
    min_amount = (limits.get("amount") or {}).get("min")
    if min_amount is not None and order_amount < min_amount:
        order_amount = min_amount

    min_cost = (limits.get("cost") or {}).get("min")
    if min_cost is not None and price:
        notional = order_amount * price * contract_size
        if notional < min_cost:
            # +0.5% запас — компенсирует округление ВНИЗ до точности лота
            # биржи (precision.amount) при отправке ордера: этот код НЕ
            # вызывает exchange.amount_to_precision() перед create_order
            # (CCXT/сама биржа округляют сами при кодировании запроса), и
            # если точность грубая (реальный случай 2026-09-08: HEMI на
            # Hyperliquid, precision.amount=1.0 — только целые лоты), ровно
            # посчитанный минимум 1156.60 срезается до 1156, notional падает
            # с $10.00 до $9.996 — БИРЖА ОТКЛОНЯЕТ ордер как ниже минимума
            # (реальная ошибка: "Order must have minimum value of $10").
            # Небольшой запас гарантирует, что даже после округления вниз
            # результат останется НАД минимумом, а не точно на границе.
            order_amount = (min_cost / (price * contract_size)) * 1.005

    return order_amount


def _estimate_min_notional(market: dict, price, contract_size: float) -> "float | None":
    """Сколько РЕАЛЬНО будет стоить (в валюте котировки) минимально
    допустимый лот этого рынка. Та же арифметика, что и в
    _bump_amount_to_minimum выше, но БЕЗ побочных эффектов — только
    оценка, чтобы решить, стоит ли вообще заходить.

    Добавлено 2026-09-13 по прямой просьбе пользователя после реального
    случая ANTHROPIC: при цели $8 на ногу минимальный лот 0.01 шт при цене
    ~$2100 дал позицию на ~$21 — В 2.6 РАЗА больше задуманного, т.е. мы не
    контролировали собственный риск. Пользователь: "если мы не можем
    открыть на 5 маржи и 5 плечо что-то, то можем просто понизить ставку —
    вместо того чтобы повышать". Ниже минимального лота биржа физически не
    пустит, поэтому "понизить" на практике = не брать такую монету вовсе и
    оставить деньги на монеты, куда наш размер помещается нормально."""
    if not price:
        return None
    limits = market.get("limits") or {}
    min_amount = (limits.get("amount") or {}).get("min")
    min_cost = (limits.get("cost") or {}).get("min")

    candidates = []
    if min_amount is not None:
        candidates.append(min_amount * price * contract_size)
    if min_cost is not None:
        candidates.append(min_cost)
    if not candidates:
        return None
    return max(candidates)


# =============================================================================
# _parse_required_minimum_usdt — вытаскивает конкретную МИНИМАЛЬНУЮ сумму
# (в USDT), которую требует биржа, из текста её ошибки — используется
# амаунт-лестницей в _place_single_order (см. там), когда биржа отклоняет
# ордер именно как "СЛИШКОМ МАЛЕНЬКИЙ" (а не риск-движком/слиппеджем).
# Добавлено 2026-09-06 по явной просьбе пользователя после реального
# случая: MEXC (code 7008) отклонял $2.50 как "Cannot be less than the
# minimum order amount 5 USDT" — уменьшающаяся лестница сумм тут в
# принципе не поможет (наоборот, требуется БОЛЬШЕ, а не меньше), и без
# этой функции бот просто перебирал ещё более мелкие обречённые суммы.
# =============================================================================
def _parse_required_minimum_usdt(exc: Exception) -> Optional[float]:
    msg = str(exc)
    # MEXC отдаёт точное число структурно: "_extend":{"value":5} — самый
    # надёжный источник, если он есть в тексте ошибки.
    match = re.search(r'"value"\s*:\s*(\d+(?:\.\d+)?)', msg)
    if match:
        return float(match.group(1))
    # Общие текстовые формулировки разных бирж ("minimum order amount N",
    # "less than the minimum amount N" и т.п.) — тот же паттерн видели и у
    # bitget (code 45110: "less than the minimum amount 5 USDT").
    match = re.search(
        r"minimum (?:order )?amount(?: of)?\s*(\d+(?:\.\d+)?)", msg, re.IGNORECASE
    )
    if match:
        return float(match.group(1))
    return None


# Значение переменной окружения считается НЕ настоящим ключом (плейсхолдером
# из .env.example), если оно пустое или начинается с "your_" — так помечены
# все примеры-заглушки в .env.example (your_exchange_1_api_key и т.п.).
def _looks_like_placeholder(value: str) -> bool:
    return not value or value.strip().lower().startswith("your_")


# =============================================================================
# DEMO/PAPER TRADING — DEMO_TRADING=True в .env переключает бота на демо-
# (paper-)счета бирж: ключи те же по названию переменных (см.
# _get_exchange_credentials), но с суффиксом _DEMO_, и CCXT для
# ПОДДЕРЖИВАЕМЫХ бирж отправляет запросы на отдельный demo-эндпоинт биржи
# (не боевой) — ордера исполняются по-настоящему (реальная логика биржи,
# реальные цены/задержки), но виртуальными деньгами, без риска.
#
# ВАЖНО: CCXT реализует нормальный demo-режим встроенными средствами НЕ
# для всех бирж — это видно из исходников самого CCXT (features.sandbox=
# True/False в describe()). bybit/bitget/gate поддерживаются "из коробки"
# (см. enable_demo_trading()/set_sandbox_mode() в _build_exchange_client
# ниже). mexc CCXT официально НЕ поддерживает (features.sandbox=False), но
# у mexc есть отдельный домен фьючерсного тестнета (futures.testnet.mexc.com),
# зеркалящий структуру production API 1-в-1 — переключаем URL вручную (см.
# _build_exchange_client), поэтому mexc тоже в этом множестве. aster
# аналогично: CCXT тоже не умеет её testnet (features.sandbox=False), но у
# Aster есть отдельный домен тестнета (fapi.asterdex-testnet.com,
# подтверждено по офиц. документации asterdex/api-docs), зеркалящий
# структуру /fapi/v3/... 1-в-1 с боевым — переключаем URL вручную так же,
# как для mexc (см. _build_exchange_client).
DEMO_TRADING_SUPPORTED = {"bybit", "bitget", "gate", "mexc", "aster"}


def _is_demo_trading() -> bool:
    return os.getenv("DEMO_TRADING", "False").lower() == "true"


# ⚠️ По умолчанию, пока включён DEMO_TRADING, биржи ВНЕ DEMO_TRADING_SUPPORTED
# (mexc, aster) блокируются целиком (см. _is_exchange_configured) — иначе
# одна нога спреда могла бы уйти в demo (виртуальные деньги), а другая — на
# такую биржу с РЕАЛЬНЫМИ деньгами без реального хеджа. DEMO_ALLOW_LIVE_EXCHANGES
# в .env — явный, осознанный обход этой защиты для конкретных бирж (список
# через запятую, например "aster") — ставьте сюда биржу, только если вы
# понимаете и принимаете риск незахеджированной реальной позиции, когда
# вторая нога сделки окажется в demo.
def _demo_allowed_live_exchanges() -> set[str]:
    raw = os.getenv("DEMO_ALLOW_LIVE_EXCHANGES", "")
    return {name.strip().lower() for name in raw.split(",") if name.strip()}


# Кэш процесса: биржи (по имени), для которых уже подтверждён one_way_mode
# (см. _ensure_bitget_one_way_mode) — чтобы не дёргать лишний API-запрос
# перед КАЖДЫМ ордером, если это уже было сделано в текущем запуске бота.
_position_mode_synced: set[str] = set()

# Кэш процесса: УЖЕ ЗАГРУЖЕННЫЕ рынки CCXT (exchange.markets), по одному
# набору на биржу — переиспользуются между сделками (см. _get_ready_client).
# Без этого кэша каждый ОТДЕЛЬНЫЙ ордер заново тратил несколько секунд на
# load_markets() — и именно это было главной причиной заметного разрыва
# по времени между LONG- и SHORT-ногой одной сделки (проверено 2026-09-05:
# до ~11 секунд). По просьбе пользователя обе ноги должны уходить
# практически ОДНОВРЕМЕННО — этот кэш устраняет переменную задержку для
# всех сделок, кроме самой первой на каждой бирже за запуск.
#
# ВАЖНО: кэшируем только САМИ ДАННЫЕ о рынках (plain dict), а НЕ живой
# объект клиента CCXT целиком — тот несёт внутри aiohttp-сессию, привязанную
# к конкретному event loop. Каждый вызов _run()/open_spread() и т.п. идёт
# через asyncio.run(), который создаёт НОВЫЙ loop на каждый вызов — если бы
# кэшировался сам клиент, второй и все последующие вызовы падали бы с
# "Event loop is closed" при попытке переиспользовать сессию из уже
# закрытого loop прошлого вызова (проверено 2026-09-05 на реальном ордере
# MEXC). Кэш market-данных даёт тот же выигрыш в скорости (не нужен новый
# сетевой round-trip к load_markets()), но клиент при этом всегда свежий и
# привязан к ТЕКУЩЕМУ loop.
_warm_markets_cache: dict[str, dict] = {}

# =============================================================================
# ПЕРСИСТЕНТНЫЙ EVENT LOOP БОТА + ПЕРЕИСПОЛЬЗУЕМЫЕ КЛИЕНТЫ — добавлено
# 2026-09-08 по явной просьбе пользователя ("уменьшить задержку на
# мониторинг, проверку цены и вход") в ДОПОЛНЕНИЕ к кэшу market-данных
# выше. Тот кэш (комментарий над _warm_markets_cache) решал ТОЛЬКО
# load_markets() — сам объект клиента (а с ним и TCP/TLS-соединение с
# биржей) всё равно пересоздавался на КАЖДЫЙ ордер/проверку/закрытие,
# т.к. каждый вызов открытия/закрытия шёл через asyncio.run() — тот
# создаёт НОВЫЙ event loop на каждый вызов, а клиент CCXT (aiohttp-сессия
# внутри) привязан к loop, на котором был создан — использовать его с
# ДРУГОГО loop после закрытия исходного нельзя ("Event loop is closed").
#
# Решение: main.py:listen() один раз передаёт сюда СВОЙ (Telethon/scanner)
# ПОСТОЯННЫЙ event loop через set_bot_event_loop() — тот живёт всё время
# работы бота. Реальное открытие/закрытие сделки (см. _run_coro_blocking)
# теперь планирует свою корутину НА ЭТОТ loop через
# asyncio.run_coroutine_threadsafe() (тот же кросс-тредовый механизм, что
# notifier.py уже использует для отправки Telegram-сообщений из фонового
# потока) — вместо того чтобы поднимать одноразовый loop. Раз корутина
# реально выполняется на ОДНОМ и том же loop между вызовами, клиенты CCXT
# (а с ними и TCP/TLS-соединения) можно закэшировать и переиспользовать
# (_warm_trade_clients) — не поднимать заново на каждое действие.
#
# ВАЖНО (безопасность/откат): если set_bot_event_loop() не вызывался
# (например, demo-режим, отдельный тестовый скрипт, юнит-тест) —
# _bot_event_loop остаётся None, и ВСЁ поведение полностью откатывается к
# прежнему (asyncio.run() на каждый вызов, клиент свежий каждый раз,
# закрывается после использования) — новая логика активируется ТОЛЬКО
# внутри полного, живого бота. Дополнительно управляется флагом
# TRADE_USE_PERSISTENT_CONNECTIONS (по умолчанию True) — можно мгновенно
# откатить на старое поведение одной переменной в .env без отката кода.
# =============================================================================
_bot_event_loop = None  # type: "asyncio.AbstractEventLoop | None"
_warm_trade_clients: dict = {}  # exchange_id -> живой, готовый (авторизованный, с рынками) клиент CCXT


def set_bot_event_loop(loop) -> None:
    """Вызывается ОДИН раз из main.py:listen() сразу после создания
    Telegram-клиента — сообщает trade_tool.py, на каком event loop
    реально планировать исполнение сделок (см. комментарий выше)."""
    global _bot_event_loop
    _bot_event_loop = loop


def _persistent_connections_enabled() -> bool:
    return os.getenv("TRADE_USE_PERSISTENT_CONNECTIONS", "True").lower() == "true"


def _run_coro_blocking(coro):
    """Выполняет корутину и блокирует вызывающий (синхронный, вызывается
    из отдельного потока — см. run_in_executor в scanner.py/main.py) код
    до результата. См. комментарий у _bot_event_loop выше: если
    персистентный loop настроен и включён — планирует корутину НА НЕГО
    (run_coroutine_threadsafe), иначе — прежнее поведение (asyncio.run(),
    свой одноразовый loop на каждый вызов)."""
    if _bot_event_loop is not None and _persistent_connections_enabled() and _bot_event_loop.is_running():
        future = asyncio.run_coroutine_threadsafe(coro, _bot_event_loop)
        return future.result()
    return asyncio.run(coro)


class UnsupportedExchangeError(Exception):
    """Биржа названа в сигнале, но её нет в CCXT (например, OURBIT)."""


class ExchangeNotConfiguredError(Exception):
    """Биржа поддерживается CCXT, но для неё не заданы API-ключи в .env."""


# =============================================================================
# СХЕМА ВХОДНЫХ АРГУМЕНТОВ ИНСТРУМЕНТА
# =============================================================================
# CrewAI использует Pydantic-модель, чтобы LLM понимала, какие именно
# аргументы и в каком формате нужно передать при вызове инструмента.
class TradeToolInput(BaseModel):
    """Схема аргументов, которые LLM должна передать в trade_tool."""

    # Тикер монеты, например "BTC". Указывается без "/USDT".
    coin: str = Field(..., description="Тикер монеты, например 'BTC' или 'ETH'")

    # Название биржи (как в CCXT, например 'binance'), где открываем LONG.
    long_exchange: str = Field(
        ..., description="Название биржи для открытия LONG-позиции (например 'binance')"
    )

    # Название биржи (как в CCXT, например 'bybit'), где открываем SHORT.
    short_exchange: str = Field(
        ..., description="Название биржи для открытия SHORT-позиции (например 'bybit')"
    )


# =============================================================================
# КЛАСС ИНСТРУМЕНТА TradeExecutionTool
# =============================================================================
class TradeExecutionTool(BaseTool):
    # name и description — LLM видит их в промпте, чтобы понять, ЗАЧЕМ нужен
    # этот инструмент и когда его вызывать.
    name: str = "trade_tool"
    description: str = (
        "Одновременно открывает LONG-позицию на одной фьючерсной бирже и "
        "SHORT-позицию на другой бирже по заданной монете, используя "
        "асинхронное исполнение CCXT для минимизации задержки между ногами "
        "арбитражного спреда. Принимает: coin, long_exchange, short_exchange."
    )
    # args_schema связывает инструмент со схемой Pydantic выше — CrewAI
    # автоматически валидирует аргументы перед вызовом _run().
    args_schema: Type[BaseModel] = TradeToolInput

    # -------------------------------------------------------------------
    # _run — точка входа, которую вызывает CrewAI, когда LLM решает
    # использовать инструмент. CrewAI ожидает СИНХРОННЫЙ метод, поэтому
    # внутри мы запускаем асинхронную логику через asyncio.run().
    # -------------------------------------------------------------------
    def _run(self, coin: str, long_exchange: str, short_exchange: str) -> str:
        # См. _run_coro_blocking выше — на персистентном loop бота (если
        # настроен) переиспользует живые соединения с биржами, иначе
        # (как раньше) asyncio.run() создаёт новый одноразовый loop.
        return _run_coro_blocking(
            self._execute_spread_async(coin, long_exchange, short_exchange)
        )

    # -------------------------------------------------------------------
    # _build_exchange_client — вспомогательная функция: создаёт объект
    # клиента CCXT для указанной биржи, подставляя API-ключи из .env.
    # -------------------------------------------------------------------
    def _build_exchange_client(self, exchange_name: str, ccxt_module=None):
        """ccxt_module (добавлен 2026-09-14) — какой вариант CCXT
        использовать для КЛАССА биржи. По умолчанию ccxt.async_support
        (обычный REST-клиент бота). private_stream.py передаёт сюда
        ccxt.pro, чтобы получить WebSocket-клиента, настроенного АБСОЛЮТНО
        так же: те же ключи, та же passphrase, тот же обход подписи для
        Aster (разные кошельки user/signer), те же demo-домены. Это
        принципиально: приватный поток авторизуется теми же учётными
        данными, что и боевые ордера — если бы конфиг собирался отдельно,
        любое расхождение всплыло бы как «поток молчит» в самый неудобный
        момент. Классы ccxt.pro наследуют ccxt.async_support, поэтому вся
        логика ниже применима к ним без изменений."""
        module = ccxt_module if ccxt_module is not None else ccxt_async
        # Приводим имя к ID класса CCXT: сначала смотрим алиасы (например,
        # "hliquid" -> "hyperliquid"), иначе просто берём имя в нижнем
        # регистре как есть (для большинства бирж оно и есть ID CCXT).
        exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())

        # Не все биржи из канала сигналов есть в CCXT (например, OURBIT).
        # Явно проверяем поддержку и кидаем понятную ошибку вместо
        # AttributeError где-то в недрах getattr().
        if not hasattr(module, exchange_id):
            raise UnsupportedExchangeError(
                f"Биржа '{exchange_name}' не поддерживается библиотекой CCXT"
            )

        # getattr(module, "bybit") эквивалентно module.bybit —
        # так мы динамически получаем класс биржи по её строковому имени.
        exchange_class = getattr(module, exchange_id)

        api_key, api_secret = self._get_exchange_credentials(exchange_name)
        if _looks_like_placeholder(api_key) or _looks_like_placeholder(api_secret):
            raise ExchangeNotConfiguredError(
                f"Для биржи '{exchange_name}' не заданы API-ключи в .env "
                f"(ожидались {exchange_name.upper()}_API_KEY / "
                f"{exchange_name.upper()}_API_SECRET)"
            )

        # defaultType: у CCXT это НЕ везде "future" — MEXC, ASTER, BINGX и
        # Hyperliquid поддерживают только "swap" (перпетуал-контракты под
        # другим именем в CCXT); "future" на них падает с ошибкой. "swap"
        # поддерживают вообще все биржи канала, поэтому используем его
        # универсально, а не "future" (как было раньше — рабочим это было
        # только для Bybit/Bitget/Gate, случайно).
        config = {
            "enableRateLimit": True,  # Автоматически соблюдать лимиты биржи
            "options": {"defaultType": "swap"},
            # БЕЗ явного timeout один зависший (не оборвавшийся, просто
            # "молчащий") сетевой запрос к бирже блокирует поток
            # run_in_executor'а НАВСЕГДА (проверено 2026-09-05: бот
            # "жил" по мнению ОС, но не делал ничего много минут — виноват
            # именно такой зависший вызов, а не сам сканер). У ccxt по
            # умолчанию timeout есть, но не для всех методов/бирж
            # одинаково надёжно применяется — фиксируем явно.
            "timeout": 15000,  # 15 секунд на любой сетевой запрос
        }

        if exchange_id in ("aster", "hyperliquid"):
            # Aster и Hyperliquid — DEX-биржи, авторизуются НЕ через
            # apiKey/secret, а через приватный ключ EVM-кошелька
            # ('privateKey' в CCXT). Договорённость для этого проекта:
            # <БИРЖА>_API_KEY хранит адрес кошелька ("walletAddress"),
            # <БИРЖА>_API_SECRET — приватный ключ, которым подписываются
            # запросы. Для Aster это МОЖЕТ быть отдельный делегированный
            # "signer"-кошелёк, отличный от основного аккаунта с балансом
            # (см. обход ниже, специфичный именно для Aster) — для
            # Hyperliquid такого разделения нет, CCXT сам корректно выводит
            # адрес из приватного ключа (requiredCredentials: apiKey=False,
            # secret=False, walletAddress=True, privateKey=True — см.
            # ccxt/async_support/hyperliquid.py).
            config["walletAddress"] = api_key
            config["privateKey"] = api_secret
        else:
            config["apiKey"] = api_key
            config["secret"] = api_secret

            # Некоторым биржам (например, Bitget) кроме key/secret нужен
            # ещё "passphrase" (задаётся при создании ключа в личном
            # кабинете) — CCXT ожидает его в поле "password". Если
            # <БИРЖА>_PASSPHRASE не задан — просто не передаём это поле.
            # При DEMO_TRADING сначала пробуем _DEMO_PASSPHRASE, с
            # fallback на обычный (демо-аккаунт Bitget обычно использует
            # ту же passphrase, что и основной, если отдельная не задана).
            passphrase = ""
            if _is_demo_trading():
                passphrase = os.getenv(f"{exchange_name.upper()}_DEMO_PASSPHRASE", "")
            if not passphrase:
                passphrase = os.getenv(f"{exchange_name.upper()}_PASSPHRASE", "")
            if passphrase:
                config["password"] = passphrase

        exchange = exchange_class(config)

        if exchange_id == "aster":
            # ОБХОД (реализован 2026-09-06 по просьбе пользователя, когда
            # выяснилось, что основной адрес аккаунта Aster и адрес,
            # соответствующий приватному ключу signer'а — РАЗНЫЕ кошельки):
            # aster.sign() в CCXT по умолчанию вычисляет оба поля запроса,
            # "user" И "signer", из ОДНОГО и того же переданного privateKey
            # (см. self.eth_get_address_from_private_key в исходнике
            # ccxt/async_support/aster.py) — если это разные кошельки,
            # запрос уходит с user=signer-адрес, и биржа отвечает
            # "No aster user found" (основной аккаунт с таким адресом не
            # существует). Aster API поддерживает именно такое разделение
            # (user=основной кошелёк, signer=делегированный API-кошелёк),
            # просто CCXT не даёт высокоуровневого параметра для этого —
            # подменяем внутренние opts вручную, как задумано самим sign():
            # если options['cachedWalletAddress'] уже задан и совпадает по
            # хэшу приватного ключа — sign() использует его как "user"
            # НАПРЯМУЮ, а не пересчитывает из privateKey (см. код sign()).
            privkey_hash = exchange.hash(exchange.encode(exchange.privateKey), "keccak", "hex")
            exchange.options["cachedWalletAddress"] = api_key
            exchange.options["privateKeyHashForCachedWalletAddress"] = privkey_hash
            exchange.options["signerAddress"] = exchange.eth_get_address_from_private_key(exchange.privateKey)

        if _is_demo_trading():
            if exchange_id in DEMO_TRADING_SUPPORTED:
                # ВАЖНО: НЕЛЬЗЯ выбирать метод через hasattr(exchange,
                # "enable_demo_trading") — этот метод определён в БАЗОВОМ
                # классе ccxt.Exchange для ВСЕХ бирж без исключения (в том
                # числе gate), поэтому hasattr всегда True. Но у gate он НЕ
                # переопределён и делает self.urls['api'] = self.urls['demo'] —
                # а такого ключа у gate попросту нет -> KeyError: 'demo'
                # (проверено на реальном ордере 2026-09-05). Поэтому здесь —
                # явная развилка по конкретной бирже, а не автоопределение:
                #   bybit/bitget — используют СОБСТВЕННЫЙ enable_demo_trading()
                #     (bybit переключает на отдельный домен api-demo.*, у
                #     bitget это просто алиас set_sandbox_mode с PAPTRADING
                #     заголовком) — это НАСТОЯЩИЙ demo-режим ("Demo Trading"
                #     в личном кабинете), а НЕ testnet.
                #   gate — свой enable_demo_trading не переопределён, зато
                #     set_sandbox_mode() у gate реализован правильно (см.
                #     'test' в urls, переключает на sandbox-домен) — именно
                #     его и используем напрямую.
                if exchange_id in ("bybit", "bitget"):
                    exchange.enable_demo_trading(True)
                elif exchange_id == "gate":
                    exchange.set_sandbox_mode(True)
                elif exchange_id == "mexc":
                    # MEXC не имеет ни enable_demo_trading, ни рабочего
                    # set_sandbox_mode в CCXT (features.sandbox=False) — НО
                    # у MEXC есть отдельный домен для фьючерсного тестнета,
                    # который зеркалит структуру production API 1-в-1
                    # (проверено вручную 2026-09-05: те же пути
                    # /api/v1/contract/..., тот же формат ответов; реальный
                    # виртуальный баланс $44500 подтверждён через этот
                    # домен теми же MEXC_API_KEY/SECRET, что на проде
                    # показывали $0 — значит, эти ключи и есть testnet-
                    # аккаунт, просто CCXT стучится не туда). CCXT не даёт
                    # готового метода для такой замены — переключаем URL
                    # контрактного (фьючерсного) API вручную.
                    exchange.urls["api"]["contract"] = {
                        "public": "https://futures.testnet.mexc.com/api/v1/contract",
                        "private": "https://futures.testnet.mexc.com/api/v1/private",
                    }
                elif exchange_id == "aster":
                    # Aster тоже не имеет sandbox в CCXT (features.sandbox=
                    # False) — но у неё есть отдельный домен тестнета для
                    # фьючерсов, подтверждённый офиц. документацией
                    # (asterdex/api-docs, aster-finance-futures-api-testnet.md):
                    # "The base endpoint is: https://fapi.asterdex-testnet.com",
                    # зеркалящий /fapi/v3/... 1-в-1 с боевым (fapi.asterdex.com).
                    # ASTER_DEMO_API_KEY/_SECRET (адрес/приватный ключ ОТДЕЛЬНОГО
                    # тестнет-кошелька) читаются автоматически через
                    # _get_exchange_credentials, т.к. aster теперь в
                    # DEMO_TRADING_SUPPORTED. sapi (спот) не трогаем — этот
                    # бот торгует только фьючерсами (fapi*).
                    exchange.urls["api"]["fapiPublic"] = "https://fapi.asterdex-testnet.com/fapi"
                    exchange.urls["api"]["fapiPrivate"] = "https://fapi.asterdex-testnet.com/fapi"
            elif exchange_id in _demo_allowed_live_exchanges():
                # Пользователь ЯВНО разрешил этой бирже торговать реальными
                # деньгами даже во время DEMO_TRADING (DEMO_ALLOW_LIVE_EXCHANGES) —
                # используются обычные боевые ключи (см. _get_exchange_credentials),
                # предупреждаем в консоли, чтобы это не терялось в логах.
                print(f"[demo] {exchange_name}: торгует РЕАЛЬНЫМИ деньгами (явно разрешено).")
            else:
                # Биржа вне DEMO_TRADING_SUPPORTED (нет ни sandbox, ни
                # отдельного тестнет-домена) и пользователь её не разрешал —
                # сюда фактически не дойдём (см. _is_exchange_configured: pre-check отменит
                # сделку раньше), но оставляем предупреждение на случай
                # прямого вызова в обход pre-check.
                print(
                    f"[demo] {exchange_name}: CCXT не поддерживает demo-режим "
                    f"для этой биржи — запрос уйдёт на боевой эндпоинт."
                )

        return exchange

    # -------------------------------------------------------------------
    # ПРИВАТНЫЕ ПОТОКИ (см. private_stream.py) — поднимаются лениво, из
    # уже работающего цикла бота. Клиент строится ТЕМ ЖЕ методом, что и
    # боевой REST-клиент, только на классе ccxt.pro: одни и те же ключи,
    # passphrase, обход подписи Aster и demo-домены. Расхождение конфигов
    # здесь означало бы «поток молча не авторизовался» — ровно тот сорт
    # поломки, который заметен не сразу и дорого обходится.
    # -------------------------------------------------------------------
    async def ensure_private_streams(self, exchange_names) -> None:
        import ccxt.pro as ccxt_pro_module

        for exchange_name in exchange_names:
            await private_stream.subscribe(
                exchange_name,
                lambda n=exchange_name: self._build_exchange_client(n, ccxt_module=ccxt_pro_module),
            )

    async def _get_ready_client(self, exchange_name: str):
        """Возвращает клиент CCXT с уже заполненными рынками.

        ДВА РЕЖИМА (см. комментарий у _bot_event_loop выше):
        1) На персистентном loop бота (обычный случай на живом боте) —
           возвращает ГОРЯЧИЙ, уже открытый клиент из _warm_trade_clients
           (создаётся один раз на биржу, дальше переиспользуется — TCP/TLS-
           соединение не поднимается заново на каждую сделку). Вызывающий
           код НЕ должен закрывать такой клиент — проверяйте
           exchange._is_persistent_trade_client перед close() (см.
           _place_single_order/_close_single_order/_get_book_snapshot).
        2) Иначе (demo/тестовый скрипт без полного бота, либо
           TRADE_USE_PERSISTENT_CONNECTIONS=False) — прежнее поведение:
           СВЕЖИЙ клиент на каждый вызов, рынки заполняются мгновенно из
           _warm_markets_cache (без сетевого round-trip), вызывающий код
           ОБЯЗАН его закрыть после использования."""
        key = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())

        use_persistent = False
        try:
            use_persistent = (
                _bot_event_loop is not None
                and _persistent_connections_enabled()
                and asyncio.get_running_loop() is _bot_event_loop
            )
        except RuntimeError:
            pass  # нет запущенного loop (не должно случаться внутри async-функции, но на всякий случай)

        if use_persistent and key in _warm_trade_clients:
            return _warm_trade_clients[key]

        exchange = self._build_exchange_client(exchange_name)
        cached_markets = _warm_markets_cache.get(key)
        if cached_markets is not None:
            exchange.set_markets(cached_markets)
        else:
            await exchange.load_markets()
            _warm_markets_cache[key] = exchange.markets

        if use_persistent:
            exchange._is_persistent_trade_client = True
            _warm_trade_clients[key] = exchange
        return exchange

    @staticmethod
    async def _ensure_bitget_one_way_mode(exchange, exchange_name: str, symbol: str) -> None:
        # Bitget-специфичный фикс ошибки 40774 "The order type for
        # unilateral position must also be the unilateral position type."
        # Весь остальной код бота (buy=long, sell=short, reduceOnly=закрыть,
        # без posSide/tradeSide) реализован под one_way_mode ("unilateral",
        # как сам Bitget называет его в тексте ошибки) — а НОВЫЕ аккаунты
        # Bitget по умолчанию создаются в hedge_mode (двусторонние позиции),
        # из-за чего наш "однобокий" запрос ордера конфликтует с настройкой
        # аккаунта и биржа отклоняет ордер целиком. Явно переключаем
        # аккаунт в one_way_mode ОДИН РАЗ за запуск процесса (см.
        # _position_mode_synced) перед первым же ордером на этой бирже —
        # идемпотентно, если уже стоит нужный режим, Bitget просто вернёт
        # успех повторно.
        #
        # params={'uta': False} — КРИТИЧНО для аккаунтов, у которых Bitget
        # включил Unified Trading Account (UTA, проверено 2026-09-05 на
        # demo-ключах): CCXT автоматически детектит uta=True и шлёт
        # set_position_mode на UTA-эндпоинт, который отвечает "success", но
        # реально НЕ переключает режим classic mix-аккаунта (тот, где лежит
        # весь наш баланс/плечо/ордера) — из-за этого рассинхрон остаётся
        # (аккаунт продолжает быть в hedge_mode), и ЛЮБОЙ ордер после этого
        # падает с "25203 Insufficient margin" (реальный баланс есть, но
        # margin считается для неправильного/несовпадающего режима позиции).
        # Форсируем classic-эндпоинт явно, вместо того чтобы полагаться на
        # автодетект CCXT — подтверждено живым тестовым ордером.
        cache_key = exchange_name.lower()
        if cache_key in _position_mode_synced:
            return
        try:
            await exchange.set_position_mode(False, symbol, {"uta": False})
            _position_mode_synced.add(cache_key)
        except Exception as exc:
            # Не получилось переключить режим (например, уже есть открытые
            # хедж-позиции на аккаунте) — не роняем размещение ордера из-за
            # этого, он просто провалится тем же понятным кодом 40774, что
            # и раньше, если режимы действительно несовместимы.
            print(f"[bitget] Не удалось выставить one_way_mode: {exc}")

    @staticmethod
    async def _set_leverage_with_fallback(exchange, leverage: int, symbol: str, leverage_params: dict) -> int:
        """Выставляет плечо с автоматическим откатом на МЕНЬШЕЕ, если
        запрошенное биржа не разрешает для этого символа прямо сейчас —
        по явной просьбе пользователя 2026-09-06: минимальное плечо везде
        TRADE_LEVERAGE (по умолчанию 10x), но если биржа его не даёт —
        нога должна всё равно открыться на том плече, что реально разрешено,
        а не откатываться целиком (как было раньше с DOT на Bitget:
        "The current trading pair is in a low-liquidity period. The
        maximum available leverage is 3x" — при плече 10 такая связка
        КАЖДЫЙ раз проваливалась и требовала rollback второй ноги, хотя
        сама сделка вполне могла открыться на разрешённых 3x).

        Стратегия:
        1) Пробуем запрошенное плечо как есть.
        2) Bybit код 110043 "leverage not modified" — не настоящая ошибка
           (плечо уже стоит нужное), просто считаем успехом.
        3) Если биржа в тексте ошибки прямо называет максимум ("maximum
           available leverage is Nx" и похожие формулировки) — пробуем
           ИМЕННО это N.
        4) Иначе перебираем убывающую лестницу разумных значений
           (в т.ч. само переданное leverage, если оно не было первым),
           пока одно не сработает.
        Возвращает РЕАЛЬНО применённое плечо (может быть меньше
        запрошенного) — вызывающий код это не использует напрямую, но
        значение доступно на будущее (например, для отчёта)."""
        try:
            await exchange.set_leverage(leverage, symbol, leverage_params)
            return leverage
        except ccxt_async.ExchangeError as exc:
            # ВАЖНО: ловим базовый ExchangeError, а не только BadRequest —
            # проверено 2026-09-06: Bitget на "low-liquidity period, max
            # leverage 3x" кидает именно ExchangeError (код 40940), а не
            # BadRequest, и первая версия этого фикса с except BadRequest
            # его пропускала мимо, оставляя старое поведение (провал ноги).
            msg = str(exc)
            if "110043" in msg or "leverage not modified" in msg.lower():
                return leverage

            # Пытаемся вытащить конкретное разрешённое значение из текста
            # ошибки биржи — так откат попадает в цель с первой попытки.
            match = re.search(r"(?:maximum available leverage is|max(?:imum)? leverage(?: is)?)\s*(\d+(?:\.\d+)?)x?", msg, re.IGNORECASE)
            candidates = []
            if match:
                candidates.append(float(match.group(1)))
            for fallback in (10, 5, 3, 2, 1):
                if fallback != leverage and fallback not in candidates:
                    candidates.append(fallback)

            last_exc = exc
            for candidate in candidates:
                try:
                    await exchange.set_leverage(candidate, symbol, leverage_params)
                    print(f"[leverage] {symbol}: запрошенное {leverage}x недоступно ({msg}) — применено {candidate}x.")
                    return candidate
                except ccxt_async.ExchangeError as retry_exc:
                    last_exc = retry_exc
                    continue
            raise last_exc

    @staticmethod
    def _get_exchange_credentials(exchange_name: str) -> tuple[str, str]:
        """Читает API-ключ/секрет биржи из переменных окружения по её
        имени: для 'bybit' это BYBIT_API_KEY / BYBIT_API_SECRET, для
        'mexc' — MEXC_API_KEY / MEXC_API_SECRET и т.д. Так поддерживается
        любое количество бирж (сигналы канала используют больше двух),
        а не только жёстко заданная пара EXCHANGE_1/EXCHANGE_2, как было
        раньше (та схема к тому же содержала баг: любая третья биржа
        молча получала ключи от EXCHANGE_2).

        При DEMO_TRADING=True сначала ищет <БИРЖА>_DEMO_API_KEY/_SECRET
        (отдельные ключи демо-счёта) — НО только для бирж из
        DEMO_TRADING_SUPPORTED (bybit/bitget/gate — настоящий sandbox;
        mexc/aster — свой домен тестнета, см. _build_exchange_client). Для
        всех ОСТАЛЬНЫХ бирж (нет ни sandbox, ни отдельного тестнет-домена)
        DEMO_TRADING=True на них не влияет вообще — подставляются только
        боевые ключи, поэтому переключение demo/live не требует
        переписывать .env."""
        prefix = exchange_name.upper()
        exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
        if _is_demo_trading() and exchange_id in DEMO_TRADING_SUPPORTED:
            demo_key = os.getenv(f"{prefix}_DEMO_API_KEY", "")
            demo_secret = os.getenv(f"{prefix}_DEMO_API_SECRET", "")
            if not _looks_like_placeholder(demo_key) and not _looks_like_placeholder(demo_secret):
                return demo_key, demo_secret
        api_key = os.getenv(f"{prefix}_API_KEY", "")
        api_secret = os.getenv(f"{prefix}_API_SECRET", "")
        return api_key, api_secret

    # -------------------------------------------------------------------
    # _place_single_order — открывает ОДИН рыночный ордер на одной бирже.
    # Вынесено в отдельную корутину, чтобы обе ноги сделки (long/short)
    # можно было запустить параллельно через asyncio.gather().
    # -------------------------------------------------------------------
    async def warm_leverage(self, exchange_name: str, coin: str, side: str, leverage: int) -> bool:
        """ФОНОВЫЙ прогрев: заранее выставить плечо для (биржа, монета,
        сторона), чтобы реальный вход по этой связке не платил 0.5-1.9с за
        set_leverage. Возвращает True, если что-то реально было выставлено
        (False — уже было в кэше или не получилось).

        Никогда не бросает исключений наружу: это оптимизация, а не
        торговая логика. Если биржа отказала — просто не прогрели, вход
        сделает это сам, как раньше."""
        try:
            exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
            symbol = _build_symbol(exchange_name, coin)
            if (exchange_id, symbol, leverage, side) in _LEVERAGE_CONFIGURED:
                return False
            exchange = await self._get_ready_client(exchange_name)
            if symbol not in exchange.markets:
                return False  # монеты нет на этой бирже — прогревать нечего
            await self.ensure_leverage_configured(exchange, exchange_name, symbol, side, leverage)
            return True
        except Exception as exc:
            print(f"[leverage-warm] {exchange_name}/{coin} {side}: не удалось ({type(exc).__name__}: {exc})")
            return False

    async def ensure_leverage_configured(
        self, exchange, exchange_name: str, symbol: str, side: str, leverage: int
    ) -> str:
        """Выставляет плечо и маржинальный режим для (биржа, символ, плечо,
        сторона), если это ещё не сделано в этом процессе. Возвращает
        exchange_id (он нужен вызывающему коду дальше).

        Вынесено из _place_single_order 2026-09-14, когда добавился ФОНОВЫЙ
        ПРОГРЕВ (scanner.py: _warm_leverage_for_candidates) — важно, чтобы
        прогрев и реальная сделка шли ОДНИМ И ТЕМ ЖЕ кодом и писали в ОДИН
        кэш: иначе прогрев мог бы выставить не то, что потом ждёт ордер, и
        мы бы этого не заметили.

        Зачем прогрев: замер 2026-09-14 показал, что set_leverage стоит
        500-1875мс (aster 1875, gate 1172, bitget 610, mexc 609, binance
        594, bybit 500), и на ПЕРВОЙ сделке по монете эту цену платил сам
        вход. Биржа хранит настройку за символом, поэтому её можно сделать
        заранее — тогда вход сразу идёт к create_order.

        Ключ кэша включает leverage (смена плеча в .env должна
        перенастроить символ) и side (у MEXC positionType зависит от
        стороны)."""
        exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
        key = (exchange_id, symbol, leverage, side)
        if key in _LEVERAGE_CONFIGURED:
            return exchange_id

        # MEXC — особый случай: её set_leverage() требует ЯВНО указать
        # openType (1=isolated/2=cross) и positionType (1=long/2=short)
        # параметрами, иначе кидает ArgumentsRequired (проверено на реальном
        # боевом ордере 2026-08-28 — без этого сделка на MEXC не
        # открывается вообще).
        leverage_params = {}
        # CROSS margin — по явной просьбе пользователя 2026-09-06 ("давай
        # вернём обратно на кросс на всех биржах"), после короткого
        # перехода на isolated тем же вечером.
        if exchange_id == "mexc":
            leverage_params = {
                "openType": MEXC_OPEN_TYPE_CROSS,
                "positionType": 1 if side == "long" else 2,
            }
        elif exchange_id == "bitget":
            # См. _ensure_bitget_one_way_mode — иначе ордер падает с 40774
            # (несовпадение hedge_mode/one_way_mode аккаунта).
            await self._ensure_bitget_one_way_mode(exchange, exchange_name, symbol)
            # CROSS margin (см. MEXC_OPEN_TYPE_CROSS выше). У Bitget
            # margin mode не связан с leverage/position mode — отдельный
            # явный вызов; идемпотентно (если уже crossed, вернёт успех).
            try:
                await exchange.set_margin_mode("cross", symbol, params={"uta": False})
            except ccxt_async.ExchangeError as exc:
                print(f"[margin] {symbol}: не удалось явно выставить cross margin ({exc}) — продолжаю с текущим режимом счёта.")
            # Тот же UTA — форсируем classic-эндпоинт и для set_leverage,
            # для консистентности со всеми остальными bitget-вызовами.
            leverage_params = {"uta": False}
        elif exchange_id == "gate":
            # У Gate margin mode задаётся ПРЯМО в вызове set_leverage (нет
            # отдельного setMarginMode — см. исходник ccxt gate.py:
            # set_leverage): без marginMode='cross' запрос уходит в
            # изолированном формате (request['leverage']=N) — именно так
            # было ДО просьбы пользователя 2026-09-06 вернуть кросс. С
            # marginMode='cross' CCXT сам переключает запрос в кросс-формат
            # (request['cross_leverage_limit']=N, request['leverage']='0').
            leverage_params = {"marginMode": "cross"}

        await self._set_leverage_with_fallback(exchange, leverage, symbol, leverage_params)
        _LEVERAGE_CONFIGURED.add(key)
        return exchange_id

    async def _place_single_order(
        self, exchange_name: str, coin: str, side: str, amount_usdt: float, leverage: int,
        known_price: Optional[float] = None,
    ) -> dict:
        # Символ строим ЗДЕСЬ (не принимаем готовым снаружи) — у каждой
        # ноги может быть своя биржа с СВОЕЙ валютой котировки (см.
        # _build_symbol/EXCHANGE_QUOTE_CURRENCY: Hyperliquid — USDC,
        # остальные — USDT). Общий символ на обе ноги был бы неверен, если
        # long и short — на разных по этому признаку биржах.
        symbol = _build_symbol(exchange_name, coin)

        # exchange создаём ВНУТРИ try (а не до него), т.к. сама сборка
        # клиента может упасть с UnsupportedExchangeError (биржа из
        # сигнала не поддерживается CCXT, например OURBIT) или
        # ExchangeNotConfiguredError (нет ключей в .env) — такой отказ
        # должен вернуться как обычная строка отчёта, а не уронить
        # asyncio.gather() и вторую ногу сделки вместе с ней.
        exchange = None
        try:
            # DRY_RUN=True — режим "сухого прогона": ордер НЕ отправляется
            # на биржу, а только эмулируется. Это безопасный режим по
            # умолчанию для тестирования пайплайна без реальных денег —
            # поэтому в dry-run НЕ требуем настоящих API-ключей (проверяем
            # только, что биржа вообще поддерживается CCXT), иначе
            # тестирование было бы невозможно без реальных ключей.
            dry_run = os.getenv("DRY_RUN", "True").lower() == "true"

            if dry_run:
                exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
                if not hasattr(ccxt_async, exchange_id):
                    raise UnsupportedExchangeError(
                        f"Биржа '{exchange_name}' не поддерживается библиотекой CCXT"
                    )
                # Эмулируем небольшую сетевую задержку, как будто реально
                # сходили на биржу, чтобы замер задержки был реалистичным.
                await asyncio.sleep(0.05)
                return {
                    "exchange": exchange_name,
                    "side": side,
                    "status": "DRY_RUN_OK",
                    "order_id": "dry-run-simulated",
                    "price": None,
                }

            # Боевой режим: получаем УЖЕ подключённый/прогретый клиент
            # CCXT (см. _get_ready_client) — это упадёт с
            # UnsupportedExchangeError, если биржи нет в CCXT, или с
            # ExchangeNotConfiguredError, если для неё не заданы ключи.
            exchange = await self._get_ready_client(exchange_name)
            if symbol not in exchange.markets:
                # У Gate (6500+ рынков, грузятся параллельно постранично
                # внутри самого CCXT) load_markets() иногда возвращает
                # НЕПОЛНЫЙ список — проверено 2026-09-05: один вызов вернул
                # 6033 рынка вместо полных 6579, без символа, который
                # сканер только что видел в своих же данных. Это не
                # настоящее отсутствие рынка, а разовая недогрузка —
                # форсируем повторную загрузку один раз, прежде чем сдаться.
                await exchange.load_markets(True)
                # Обновляем кэш посвежее загруженными рынками — иначе
                # следующий вызов снова получит из кэша неполный список.
                _warm_markets_cache[EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())] = exchange.markets

            # КЭШ «ПЛЕЧО УЖЕ ВЫСТАВЛЕНО» (добавлено 2026-09-13 по разбору
            # [timing] ANTHROPIC: фаза ордеров 2515мс при 360мс на стакан).
            # set_leverage/set_margin_mode — это НАСТРОЙКИ СИМВОЛА, которые
            # биржа ХРАНИТ: выставили 5x cross для монеты один раз — оно там
            # и остаётся. Раньше эти 1-2 запроса уходили ПЕРЕД КАЖДЫМ ордером
            # — на gate (~500-950мс на запрос сегодня) это до трети всей
            # задержки ноги впустую. Теперь первая сделка по паре
            # (биржа, символ, плечо, сторона) настраивает и запоминает, все
            # следующие — сразу к create_order. Ключ включает leverage, чтобы
            # смена плеча в .env (3x -> 5x сегодня) гарантированно
            # перенастроила символ, и side — у MEXC positionType зависит от
            # стороны. Кэш живёт в памяти процесса: рестарт = перенастройка.
            # Плечо/маржинальный режим — единый метод для реальной сделки
            # и для фонового прогрева (см. ensure_leverage_configured).
            exchange_id = await self.ensure_leverage_configured(
                exchange, exchange_name, symbol, side, leverage
            )

            # ЦЕНА ДЛЯ РАСЧЁТА ОБЪЁМА — если вызывающий код (_open_both_legs_async,
            # после проверки направления) уже получил свежую цену буквально
            # мгновение назад, переиспользуем её вместо ПОВТОРНОГО сетевого
            # запроса (добавлено 2026-09-06 по явной просьбе пользователя:
            # "заходи по точному спреду... делай эти операции за очень
            # короткое время") — экономит один полный round-trip на КАЖДУЮ
            # ногу прямо перед отправкой ордера, а значит меньше времени
            # успевает пройти между обнаружением спреда и реальным входом.
            # Используется ТОЛЬКО для расчёта размера ордера в монете —
            # реальная цена исполнения всё равно берётся из ответа биржи
            # (fill_price ниже), так что небольшая неточность здесь не
            # искажает фактический результат сделки.
            if known_price is not None:
                price = known_price
            else:
                ticker = await exchange.fetch_ticker(symbol)
                price = _extract_ticker_price(ticker)
            if price is None:
                # Реальный случай (проверено 2026-09-05): у MEXC testnet
                # 'last' для BNB был None при живых bid/ask — без фолбэка
                # get() ниже упало бы "unsupported operand type(s) for /:
                # 'float' and 'NoneType'" уже ПОСЛЕ того, как ПЕРВАЯ нога
                # (на другой бирже) реально открылась — оставляя
                # незахеджированную позицию. Явная проверка на None здесь
                # превращает это в штатный ERROR-статус, а не в падение
                # посреди исполнения.
                raise ccxt_async.ExchangeError(f"не удалось определить цену {symbol} (last/bid/ask пустые)")

            market = exchange.market(symbol)
            contract_size = market.get("contractSize") or 1

            # create_order(symbol, type, side, amount) — универсальный метод
            # CCXT: type="market" значит рыночный ордер (по текущей цене),
            # side="buy" открывает LONG, side="sell" открывает SHORT.
            order_params = {}
            if exchange_id == "mexc":
                # Явно дублируем openType и здесь (не полагаемся на дефолт
                # CCXT внутри create_swap_order_request) — иначе есть риск
                # рассинхрона между margin-режимом, для которого выставлено
                # плечо (set_leverage выше), и margin-режимом самого ордера.
                order_params["openType"] = MEXC_OPEN_TYPE_CROSS
            elif exchange_id == "bitget":
                # См. _ensure_bitget_one_way_mode — тот же форс classic-
                # эндпоинта вместо UTA, иначе именно ЭТОТ вызов (а не только
                # set_position_mode/set_leverage) может уйти не на тот
                # аккаунт и снова словить "Insufficient margin".
                order_params["uta"] = False

            # ОДИН прямой ордер на запрошенную сумму — БЕЗ лестницы
            # уменьшения и БЕЗ каскада подъёма (убрано 2026-09-06 по явной
            # просьбе пользователя: "не пробуй делать каскадами цен, просто
            # запускай один ордер на 6 долларов, главное — минимальная
            # задержка между ногами"). Раньше здесь была лестница
            # уменьшающихся/увеличивающихся сумм для борьбы с отказами
            # биржи — она реально помогала успеху открытия, но каждая
            # дополнительная попытка добавляет реальное сетевое время
            # ПОСЛЕ того, как ВТОРАЯ нога (на другой бирже) уже исполнилась
            # или тоже перебирает свою лестницу — то есть напрямую увеличивает
            # разброс по времени между ногами, который для этого бота
            # приоритетнее шанса дожать проблемную сумму. Единственное, что
            # остаётся — _bump_amount_to_minimum: разовый (без сетевого
            # запроса) расчётный подъём, если сумма меньше минимального
            # лота монеты — он не добавляет задержки, просто иначе
            # выбранный объём.
            amount_in_coin = amount_usdt / price
            # ВАЖНО: параметр amount в create_order() для НЕКОТОРЫХ бирж
            # (проверено на Gate и MEXC 2026-09-05) — это НЕ количество
            # монеты, а количество КОНТРАКТОВ ("vol"/"size"), которое CCXT
            # передаёт как есть, БЕЗ деления на contractSize рынка (в
            # отличие от bybit/bitget/aster, где contractSize всегда 1, и
            # разница не проявляется). У Gate/MEXC contractSize реально
            # разный для разных монет (от 0.0001 до 10 000 000!) — без
            # этого деления реальный объём позиции отличался бы от
            # задуманного в десятки-сотни раз. Приводим количество монеты
            # к количеству контрактов явно, для ЛЮБОЙ биржи — там, где
            # contractSize=1, деление ничего не меняет, так что это
            # безопасно везде.
            order_amount = amount_in_coin / contract_size
            order_amount = _bump_amount_to_minimum(market, order_amount, price, contract_size)

            # Hyperliquid — единственная биржа канала, где "market"-ордер
            # реализован как IOC-лимитка с автоматическим slippage (сама
            # биржа/CCXT не знает, от какой цены его считать) — без
            # ЯВНОГО price в самом create_order() (не в params!) CCXT
            # кидает ArgumentsRequired ("market orders require price to
            # calculate the max slippage price"), реальный случай
            # 2026-09-08: HEMI SHORT на hyperliquid упал на этом ровно
            # после того, как LONG на gate уже исполнился (откат сработал,
            # но сделка не засчиталась). Передаём последнюю известную цену
            # (ту же, что уже используется для расчёта amount_in_coin выше)
            # — дефолтный slippage CCXT (5%) достаточен, реальное
            # исполнение всё равно верифицируется ниже (_verify_order_filled).
            order_kwargs = {}
            if exchange_id == "hyperliquid":
                order_kwargs["price"] = price

            order = await exchange.create_order(
                symbol=symbol,
                type="market",
                side="buy" if side == "long" else "sell",
                amount=order_amount,
                params=order_params,
                **order_kwargs,
            )

            # ВЕРИФИКАЦИЯ РЕАЛЬНОГО ИСПОЛНЕНИЯ — критично, не декоративно.
            # См. _verify_order_filled — без неё код считал сделку успешной
            # (status="OK", цена — из ТИКЕРА до отправки ордера, а не
            # реальная цена исполнения) даже когда ордер был тихо отменён
            # биржей, оставляя вторую ногу спреда РЕАЛЬНО открытой и
            # НЕЗАХЕДЖИРОВАННОЙ (rollback не срабатывал, т.к. эта нога
            # считалась успешной).
            order = await _verify_order_filled(exchange, order, symbol, exchange_id, exchange_name)
            filled = order.get("filled") or 0
            status = order.get("status")
            if status == "canceled" or filled <= 0:
                raise ccxt_async.ExchangeError(
                    f"ордер создан (id={order.get('id')}), но НЕ исполнен биржей "
                    f"(status={status}, filled={filled}) — вероятно, отменён "
                    f"риск-движком/лимитом ликвидности биржи"
                )

            fill_price = await _resolve_fill_price(exchange, order, symbol, exchange_id)
            _learn_fee_from_fill(exchange_id, order)  # реальная комиссия -> кэш ставок (см. _FEE_RATE_CACHE)
            _invalidate_balance(exchange_name)  # маржа изменилась — кэш баланса больше не годится
            if fill_price is None:
                fill_price = price  # тикер ДО ордера — крайний случай, как раньше
            # filled_amount_coin — РЕАЛЬНО исполненное количество монеты
            # (контракты * contractSize), а не наивная оценка
            # amount_usdt/price снаружи. Критично из-за _bump_amount_to_minimum:
            # реальная позиция может отличаться от исходных $amount_usdt —
            # без этого поля _open_both_legs_async посчитал бы объём для
            # закрытия неверно и оставил бы хвост позиции незакрытым.
            filled_contracts = order.get("filled") or order_amount
            return {
                "exchange": exchange_name,
                "side": side,
                "status": "OK",
                "order_id": order.get("id"),
                "price": fill_price,
                "filled_amount_coin": filled_contracts * contract_size,
                "actual_amount_usdt": amount_usdt,
            }
        except UnsupportedExchangeError as exc:
            # Биржа названа в сигнале, но её нет в CCXT (например, OURBIT).
            # Это штатный, ожидаемый отказ, а не программная ошибка —
            # возвращаем отдельный статус, чтобы агент честно отразил его
            # в отчёте, а не выдал за успех и не за "непонятную ошибку".
            return {
                "exchange": exchange_name,
                "side": side,
                "status": f"EXCHANGE_NOT_SUPPORTED: {exc}",
                "order_id": None,
                "price": None,
            }
        except ExchangeNotConfiguredError as exc:
            # Биржа поддерживается CCXT, но для неё не заданы ключи в .env.
            # Тоже штатный отказ (не программная ошибка) — только в
            # боевом режиме, т.к. в DRY_RUN ключи не требуются вовсе.
            return {
                "exchange": exchange_name,
                "side": side,
                "status": f"NO_CREDENTIALS_CONFIGURED: {exc}",
                "order_id": None,
                "price": None,
            }
        except Exception as exc:
            # Любую ошибку биржи (недостаточно средств, неверный символ и
            # т.д.) перехватываем и возвращаем как часть отчёта, а не роняем
            # весь процесс — вторая нога спреда должна успеть исполниться
            # независимо от результата первой.
            return {
                "exchange": exchange_name,
                "side": side,
                "status": f"ERROR: {exc}",
                "order_id": None,
                "price": None,
            }
        finally:
            # Персистентный клиент (см. _get_ready_client/_bot_event_loop) —
            # НЕ закрываем, он живёт между сделками. Иначе (свежий на каждый
            # вызов клиент) — закрываем, иначе соединения аккумулируются
            # без освобождения на каждой сделке.
            if exchange is not None and not getattr(exchange, "_is_persistent_trade_client", False):
                await exchange.close()

    @staticmethod
    def _is_exchange_supported(exchange_name: str) -> bool:
        """Проверяет поддержку биржи в CCXT БЕЗ создания клиента —
        используется для предварительной проверки ОБЕИХ ног ДО отправки
        любого ордера (см. комментарий в _execute_spread_async)."""
        exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
        return hasattr(ccxt_async, exchange_id)

    @classmethod
    def _is_exchange_configured(cls, exchange_name: str) -> bool:
        """Проверяет, заданы ли для биржи реальные (не плейсхолдерные)
        API-ключи в .env — по переменным <БИРЖА>_API_KEY/_API_SECRET.

        БЕЗОПАСНОСТЬ при DEMO_TRADING=True: биржи вне DEMO_TRADING_SUPPORTED
        (mexc, aster) намеренно считаются "не сконфигурированными" — иначе
        одна нога спреда могла бы уйти в demo (виртуальные деньги, bybit/
        bitget/gate), а другая — на mexc/aster с РЕАЛЬНЫМИ деньгами, и
        получилась бы настоящая незахеджированная позиция, пока пользователь
        думает, что просто тестирует demo-режим. В demo-режиме сигналы,
        затрагивающие mexc/aster, честно пропускаются целиком (та же логика
        pre-check, что и для неподдерживаемых бирж вроде OURBIT) — ЕСЛИ
        только пользователь явно не разрешил это в DEMO_ALLOW_LIVE_EXCHANGES
        (см. _demo_allowed_live_exchanges) — осознанное согласие торговать
        этой биржей реальными деньгами даже в паре с demo-ногой."""
        exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
        if (
            _is_demo_trading()
            and exchange_id not in DEMO_TRADING_SUPPORTED
            and exchange_id not in _demo_allowed_live_exchanges()
        ):
            return False
        api_key, api_secret = cls._get_exchange_credentials(exchange_name)
        return not _looks_like_placeholder(api_key) and not _looks_like_placeholder(api_secret)

    async def _get_taker_fee_rate(self, exchange_name: str, symbol: str) -> Optional[float]:
        """Ставка taker-комиссии биржи (например, 0.0008 = 0.08%) — из КЭША,
        мгновенно, без сетевых запросов на пути сделки.

        ПЕРЕПИСАНО 2026-09-13 (см. большой комментарий у _FEE_RATE_CACHE):
        раньше на каждую сделку создавался новый клиент и качался ВЕСЬ
        список рынков (12-16с, регулярные таймауты, None в журнале), а
        ставка бралась из публичного справочника CCXT, который на mexc/
        bybit/bitget занижал реальную комиссию в 1.7-4 раза.

        Приоритет: fill (реальная, с наших исполнений) > override (.env) >
        static (market["taker"] с УЖЕ загруженного персистентного клиента,
        кэш на FEE_RATE_CACHE_TTL_SECONDS). Метод стал instance-методом
        (был @staticmethod), т.к. фолбэку нужен self._get_ready_client —
        все вызовы и так шли через self."""
        exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())

        # 1) Реальная ставка с наших же исполнений — всегда лучше любых
        #    справочников; не устаревает, обновляется каждой сделкой.
        cached = _FEE_RATE_CACHE.get(exchange_id)
        if cached and cached.get("source") == "fill" and cached.get("rate"):
            return cached["rate"]

        # 2) Явная ставка из .env (посеяно реальными замерами 2026-09-13) —
        #    пока по бирже нет ни одного своего исполнения.
        override = _fee_overrides().get(exchange_id)
        if override is not None:
            return override

        # 3) Статичная из справочника, но БЕЗ нового клиента и БЕЗ
        #    load_markets — рынки у персистентного клиента уже в памяти.
        if cached and cached.get("rate") and time.monotonic() - cached["ts"] < _fee_cache_ttl_seconds():
            return cached["rate"]
        try:
            exchange = await self._get_ready_client(exchange_name)
            market = exchange.markets.get(symbol) if exchange.markets else None
            rate = market.get("taker") if market else None
        except Exception:
            rate = None
        if rate is not None:
            _FEE_RATE_CACHE[exchange_id] = {"rate": rate, "ts": time.monotonic(), "source": "static"}
            return rate
        # Ничего не получили — отдаём хоть протухший кэш, если он есть.
        return cached.get("rate") if cached else None

    @staticmethod
    def _vwap_from_levels(levels, amount_usdt: float) -> Optional[float]:
        """Общая логика "прохода" (walking) по одной стороне стакана
        (bids ИЛИ asks) до набора нужного объёма в USDT — вынесена в
        отдельный метод, чтобы _get_book_snapshot мог посчитать VWAP для
        ОБЕИХ сторон одного и того же стакана без дублирования кода (было
        раньше внутри _estimate_execution_price, которая делала это ПО
        ОТДЕЛЬНОМУ сетевому запросу на каждую сторону)."""
        if not levels:
            return None
        remaining_usdt = amount_usdt
        total_cost = 0.0
        total_amount = 0.0
        for level in levels:
            # Некоторые биржи (например MEXC) возвращают на уровень стакана
            # НЕ пару [price, amount], а тройку [price, amount, count]
            # (количество ордеров на уровне) — распаковка "price, amount ="
            # с прямым unpacking падает с ValueError на такой тройке, тихо
            # проглатываемой внешним except — из-за этого раньше проверка
            # на MEXC всегда молча возвращала None. Берём первые два
            # элемента явно, независимо от длины уровня.
            if len(level) < 2:
                continue
            price, amount = level[0], level[1]
            if not price or not amount:
                continue
            level_value = price * amount
            if level_value >= remaining_usdt:
                take_amount = remaining_usdt / price
                total_cost += take_amount * price
                total_amount += take_amount
                remaining_usdt = 0.0
                break
            total_cost += level_value
            total_amount += amount
            remaining_usdt -= level_value

        if remaining_usdt > 0:
            # Стакан (в пределах запрошенной глубины limit=50) тоньше, чем
            # весь наш объём — берём цену последнего уровня как
            # консервативную (пессимистичную) оценку "хуже уже не будет в
            # пределах видимой книги", а не молчим вообще.
            return levels[-1][0] if levels and levels[-1][0] else None
        if total_amount <= 0:
            return None
        return total_cost / total_amount  # VWAP

    async def _refresh_balance_if_stale(self, exchange_name: str, max_age_seconds: float) -> None:
        """Обновить кэш баланса, если он старше max_age_seconds. Вызывается
        из ФОНОВОГО цикла сверки позиций (scanner.py), чтобы к моменту
        реальной сделки баланс уже лежал в кэше и вход не платил 0.7-2.0с
        за его запрос. Ничего не возвращает и никогда не бросает наружу —
        это чистый прогрев."""
        exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
        cached = _BALANCE_CACHE.get(exchange_id)
        if cached and time.monotonic() - cached["ts"] < max_age_seconds:
            return
        # Сбрасываем запись, чтобы _get_free_balance пошёл за свежей (он
        # сам положит результат в кэш).
        _BALANCE_CACHE.pop(exchange_id, None)
        try:
            await self._get_free_balance(exchange_name)
        except Exception:
            pass

    async def _get_free_balance(self, exchange_name: str) -> "float | None":
        """СВОБОДНЫЙ (не занятый под открытые позиции) баланс биржи в её
        валюте расчёта. None при любой ошибке — вызывающий код тогда просто
        не блокирует сделку (fail-open: лучше попробовать и получить отказ
        биржи, чем молча не торговать из-за сбоя вспомогательного запроса).

        Добавлено 2026-09-13 после реального случая CVC: gate отклонил ногу
        по нехватке маржи ("margin 5.042336 while available 3.00354988277")
        уже ПОСЛЕ того, как вторая нога на bybit успешно открылась — деньги
        ушли на комиссию за открытие и откат впустую. Причина нехватки была
        известна заранее: на gate $5.09 из $8.74 заперты под другой
        открытой позицией (MTL), а свободных оставалось всего $3."""
        exchange_id_cache = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
        cached = _BALANCE_CACHE.get(exchange_id_cache)
        if cached and time.monotonic() - cached["ts"] < _balance_cache_ttl():
            return cached["free"]
        try:
            exchange = await self._get_ready_client(exchange_name)
            balance = await exchange.fetch_balance()
        except Exception as exc:
            print(f"[balance] {exchange_name}: не удалось получить баланс ({type(exc).__name__}: {exc})")
            return None
        # Hyperliquid считает в USDC, остальные наши биржи — в USDT
        # (см. EXCHANGE_QUOTE_CURRENCY).
        exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
        currency = EXCHANGE_QUOTE_CURRENCY.get(exchange_id, "USDT")
        entry = balance.get(currency) or {}
        free = entry.get("free")
        if free is not None:
            _BALANCE_CACHE[exchange_id_cache] = {"free": free, "ts": time.monotonic()}
        return free

    async def _get_book_snapshot(
        self, exchange_name: str, coin: str, amount_usdt: float
    ) -> Optional[dict]:
        """ОДИН запрос стакана (fetch_order_book), который отдаёт ВСЁ,
        что нужно перед входом: и цену для проверки направления, и VWAP
        для проверки актуальности спреда на обе возможные стороны сразу.

        Переписано 2026-09-07 по явной просьбе пользователя ("заходим
        поздно из-за второй проверки, можем ли уменьшить время") — раньше
        это были ДВА ПОСЛЕДОВАТЕЛЬНЫХ сетевых раунда на биржу: сначала
        fetch_ticker (для направления, см. бывшую _get_reference_price),
        потом ОТДЕЛЬНЫЙ fetch_order_book (для VWAP, см. бывшую
        _estimate_execution_price) — притом на КАЖДУЮ ногу, т.е. итого 4
        последовательных сетевых вызова между "нашли связку" и "отправили
        ордер". Bid/ask стакана и так дают полноценную "референсную" цену
        (не хуже last по свежести — это ЖИВОЙ топ стакана, а не цена
        ПОСЛЕДНЕЙ прошедшей сделки), а сам стакан УЖЕ содержит обе стороны
        (bids И asks) — значит VWAP на покупку и на продажу можно посчитать
        из ОДНОГО и того же fetch_order_book, не запрашивая его дважды.
        Итог: 4 сетевых вызова -> 2 (по одному на биржу, оба в parallel
        через asyncio.gather в вызывающем коде) — вдвое короче задержка
        между "нашли спред" и "отправили ордера", меньше шанс, что спред
        успеет уйти за время проверки.

        Возвращает None при любой ошибке (сеть, символ не найден, пустой
        стакан) — вызывающий код тогда пропускает и направление-, и
        spread-проверку, как и раньше при сетевых сбоях (fail-open)."""
        # СНАЧАЛА — ПОТОКОВЫЙ СТАКАН (добавлено 2026-09-14 по прямой просьбе
        # пользователя "можем ли всё сделать на веб-сокете, чтобы быстрее").
        # Если сканер уже держит подписку на эту пару (открытая позиция или
        # КАНДИДАТ на вход — см. scanner.py:_note_book_candidates), книга
        # лежит в памяти и читается за 0мс вместо 282-2859мс REST (медиана
        # ~540мс по [timing]). get_book сам отдаёт None, если поток молчит
        # дольше BOOK_STREAM_MAX_AGE_SECONDS или книга пустая — тогда
        # честно идём по REST ниже, торговая логика не меняется.
        symbol = _build_symbol(exchange_name, coin)
        exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
        streamed = book_stream.get_book(exchange_id, symbol)
        if streamed is not None:
            bids, asks = streamed.get("bids"), streamed.get("asks")
            if bids and asks and bids[0] and asks[0]:
                best_bid, best_ask = bids[0][0], asks[0][0]
                return {
                    "reference_price": (best_bid + best_ask) / 2 if best_bid and best_ask else None,
                    "buy_vwap": self._vwap_from_levels(asks, amount_usdt),
                    "sell_vwap": self._vwap_from_levels(bids, amount_usdt),
                    "source": "поток",
                }

        exchange = None
        try:
            exchange = await self._get_ready_client(exchange_name)
            if symbol not in exchange.markets:
                await exchange.load_markets(True)
                _warm_markets_cache[EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())] = exchange.markets
                if symbol not in exchange.markets:
                    return None
            # ВАЖНО (найдено 2026-09-09 на реальном инциденте: PORTAL
            # kucoin/aster НИ РАЗУ не смогла открыться — "не удалось
            # получить цену/стакан хотя бы по одной ноге"): у большинства
            # бирж fetch_order_book принимает произвольный limit, но
            # KuCoin Futures — исключение, API жёстко требует РОВНО 20 или
            # 100 ("fetchOrderBook() limit argument must be 20 or 100") —
            # наши обычные limit=50 всегда падали BadRequest, что
            # ловилось общим except ниже и молча превращалось в None
            # (fail-closed — сделка отменялась, а НЕ падала с ошибкой в
            # логах, поэтому причина была не видна).
            order_book_limit = 100 if EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower()) == "kucoinfutures" else 50
            order_book = await exchange.fetch_order_book(symbol, limit=order_book_limit)
            bids, asks = order_book.get("bids"), order_book.get("asks")
            if not bids or not asks:
                return None
            best_bid, best_ask = bids[0][0], asks[0][0]
            reference_price = (best_bid + best_ask) / 2 if best_bid and best_ask else None
            return {
                "reference_price": reference_price,
                "buy_vwap": self._vwap_from_levels(asks, amount_usdt),   # LONG (покупка) — идём по asks
                "sell_vwap": self._vwap_from_levels(bids, amount_usdt),  # SHORT (продажа) — идём по bids
                "source": "REST",
            }
        except Exception as exc:
            # Раньше здесь была ТИХАЯ отмена (return None без единой
            # строки в логе) — именно поэтому баг с лимитом стакана
            # KuCoin (см. комментарий выше) пришлось диагностировать
            # вручную, воспроизводя вызов отдельным скриптом. Теперь
            # причина видна сразу в логе бота.
            print(f"[book-snapshot] {exchange_name}/{coin}: {type(exc).__name__}: {exc}")
            return None
        finally:
            # См. _get_ready_client — персистентный клиент не закрываем.
            if exchange is not None and not getattr(exchange, "_is_persistent_trade_client", False):
                await exchange.close()

    # -------------------------------------------------------------------
    # _open_both_legs_async — ЯДРО открытия спреда: одновременно запускает
    # LONG- и SHORT-ордера через asyncio.gather() и замеряет реальную
    # задержку между началом и завершением обеих операций. Возвращает
    # СТРУКТУРИРОВАННЫЙ словарь (не текст) — его использует и LLM-обёртка
    # _execute_spread_async (форматирует в текст для агента), и публичный
    # open_spread() (для прямого вызова из Python-кода бота, в обход LLM —
    # см. main.py: цена входа/order_id нужны боту для учёта позиции и
    # расчёта PnL при закрытии, а доверять эти числа генерации текста
    # языковой моделью для реальных денег не стоит).
    # -------------------------------------------------------------------
    async def _open_both_legs_async(
        self, coin: str, long_exchange: str, short_exchange: str, amount_usdt: Optional[float] = None,
        prefetched_books=None, prefetched_books_at=None,
    ) -> dict:
        # amount_usdt можно передать явно (например, scanner.py передаёт
        # AUTO_TRADE_AMOUNT_USDT — свой размер сделки для авто-найденных
        # связок, отдельный от размера сигналов канала) — если не передан,
        # берём общий TRADE_SIZE_USDT из .env, как и раньше.
        if amount_usdt is None:
            amount_usdt = float(os.getenv("TRADE_SIZE_USDT", "100"))
        leverage = int(os.getenv("TRADE_LEVERAGE", "10"))
        dry_run = os.getenv("DRY_RUN", "True").lower() == "true"

        # ВАЖНО: проверяем ОБЕ ноги ДО того, как отправлять ХОТЬ ОДИН
        # ордер. Иначе при DRY_RUN=False возможна ситуация: LONG-нога
        # улетает на поддерживаемую/сконфигурированную биржу, а SHORT-нога
        # тут же отваливается (биржи нет в CCXT ИЛИ для неё не заданы
        # ключи) — и мы остаёмся с НЕЗАХЕДЖИРОВАННОЙ позицией на реальные
        # деньги. Обе корутины в asyncio.gather() стартуют конкурентно,
        # поэтому такую проверку нельзя переложить на except-блок внутри
        # них — к этому моменту уже поздно, ордер мог уйти на биржу.
        unsupported = [
            name for name in (long_exchange, short_exchange)
            if not self._is_exchange_supported(name)
        ]
        # Ключи проверяем ТОЛЬКО в боевом режиме — DRY_RUN намеренно не
        # требует настоящих API-ключей ни для одной биржи.
        not_configured = [
            name for name in (long_exchange, short_exchange)
            if name not in unsupported and not dry_run and not self._is_exchange_configured(name)
        ]
        if unsupported or not_configured:
            reasons = []
            if unsupported:
                reasons.append(
                    f"биржа(и) {', '.join(unsupported)} не поддерживается(-ются) CCXT"
                )
            if not_configured:
                reasons.append(
                    f"для биржи(биржи) {', '.join(not_configured)} не заданы API-ключи в .env"
                )
            return {
                "coin": coin.upper(),
                "cancelled": True,
                "reason": "; ".join(reasons),
                "long": None,
                "short": None,
                "elapsed_ms": None,
            }

        # =====================================================================
        # ПРОВЕРКА НАПРАВЛЕНИЯ ВХОДА (добавлено 2026-09-06 по просьбе
        # пользователя после реального случая с XRP: сигнал канала назвал
        # long_exchange биржу, которая к МОМЕНТУ ИСПОЛНЕНИЯ оказалась
        # ДОРОЖЕ short-биржи — вход получился с ОТРИЦАТЕЛЬНЫМ спредом
        # (купили дороже, продали дешевле), хотя должно быть наоборот.
        #
        # Причина: в отличие от scanner.py (тот сам всегда ставит LONG на
        # более дешёвую биржу — см. _evaluate_pair), сигналы Telegram-канала
        # задают long_exchange/short_exchange НАПРЯМУЮ текстом сообщения —
        # если цена успела измениться между моментом сигнала и моментом
        # исполнения (или сам канал ошибся), мы слепо исполняли ЕГО
        # направление. Здесь — та же подстраховка, что и у сканера: прямо
        # перед отправкой ордеров сверяем АКТУАЛЬНУЮ цену на обеих биржах и,
        # если long_exchange оказывается ДОРОЖЕ short_exchange, меняем их
        # местами — так вход ВСЕГДА происходит с положительным (или хотя бы
        # неотрицательным) спредом, независимо от источника сигнала.
        #
        # Если проверочную цену получить не удалось (сеть, символ не найден)
        # — не блокируем сделку, торгуем как задано сигналом (см.
        # _get_book_snapshot: возвращает None при любой ошибке).
        # =====================================================================
        # ЗАМЕР ПО ШАГАМ (добавлено 2026-09-13 по прямой просьбе
        # пользователя — "не знаем, где именно теряем секунды"): раньше
        # измерялась ТОЛЬКО отправка самих ордеров (elapsed_ms ниже), и
        # общая задержка 2.8-3.4с была чёрным ящиком — невозможно сказать,
        # что именно тормозит: стакан, проверки, сами ордера или запрос
        # комиссий. Теперь каждый этап замеряется отдельно и печатается
        # строкой [timing], а разбивка едет в trade_ledger (см. timings в
        # возвращаемом словаре) — чтобы оптимизировать по фактам, а не на
        # ощупь.
        timings: dict = {}
        phase_start = time.monotonic()

        long_price_check = short_price_check = None
        if not dry_run:
            # ОДИН запрос стакана на биржу вместо двух последовательных
            # раундов (см. _get_book_snapshot — переписано 2026-09-07 по
            # просьбе пользователя "заходим поздно из-за второй проверки,
            # уменьшить время") — снапшот даёт СРАЗУ и референсную цену
            # (для направления), и VWAP на обе стороны (для spread-check
            # ниже), какая бы сторона ни досталась этой бирже после
            # определения направления.
            # Балансы тянем В ТОМ ЖЕ gather'е, что и стаканы (добавлено
            # 2026-09-13 после реального случая CVC: gate отклонил ногу по
            # марже — "margin 5.04 while available 3.00" — уже ПОСЛЕ того,
            # как нога на bybit успешно открылась, и мы заплатили комиссию
            # за её открытие + откат впустую). Проверка баланса ДО отправки
            # ордеров это предотвращает, а раз она едет параллельно с уже
            # существующим запросом стаканов — общая задержка входа НЕ
            # растёт (ждём максимум из четырёх запросов вместо максимума
            # из двух, а они примерно одинаковы по времени).
            # ПЕРЕИСПОЛЬЗОВАНИЕ СВЕЖИХ СТАКАНОВ (добавлено 2026-09-14):
            # scanner.py только что запрашивал ровно эти же стаканы в своей
            # ранней проверке спреда (early-spread-check) и передал их сюда.
            # Повторный запрос стоил ~560мс медианы по [timing] и ничего не
            # уточнял: между теми двумя точками обычно миллисекунды, т.к.
            # проверка схождения почти всегда берётся из кэша. Но если она
            # НЕ попала в кэш (реальный запрос часовых свечей с двух бирж —
            # это секунды), снимок успевает устареть, и тогда честно
            # запрашиваем заново: решение о входе принимается по свежей
            # книге, экономия не должна этого ломать.
            max_age = float(os.getenv("PREFETCHED_BOOK_MAX_AGE_SECONDS", "1.0"))
            reuse_books = (
                prefetched_books
                and all(b is not None for b in prefetched_books)
                and prefetched_books_at is not None
                and (time.monotonic() - prefetched_books_at) <= max_age
            )
            if reuse_books:
                (long_snapshot, short_snapshot), (long_balance, short_balance) = prefetched_books, await asyncio.gather(
                    self._get_free_balance(long_exchange),
                    self._get_free_balance(short_exchange),
                )
                timings["book_snapshot_reused"] = True
                # Откуда сканер взял эти книги — из потока (0мс) или по REST.
                # Пишем в timings, чтобы журнал сделок сам показывал, сработал
                # ли потоковый стакан для кандидатов на РЕАЛЬНОМ входе.
                timings["book_source"] = "/".join(
                    (b or {}).get("source", "?") for b in (long_snapshot, short_snapshot)
                )
            else:
                long_snapshot, short_snapshot, long_balance, short_balance = await asyncio.gather(
                    self._get_book_snapshot(long_exchange, coin, amount_usdt),
                    self._get_book_snapshot(short_exchange, coin, amount_usdt),
                    self._get_free_balance(long_exchange),
                    self._get_free_balance(short_exchange),
                )
            timings["book_snapshot_ms"] = round((time.monotonic() - phase_start) * 1000, 1)
            phase_start = time.monotonic()
            long_price_check = long_snapshot.get("reference_price") if long_snapshot else None
            short_price_check = short_snapshot.get("reference_price") if short_snapshot else None

            # Нормализация USDC->USDT (см. _to_usdt) — ТОЛЬКО для сравнения
            # цен МЕЖДУ биржами (направление/спред); long_price_check/
            # short_price_check остаются в ЛОКАЛЬНОЙ валюте биржи и ниже
            # передаются в known_price для расчёта РАЗМЕРА ордера — там
            # нужна именно локальная цена, не нормализованная.
            long_price_norm, short_price_norm = await asyncio.gather(
                _to_usdt(long_price_check, long_exchange),
                _to_usdt(short_price_check, short_exchange),
            )
            if (
                long_price_norm is not None
                and short_price_norm is not None
                and long_price_norm > short_price_norm
            ):
                print(
                    f"[direction-check] {coin.upper()}: сигнал просил LONG {long_exchange} "
                    f"({long_price_norm:.6g} USDT-экв.) / SHORT {short_exchange} ({short_price_norm:.6g} USDT-экв.) — "
                    f"LONG-биржа дороже SHORT-биржи, меняю их местами для положительного спреда."
                )
                long_exchange, short_exchange = short_exchange, long_exchange
                long_price_check, short_price_check = short_price_check, long_price_check
                long_snapshot, short_snapshot = short_snapshot, long_snapshot

            # ПРОВЕРКА АКТУАЛЬНОСТИ СПРЕДА ПО СТАКАНУ — используем VWAP из
            # ТОГО ЖЕ снапшота, что и направление (см. _get_book_snapshot),
            # без второго сетевого запроса. Для LONG-биржи (после
            # возможного свапа выше) берём buy_vwap (walk по asks — мы
            # ПОКУПАЕМ), для SHORT-биржи — sell_vwap (walk по bids — мы
            # ПРОДАЁМ). last-цена ничего не знает о реальной глубине
            # стакана — даже при "нормальном" last спред мог провалиться
            # при фактическом рыночном ордере (реальный случай: ANSEM
            # показал по last 5%, исполнился по факту на 0.67%).
            long_vwap = long_snapshot.get("buy_vwap") if long_snapshot else None
            short_vwap = short_snapshot.get("sell_vwap") if short_snapshot else None
            # Если стакан оценить не удалось (сеть, пустая книга) — падаем
            # обратно на референсную цену (long_price_check/short_price_check)
            # как менее точную, но всё же лучше, чем ничего.
            check_long_price = long_vwap if long_vwap is not None else long_price_check
            check_short_price = short_vwap if short_vwap is not None else short_price_check
            # Та же нормализация USDC->USDT, что и в direction-check выше —
            # спред считаем по ценам, приведённым к единой валюте.
            check_long_price, check_short_price = await asyncio.gather(
                _to_usdt(check_long_price, long_exchange),
                _to_usdt(check_short_price, short_exchange),
            )
            min_spread = _min_spread_for_coin(coin, float(os.getenv("TEST_BATCH_MIN_SPREAD", os.getenv("AUTO_TRADE_MIN_SPREAD", "3.0"))))
            if check_long_price is not None and check_short_price is not None and check_long_price > 0:
                fresh_spread_pct = (check_short_price - check_long_price) / check_long_price * 100
                if fresh_spread_pct < min_spread:
                    source = "VWAP по стакану" if (long_vwap is not None and short_vwap is not None) else "референсная цена (стакан недоступен)"
                    print(
                        f"[spread-check] {coin.upper()}: оценка спреда по {source} прямо перед "
                        f"входом — {fresh_spread_pct:.2f}% (порог {min_spread}%) — "
                        f"сделка отменена ДО отправки ордеров, деньги не тратятся."
                    )
                    return {
                        "coin": coin.upper(),
                        "cancelled": True,
                        "reason": (
                            f"оценка спреда по стакану {fresh_spread_pct:.2f}% "
                            f"(порог входа {min_spread}%) — сделка отменена до отправки ордеров"
                        ),
                        "long": None,
                        "short": None,
                        "elapsed_ms": None,
                    }
            else:
                # БАГ (найден 2026-09-08 на реальной сделке BONER, gate/aster):
                # если _get_book_snapshot не смог получить данные ХОТЯ БЫ по
                # одной ноге (сеть, символ), check_long_price/check_short_price
                # ОБА становятся None (снапшот целиком None -> и VWAP, и
                # reference_price из него — тоже None) — раньше это тихо
                # пропускало ВСЮ проверку спреда (условие if просто не
                # выполнялось), и сделка уходила на биржу СОВСЕМ без
                # проверки. Реальный случай: BONER открылся и исполнился по
                # факту с спредом 0.00% вместо заявленных 5.15% — деньги
                # были реально потрачены на вход+откат. Теперь при
                # отсутствии данных ОТМЕНЯЕМ сделку (fail-closed), а не
                # тихо пропускаем проверку (fail-open) — риск открыть
                # непроверенную сделку важнее риска упустить одну
                # возможность из-за сетевого сбоя.
                print(
                    f"[spread-check] {coin.upper()}: не удалось получить цену/стакан хотя бы по одной "
                    f"ноге ({long_exchange} или {short_exchange}) — сделка отменена ДО отправки ордеров "
                    f"(не рискуем открывать БЕЗ проверки спреда)."
                )
                return {
                    "coin": coin.upper(),
                    "cancelled": True,
                    "reason": "не удалось получить цену/стакан хотя бы по одной ноге — сделка отменена без проверки спреда",
                    "long": None,
                    "short": None,
                    "elapsed_ms": None,
                }

        # ГАРД НА ПЕРЕРАЗМЕР (добавлено 2026-09-13 по прямой просьбе
        # пользователя — см. _estimate_min_notional). Если минимальный лот
        # монеты на ЛЮБОЙ из двух бирж заметно дороже нашего целевого
        # номинала, раньше мы молча открывались НА БОЛЬШЕЕ (реальный
        # ANTHROPIC: цель $8 -> факт ~$21 на ногу, в 2.6 раза больше
        # задуманного риска). Теперь — отменяем сделку ДО отправки ордеров,
        # как и spread-check выше. Проверяем ОБЕ ноги ДО gather'а: если
        # отказаться уже после старта корутин, одна нога могла бы открыться,
        # а вторая нет — и пришлось бы платить за откат.
        if not dry_run:
            try:
                max_overshoot = float(os.getenv("TRADE_MAX_MIN_LOT_OVERSHOOT", "1.5"))
            except (TypeError, ValueError):
                max_overshoot = 1.5
            for leg_exchange, leg_price, leg_free in (
                (long_exchange, long_price_check, long_balance),
                (short_exchange, short_price_check, short_balance),
            ):
                try:
                    leg_client = await self._get_ready_client(leg_exchange)
                    leg_symbol = _build_symbol(leg_exchange, coin)
                    leg_market = leg_client.market(leg_symbol)
                except Exception:
                    continue  # метаданные рынка недоступны — не блокируем (fail-open)
                leg_contract_size = leg_market.get("contractSize") or 1
                min_notional = _estimate_min_notional(leg_market, leg_price, leg_contract_size)

                # ПРОВЕРКА СВОБОДНОЙ МАРЖИ (добавлено 2026-09-13, реальный
                # случай CVC — см. _get_free_balance). Считаем по ФАКТИЧЕСКОМУ
                # номиналу: если минимальный лот дороже нашей цели, реально
                # уйдёт именно он (_bump_amount_to_minimum), и маржи нужно
                # больше. +10% запас на комиссию и движение цены между
                # проверкой и исполнением. leg_free=None означает, что баланс
                # получить не удалось — тогда НЕ блокируем (fail-open, тот же
                # принцип, что и у остальных вспомогательных проверок).
                real_notional = max(amount_usdt, min_notional or 0)
                required_margin = (real_notional / max(1, leverage)) * 1.1
                if leg_free is not None and leg_free < required_margin:
                    print(
                        f"[balance-guard] {coin.upper()}: на {leg_exchange} свободно "
                        f"${leg_free:.2f}, а под ногу нужно ~${required_margin:.2f} "
                        f"(номинал ${real_notional:.2f} при плече {leverage}x) — сделка "
                        f"отменена ДО отправки ордеров, чтобы не открыть одну ногу и не "
                        f"платить за её откат."
                    )
                    return {
                        "coin": coin.upper(),
                        "cancelled": True,
                        "reason": (
                            f"недостаточно свободной маржи на {leg_exchange}: "
                            f"${leg_free:.2f} при необходимых ~${required_margin:.2f}"
                        ),
                        "long": None,
                        "short": None,
                        "elapsed_ms": None,
                    }

                if min_notional and min_notional > amount_usdt * max_overshoot:
                    print(
                        f"[size-guard] {coin.upper()}: минимальный лот на {leg_exchange} стоит "
                        f"~${min_notional:.2f} при цели ${amount_usdt:.2f} на ногу "
                        f"(предел {max_overshoot}x = ${amount_usdt * max_overshoot:.2f}) — "
                        f"сделка отменена ДО отправки ордеров, чтобы не открывать позицию "
                        f"больше задуманной."
                    )
                    return {
                        "coin": coin.upper(),
                        "cancelled": True,
                        "reason": (
                            f"минимальный лот на {leg_exchange} (~${min_notional:.2f}) превышает "
                            f"целевой размер ${amount_usdt:.2f} более чем в {max_overshoot}x — "
                            f"сделка отменена до отправки ордеров"
                        ),
                        "long": None,
                        "short": None,
                        "elapsed_ms": None,
                    }

        timings["pre_order_checks_ms"] = round((time.monotonic() - phase_start) * 1000, 1)

        start_time = time.monotonic()  # Засекаем момент старта обеих корутин

        # asyncio.gather() запускает обе корутины ОДНОВРЕМЕННО (конкурентно)
        # и ждёт завершения обеих — это и есть "минимальная задержка между
        # ордерами", о которой говорится в задаче агента. known_price —
        # переиспользуем уже полученные выше цены (см. комментарий в
        # _place_single_order), не тратя ещё один сетевой запрос на каждую
        # ногу прямо перед отправкой ордера.
        long_result, short_result = await asyncio.gather(
            self._place_single_order(long_exchange, coin, "long", amount_usdt, leverage, known_price=long_price_check),
            self._place_single_order(short_exchange, coin, "short", amount_usdt, leverage, known_price=short_price_check),
        )

        elapsed_ms = round((time.monotonic() - start_time) * 1000, 2)
        timings["orders_ms"] = elapsed_ms
        phase_start = time.monotonic()

        # amount_in_coin нужен боту позже, чтобы ЗАКРЫТЬ ровно тот же объём
        # (см. close_spread). В DRY_RUN и при ошибке цены нет — тогда
        # рассчитывать нечего, амаунт останется None (main.py в этом
        # случае просто не заведёт позицию под учёт).
        #
        # ВАЖНО: приоритет — filled_amount_coin из _place_single_order (РЕАЛЬНО
        # исполненное количество), а не наивная оценка amount_usdt/price.
        # Если _bump_amount_to_minimum подняла объём до минимального лота
        # биржи (см. там же), реальная позиция БОЛЬШЕ исходных $amount_usdt —
        # без filled_amount_coin бот посчитал бы для закрытия старый
        # (заниженный) объём и оставил бы хвост позиции незакрытым.
        for leg_result in (long_result, short_result):
            leg_result["amount_usdt"] = amount_usdt
            leg_result["amount_coin"] = leg_result.get("filled_amount_coin") or (
                amount_usdt / leg_result["price"] if leg_result.get("price") else None
            )

        # =====================================================================
        # АВТОМАТИЧЕСКИЙ ROLLBACK: если ОДНА нога реально открылась, а
        # ВТОРАЯ — нет (например, биржа отклонила ордер по причине,
        # непредсказуемой заранее — лимит объёма/плеча на конкретный
        # символ, требование подписать соглашение и т.п. — сам pre-check
        # выше такое не ловит, т.к. проверяет только поддержку/наличие
        # ключей биржи, а не судьбу конкретного ордера), НЕЗАХЕДЖИРОВАННАЯ
        # позиция остаётся висеть на бирже, пока кто-то не закроет её
        # вручную. Проверено на реальных ордерах 2026-09-05(дважды подряд:
        # BNB на Gate) — вместо "предупредить и понадеяться на ручную
        # проверку", закрываем открывшуюся ногу СРАЗУ ЖЕ, автоматически.
        # =====================================================================
        ok_statuses = ("OK", "DRY_RUN_OK")
        long_ok = long_result["status"] in ok_statuses
        short_ok = short_result["status"] in ok_statuses

        # УЧЁТ — ПЕРВЫМ ДЕЛОМ (см. _persist_provisional_position). Обе ноги
        # исполнены — с этой секунды они наши, и учёт обязан это знать до
        # любого выравнивания, проверок, комиссий и отчётов.
        if long_ok and short_ok:
            _persist_provisional_position(
                coin, long_exchange, short_exchange, long_result, short_result, amount_usdt, leverage
            )

        # =====================================================================
        # ВЫРАВНИВАНИЕ СУММ МЕЖДУ НОГАМИ (по явной просьбе пользователя
        # 2026-09-06: "если зашло на 5 долларов на mexc, должно зайти так же
        # на 5 долларов на gate") — у каждой биржи свой минимальный
        # номинал/лот (см. лестницу сумм и подъём-до-минимума в
        # _place_single_order), поэтому ноги МОГУТ открыться на РАЗНЫЕ
        # фактические суммы (например, gate — по запрошенным $2.50, а mexc
        # пришлось поднять до её реального минимума $5.15). Если так —
        # ДОГРУЖАЕМ ту ногу, что меньше, ДОПОЛНИТЕЛЬНЫМ ордером в ТУ ЖЕ
        # сторону на разницу, чтобы обе ноги в итоге были на одинаковый
        # номинал, а не оставались частично нехеджированными.
        # =====================================================================
        if long_ok and short_ok:
            long_actual = long_result.get("actual_amount_usdt") or amount_usdt
            short_actual = short_result.get("actual_amount_usdt") or amount_usdt
            if long_actual and short_actual and abs(long_actual - short_actual) > 0.01:
                if long_actual < short_actual:
                    small_side, small_exchange, small_result = "long", long_exchange, long_result
                else:
                    small_side, small_exchange, small_result = "short", short_exchange, short_result
                target_usdt = max(long_actual, short_actual)
                topup_usdt = target_usdt - min(long_actual, short_actual)
                print(
                    f"[align] {coin.upper()}: суммы ног разошлись (long ${long_actual:.2f} / "
                    f"short ${short_actual:.2f}) — догружаю {small_side} на {small_exchange} "
                    f"ещё на ${topup_usdt:.2f} для выравнивания."
                )
                topup_result = await self._place_single_order(
                    small_exchange, coin, small_side, topup_usdt, leverage
                )
                if topup_result["status"] in ok_statuses:
                    old_amount_coin = small_result.get("amount_coin") or 0
                    old_price = small_result.get("price") or 0
                    topup_coin = topup_result.get("filled_amount_coin") or 0
                    topup_price = topup_result.get("price") or old_price
                    total_coin = old_amount_coin + topup_coin
                    if total_coin:
                        # Средневзвешенная цена входа по двум ордерам одной ноги.
                        small_result["price"] = (
                            old_amount_coin * old_price + topup_coin * topup_price
                        ) / total_coin
                    small_result["amount_coin"] = total_coin
                    small_result["amount_usdt"] = target_usdt
                    small_result["topup_order_id"] = topup_result.get("order_id")
                    print(
                        f"[align] {coin.upper()}: выравнивание прошло — {small_side} теперь "
                        f"{total_coin:.6g} монет на ${target_usdt:.2f}."
                    )
                    # Объём ноги изменился — обновляем предварительную запись
                    # в учёте, чтобы сверка и монитор видели актуальные числа.
                    _persist_provisional_position(
                        coin, long_exchange, short_exchange, long_result, short_result, amount_usdt, leverage
                    )
                else:
                    # Не удалось выровнять — оставляем позицию как есть (обе
                    # ноги уже реально открыты и захеджированы, просто на
                    # разные суммы) — это лучше, чем откатывать успешную
                    # сделку только из-за несовпадения размеров.
                    print(
                        f"[align] {coin.upper()}: не удалось выровнять суммы "
                        f"({topup_result['status']}) — позиция остаётся с разными "
                        f"объёмами по ногам."
                    )

        # =====================================================================
        # ПРОВЕРКА ФАКТИЧЕСКОГО СПРЕДА ПОСЛЕ ИСПОЛНЕНИЯ (добавлено
        # 2026-09-07 по явной просьбе пользователя: "если фактический
        # спред менее 3, то не входи" — реальный случай: проверка ДО
        # отправки ордеров (см. "ПРОВЕРКА АКТУАЛЬНОСТИ СПРЕДА" выше)
        # показала спред в норме, но САМО РЫНОЧНОЕ ИСПОЛНЕНИЕ проскользнуло
        # ниже порога — вошли по факту на 2.22% вместо требуемых 3%).
        # Предыдущая проверка защищает от УСТАРЕВШИХ данных, эта — от
        # РЕАЛЬНОГО проскальзывания при самом исполнении: если ОБЕ ноги
        # открылись, но спред по ФАКТИЧЕСКИМ ценам исполнения оказался
        # ниже порога входа — считаем сделку несостоявшейся и немедленно
        # закрываем ОБЕ ноги (тот же принцип, что и rollback ниже для
        # случая "не открылась одна нога", только здесь обе открылись, но
        # результат всё равно не соответствует требованиям).
        # =====================================================================
        if long_ok and short_ok:
            long_fill_price = long_result.get("price")
            short_fill_price = short_result.get("price")
            if long_fill_price and short_fill_price and long_fill_price > 0:
                realized_spread_pct = (short_fill_price - long_fill_price) / long_fill_price * 100
                min_spread = _min_spread_for_coin(coin, float(os.getenv("TEST_BATCH_MIN_SPREAD", os.getenv("AUTO_TRADE_MIN_SPREAD", "3.0"))))
                if realized_spread_pct < min_spread:
                    print(
                        f"[realized-spread-check] {coin.upper()}: обе ноги открылись, но "
                        f"ФАКТИЧЕСКИЙ спред по ценам исполнения — {realized_spread_pct:.2f}% "
                        f"(порог входа {min_spread}%) — закрываю ОБЕ ноги немедленно, "
                        f"сделка не засчитывается."
                    )
                    long_rb, short_rb = await asyncio.gather(
                        self._close_single_order(long_exchange, coin, "long", long_result["amount_coin"]),
                        self._close_single_order(short_exchange, coin, "short", short_result["amount_coin"]),
                    )
                    long_result["rollback"] = long_rb
                    short_result["rollback"] = short_rb
                    # Обе ноги закрыты — предварительная запись в учёте больше
                    # не соответствует реальности, снимаем (иначе сверка
                    # решила бы, что ноги «пропали», и подняла тревогу).
                    try:
                        position_store.pop_position(coin.upper())
                    except Exception:
                        pass
                    return {
                        "coin": coin.upper(),
                        "cancelled": True,
                        "reason": (
                            f"фактический спред исполнения {realized_spread_pct:.2f}% "
                            f"ниже порога {min_spread}% — обе ноги закрыты сразу после открытия"
                        ),
                        "long": long_result,
                        "short": short_result,
                        "elapsed_ms": elapsed_ms,
                    }

        # ============================================================
        # МГНОВЕННАЯ фоновая перепроверка (добавлено 2026-09-10 по прямой
        # просьбе пользователя после реального инцидента LAB/MEXC,
        # -$2.57 — см. полный комментарий у _delayed_leg_recheck ниже).
        # Не ждём (asyncio.create_task, НЕ await) — не должна тормозить
        # остальную логику; результат либо тихо ничего не найдёт (нога
        # правда не открылась), либо сама закроет голую ногу и залогирует.
        # ============================================================
        if long_ok and not short_ok and long_result.get("amount_coin"):
            rollback = await self._close_single_order(
                long_exchange, coin, "long", long_result["amount_coin"]
            )
            long_result["rollback"] = rollback
            failure_reason = f"SHORT на {short_exchange} не открылся: {short_result['status']}"
            print(
                f"[rollback] {coin.upper()}: SHORT на {short_exchange} не открылся "
                f"({short_result['status']}) — автоматически закрываю LONG на "
                f"{long_exchange}: {rollback['status']}"
            )
            # См. blocked_coins_store.py — по явной просьбе пользователя
            # 2026-09-09: после нескольких таких РЕАЛЬНЫХ откатов подряд
            # (не пустых VWAP-отмен — тут деньги реально были потрачены на
            # комиссии round-trip) монета блокируется автоматически, а не
            # продолжает пытаться раз за разом (реальный случай: 4STOCK —
            # 12 откатов подряд на Aster).
            if blocked_coins_store.record_rollback_failure(coin, failure_reason):
                print(
                    f"[blocked] {coin.upper()}: {blocked_coins_store.FAILURE_THRESHOLD}-й реальный откат "
                    f"подряд — монета АВТОМАТИЧЕСКИ ЗАБЛОКИРОВАНА до ручной разблокировки. "
                    f"Последняя причина: {failure_reason}"
                )
            if blocked_coins_store.is_permanent_error(failure_reason):
                # ТОЧЕЧНОЕ исключение (coin:биржа), а не глобальный бан монеты
                # на всех биржах — см. auto_exclude_coin_on_exchange, по явной
                # просьбе пользователя 2026-09-11. Ошибка произошла именно на
                # short_exchange (та нога не открылась), поэтому исключаем
                # монету именно там, а не везде.
                if blocked_coins_store.auto_exclude_coin_on_exchange(coin, short_exchange):
                    print(
                        f"[excluded] {coin.upper()}:{short_exchange}: ошибка биржи заведомо не "
                        f"исправится повторной попыткой ('contract not activated') — пара "
                        f"добавлена в SCANNER_EXCLUDED_COIN_EXCHANGE_PAIRS (.env), монета "
                        f"по-прежнему торгуется на остальных биржах."
                    )
            # SHORT на short_exchange был признан неудавшимся — именно эту
            # ногу перепроверяем мгновенно в фоне (см. _delayed_leg_recheck).
            asyncio.create_task(self._delayed_leg_recheck(short_exchange, coin, "short"))
        elif short_ok and not long_ok and short_result.get("amount_coin"):
            rollback = await self._close_single_order(
                short_exchange, coin, "short", short_result["amount_coin"]
            )
            short_result["rollback"] = rollback
            failure_reason = f"LONG на {long_exchange} не открылся: {long_result['status']}"
            print(
                f"[rollback] {coin.upper()}: LONG на {long_exchange} не открылся "
                f"({long_result['status']}) — автоматически закрываю SHORT на "
                f"{short_exchange}: {rollback['status']}"
            )
            if blocked_coins_store.record_rollback_failure(coin, failure_reason):
                print(
                    f"[blocked] {coin.upper()}: {blocked_coins_store.FAILURE_THRESHOLD}-й реальный откат "
                    f"подряд — монета АВТОМАТИЧЕСКИ ЗАБЛОКИРОВАНА до ручной разблокировки. "
                    f"Последняя причина: {failure_reason}"
                )
            if blocked_coins_store.is_permanent_error(failure_reason):
                # Симметрично ветке выше — ошибка произошла на long_exchange
                # (та нога не открылась), исключаем монету именно там.
                if blocked_coins_store.auto_exclude_coin_on_exchange(coin, long_exchange):
                    print(
                        f"[excluded] {coin.upper()}:{long_exchange}: ошибка биржи заведомо не "
                        f"исправится повторной попыткой ('contract not activated') — пара "
                        f"добавлена в SCANNER_EXCLUDED_COIN_EXCHANGE_PAIRS (.env), монета "
                        f"по-прежнему торгуется на остальных биржах."
                    )
            # LONG на long_exchange был признан неудавшимся — перепроверяем
            # именно эту ногу мгновенно в фоне (см. _delayed_leg_recheck).
            asyncio.create_task(self._delayed_leg_recheck(long_exchange, coin, "long"))
        elif not long_ok and not short_ok:
            # НИ ОДНА нога не открылась вообще — реальный случай 2026-09-12
            # (MICRODUCK): mexc отдал "contract not activated", а gate В ТО
            # ЖЕ ВРЕМЯ отклонил по СОВСЕМ другой причине (нехватка маржи).
            # Ветки выше проверяют auto-exclude ТОЛЬКО когда ровно одна
            # нога успешна (та, что осталась, откатывается) — если падают
            # ОБЕ сразу, ни одна из них туда не попадает, и повторяющаяся
            # "contract not activated" никогда не приводит к исключению
            # монеты на этой бирже. Откатывать здесь нечего (обе ноги и
            # так не открылись, денег никто не тратил), но КАЖДУЮ ногу
            # всё равно проверяем на постоянную ошибку независимо от
            # результата другой.
            for leg_result, leg_exchange in ((long_result, long_exchange), (short_result, short_exchange)):
                leg_status = leg_result.get("status") or ""
                if blocked_coins_store.is_permanent_error(leg_status):
                    if blocked_coins_store.auto_exclude_coin_on_exchange(coin, leg_exchange):
                        print(
                            f"[excluded] {coin.upper()}:{leg_exchange}: ошибка биржи заведомо не "
                            f"исправится повторной попыткой ('contract not activated') — пара "
                            f"добавлена в SCANNER_EXCLUDED_COIN_EXCHANGE_PAIRS (.env), монета "
                            f"по-прежнему торгуется на остальных биржах."
                        )

        # Комиссии за вход. Ордера рыночные (type="market") — на
        # подавляющем большинстве бирж это taker-исполнение, поэтому берём
        # taker-ставку. Запрашиваем ОБЕ ставки параллельно — это публичные
        # данные, ключи не нужны, работает и в DRY_RUN.
        long_fee_rate, short_fee_rate = await asyncio.gather(
            self._get_taker_fee_rate(long_exchange, _build_symbol(long_exchange, coin)),
            self._get_taker_fee_rate(short_exchange, _build_symbol(short_exchange, coin)),
        )
        for leg_result, fee_rate in ((long_result, long_fee_rate), (short_result, short_fee_rate)):
            leg_result["taker_fee_rate"] = fee_rate
            leg_result["fee_usdt"] = amount_usdt * fee_rate if fee_rate is not None else None

        # С ЭТОГО МЕСТА ОБЕ НОГИ УЖЕ ОТКРЫТЫ НА БИРЖАХ. Всё ниже — учёт и
        # косметика; ни одна ошибка здесь не имеет права превратить факт
        # «позиция открыта» в исключение «сделка не удалась» (реальный
        # инцидент BONER 2026-09-15, см. _timings_total_ms). Поэтому —
        # в try/except с громким логом, но с обязательным return результата.
        try:
            timings["fee_lookup_ms"] = round((time.monotonic() - phase_start) * 1000, 1)
            timings["total_ms"] = _timings_total_ms(timings)
            # ПОМЕТКА О ПРЕДСТАВИТЕЛЬНОСТИ ЗАМЕРА (добавлено 2026-09-14 по
            # прямой просьбе пользователя после разбора артефакта): у боевого
            # входа стакан приходит готовым из early-spread-check сканера, а у
            # любого вызова в обход сканера (тестовый скрипт, сигнал канала) —
            # запрашивается тут же и стоит ~1.5с. Числа при этом выглядят
            # одинаково, и один раз я уже сравнил боевой путь с синтетическим,
            # не заметив разницы. Теперь строка сама говорит, что это было.
            reused = timings.get("book_snapshot_reused")
            book_source = timings.get("book_source")
            source = (
                f"стакан ГОТОВЫЙ из сканера, источник {book_source}" if reused
                else "стакан ЗАПРОШЕН здесь — НЕ боевой путь"
            )
            print(
                f"[timing] {coin.upper()} ОТКРЫТИЕ: стакан {timings.get('book_snapshot_ms', 0)}мс ({source}) + "
                f"проверки {timings.get('pre_order_checks_ms', 0)}мс + ордера {timings.get('orders_ms', 0)}мс + "
                f"комиссии {timings.get('fee_lookup_ms', 0)}мс = {timings['total_ms']}мс всего"
            )
        except Exception as exc:
            timings.setdefault("total_ms", 0.0)
            print(f"[timing] {coin.upper()}: ошибка подсчёта замеров ({type(exc).__name__}: {exc}) — на сделку НЕ влияет.")

        return {
            "coin": coin.upper(),
            "cancelled": False,
            "reason": None,
            "timings": timings,
            # long_exchange/short_exchange — ФАКТИЧЕСКИ использованные биржи
            # (после возможной перестановки местами, см. "ПРОВЕРКА
            # НАПРАВЛЕНИЯ ВХОДА" выше) — вызывающий код (trade_executor.py)
            # ДОЛЖЕН ориентироваться на них, а не на свои исходные
            # long_exchange/short_exchange из сигнала, иначе отчёт/
            # уведомление/position_store запишут не ту биржу как LONG/SHORT.
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "long": long_result,
            "short": short_result,
            "elapsed_ms": elapsed_ms,
            "leverage": leverage,
        }

    # -------------------------------------------------------------------
    # _execute_spread_async — обёртка вокруг _open_both_legs_async,
    # форматирующая результат в ТЕКСТ для LLM-агента (см. _run/BaseTool).
    # -------------------------------------------------------------------
    async def _execute_spread_async(
        self, coin: str, long_exchange: str, short_exchange: str
    ) -> str:
        result = await self._open_both_legs_async(coin, long_exchange, short_exchange)

        if result["cancelled"]:
            return (
                f"Монета: {result['coin']}\n"
                f"Сделка НЕ открыта ни на одной ноге: {result['reason']}. "
                f"Открытие только одной ноги спреда создало бы "
                f"незахеджированную позицию, поэтому обе ноги отменены."
            )

        long_result, short_result = result["long"], result["short"]
        return (
            f"Монета: {result['coin']}\n"
            f"LONG на {long_exchange}: {long_result['status']} "
            f"(order_id={long_result['order_id']}, price={long_result['price']})\n"
            f"SHORT на {short_exchange}: {short_result['status']} "
            f"(order_id={short_result['order_id']}, price={short_result['price']})\n"
            f"Задержка между ногами спреда: {result['elapsed_ms']} мс"
        )

    # -------------------------------------------------------------------
    # open_spread — публичная точка входа для ПРЯМОГО вызова из Python
    # (main.py), в обход LLM/CrewAI. Возвращает структурированный словарь
    # (entry price, order_id, amount_coin на каждую ногу) — именно эти
    # данные main.py сохраняет в position_store для последующего закрытия
    # и расчёта PnL.
    # -------------------------------------------------------------------
    async def _delayed_leg_recheck(self, exchange_name: str, coin: str, expected_side: str) -> None:
        """МГНОВЕННАЯ фоновая перепроверка ноги, которую _open_both_legs_
        async ТОЛЬКО ЧТО признал неудавшейся (и откатил её партнёра) —
        добавлено 2026-09-10 по прямой просьбе пользователя после
        реального инцидента: MEXC ответил на create_order "не исполнен"
        (status=1, filled=0) даже после нескольких проверок, бот откатил
        партнёрскую ногу как единственно верное решение — а РЕАЛЬНО
        ордер на MEXC исполнился чуть ПОЗЖЕ, оставив голую позицию (LAB,
        -$2.57), которую заметили только вручную.

        Запускается через asyncio.create_task() (НЕ await) прямо из места
        отката — не блокирует и не задерживает остальную логику (по
        прямой просьбе пользователя: "не терять время", "чтобы появлялся
        ордер и в то же время проверка говорила — вышел ли он на биржу").
        Это ТОЧЕЧНАЯ, немедленная версия защиты — общая периодическая
        _reconcile_positions_loop (scanner.py) при этом остаётся как
        резервная сеть на случай, если и эта проверка почему-то не
        сработает (сеть легла именно в эти секунды и т.п.).

        ВАЖНО: полагается на то, что событийный цикл, из которого вызван
        create_task, живёт ДОЛЬШЕ, чем эта проверка (2+ секунды) — это
        так на боевом боте (persistent event loop, см. set_bot_event_loop
        в main.py), но НЕ так для разового asyncio.run() — там задача
        будет отменена вместе с закрытием цикла. Не критично: в DRY_RUN/
        разовых прогонах реальных денег и голых ног не бывает."""
        # Уменьшено с 2.0 до 0.2с по прямой просьбе пользователя
        # 2026-09-10 — учитывая, что это теперь не ЕДИНСТВЕННАЯ защита:
        # общая _reconcile_positions_loop (каждые 15с) подстрахует, если
        # 0.2с окажется недостаточно для конкретной биржи.
        await asyncio.sleep(0.2)
        try:
            exchange = await self._get_ready_client(exchange_name)
            symbol = _build_symbol(exchange_name, coin)
            positions = await exchange.fetch_positions([symbol])
            matches = [
                (p.get("contracts") or 0.0)
                for p in positions
                if p.get("symbol") == symbol
            ]
            live_contracts = max(matches) if matches else 0.0
            if live_contracts <= 0:
                return  # откат был верным решением — ноги реально нет
            print(
                f"[fast-recheck] {coin.upper()} на {exchange_name}: обнаружена РЕАЛЬНО "
                f"открытая {expected_side}-нога ПОСЛЕ того, как её сочли неудавшейся "
                f"({live_contracts} контрактов) — закрываю немедленно, не дожидаясь "
                f"общей сверки!"
            )
            market = exchange.market(symbol)
            contract_size = market.get("contractSize") or 1
            amount_in_coin = live_contracts * contract_size
            result = await self._close_single_order(exchange_name, coin, expected_side, amount_in_coin)
            print(f"[fast-recheck] {coin.upper()}/{exchange_name}: закрыто — {result}")
        except Exception as exc:
            print(
                f"[fast-recheck] {coin.upper()}/{exchange_name}: ошибка перепроверки "
                f"({type(exc).__name__}: {exc}) — резервная общая сверка (scanner.py) "
                f"поймает это в течение минуты."
            )

    def open_spread(
        self, coin: str, long_exchange: str, short_exchange: str, amount_usdt: Optional[float] = None
    ) -> dict:
        # См. _run_coro_blocking — переиспользует персистентные соединения
        # с биржами на живом боте, иначе прежнее поведение (asyncio.run()).
        return _run_coro_blocking(
            self._open_both_legs_async(coin, long_exchange, short_exchange, amount_usdt=amount_usdt)
        )

    # -------------------------------------------------------------------
    # _close_single_order — закрывает ОДНУ ногу ранее открытой позиции:
    # ордер в СТОРОНУ, ПРОТИВОПОЛОЖНУЮ входу (LONG закрывается SELL,
    # SHORT закрывается BUY), с флагом reduceOnly (там, где биржа его
    # поддерживает) — это гарантирует, что ордер именно закрывает
    # существующую позицию, а не случайно открывает новую в другую сторону.
    # -------------------------------------------------------------------
    async def _close_single_order(
        self, exchange_name: str, coin: str, original_side: str, amount_in_coin: float
    ) -> dict:
        symbol = _build_symbol(exchange_name, coin)  # см. _place_single_order — своя валюта котировки на биржу
        close_side = "sell" if original_side == "long" else "buy"
        exchange = None
        try:
            dry_run = os.getenv("DRY_RUN", "True").lower() == "true"

            if dry_run:
                exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
                if not hasattr(ccxt_async, exchange_id):
                    raise UnsupportedExchangeError(
                        f"Биржа '{exchange_name}' не поддерживается библиотекой CCXT"
                    )
                await asyncio.sleep(0.05)
                return {
                    "exchange": exchange_name,
                    "side": close_side,
                    "status": "DRY_RUN_OK",
                    "order_id": "dry-run-close-simulated",
                    "price": None,
                }

            exchange = await self._get_ready_client(exchange_name)  # см. _place_single_order — общий прогретый клиент
            if symbol not in exchange.markets:
                # См. _place_single_order — та же защита от неполной
                # постраничной загрузки рынков (замечено на Gate).
                await exchange.load_markets(True)
                _warm_markets_cache[EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())] = exchange.markets

            # См. _place_single_order — тот же перевод "количество монеты"
            # -> "количество контрактов" через contractSize рынка (для
            # Gate/MEXC это критично, для bybit/bitget/aster — no-op).
            market = exchange.market(symbol)
            contract_size = market.get("contractSize") or 1
            order_amount = amount_in_coin / contract_size

            # =================================================================
            # ЗАЩИТА ОТ "ПРОСКОКА" ЧЕРЕЗ НОЛЬ (добавлено 2026-09-06 после
            # реального инцидента: XRP на mexc и bybit — при повторных
            # попытках закрытия одним и тем же ЗАСТАРЕВШИМ объёмом из
            # position_store каждые 3с, ОБА раза ордер с reduceOnly=True
            # исполнился ПОЛНОСТЬЮ вместо капа по факту открытой позиции —
            # позиция "проскочила" через ноль и открылась в ПРОТИВОПОЛОЖНУЮ
            # сторону на небольшой излишек. Старое предположение "reduceOnly
            # не даст закрыть больше реально открытого объёма" (см. историю
            # правок) ОКАЗАЛОСЬ НЕВЕРНЫМ как минимум для этих бирж/условий.
            #
            # Поэтому теперь ПРЯМО ПЕРЕД отправкой ордера закрытия сверяем
            # РЕАЛЬНЫЙ остаток позиции на бирже и НИКОГДА не просим закрыть
            # больше — так overshoot физически невозможен независимо от
            # того, как конкретная биржа на самом деле трактует reduceOnly.
            # Раньше здесь ещё поднимался order_amount ДО min_amount биржи
            # (если запрошенный объём был меньше минимального лота) — это
            # был ИМЕННО ТОТ механизм, что усиливал риск переисполнения:
            # теперь минимальный лот больше не бампится, амаунт всегда берём
            # из факта (если он определён), а не пытаемся подогнать под
            # биржевой минимум.
            # =================================================================
            # ВАЖНО (найдено 2026-09-09 на реальном инциденте — BP на Gate:
            # MEXC-нога закрылась, Gate-нога осталась ОТКРЫТОЙ часами,
            # position_store при этом считал сделку полностью закрытой):
            # fetch_positions([symbol]) на Gate возвращает ДВЕ записи по
            # одному и тому же symbol — по одной на каждый hedge-mode слот
            # (long/short), и ПУСТОЙ слот (contracts=0.0) в списке идёт
            # ПЕРВЫМ. Старый код брал ПЕРВОЕ совпадение и делал break —
            # то есть ВСЕГДА читал 0.0, даже когда вторая запись в том же
            # списке показывала реальный contracts=1.0. Обе "перепроверки"
            # (эта и recheck ниже) страдали ОДНОЙ и той же ошибкой —
            # поэтому обе "соглашались", что позиции нет, реальный ордер
            # закрытия не отправлялся, а close-код молча считал закрытие
            # успешным. Исправление: берём МАКСИМУМ по всем совпадениям
            # symbol, а не первое попавшееся.
            live_contracts = None
            try:
                live_positions = await exchange.fetch_positions([symbol])
                matches = [
                    (p.get("contracts") or 0.0)
                    for p in live_positions
                    if p.get("symbol") == symbol
                ]
                if matches:
                    live_contracts = max(matches)
            except Exception as exc:
                print(
                    f"[close] {symbol}: не удалось сверить реальный остаток позиции "
                    f"({exc}) — использую объём из учёта как раньше."
                )

            if live_contracts is not None:
                if live_contracts <= 0:
                    # ПЕРЕПРОВЕРКА ПЕРЕД "УЖЕ ЗАКРЫТО" (добавлено 2026-09-07
                    # после реального инцидента: BP на gate — ОДИНОЧНЫЙ
                    # fetch_positions() здесь вернул contracts=0 (устаревшие/
                    # неполные данные биржи в момент запроса), код молча
                    # решил "закрывать нечего, это успех" и ПРОПУСТИЛ реальный
                    # ордер закрытия — позиция физически осталась открытой
                    # (short 1 контракт, entry 0.5873) ещё ЧАСЫ, пока не была
                    # обнаружена и закрыта вручную при прямой проверке биржи.
                    # Само API create_order тут ни при чём — проблема в том,
                    # что мы вообще не дошли до его вызова. Раньше это
                    # молча считалось успехом; теперь перепроверяем ЕЩЁ РАЗ
                    # после короткой паузы, и ТОЛЬКО если оба чтения
                    # согласны (позиции действительно нет), считаем закрытие
                    # успешным — иначе продолжаем как обычно, с реальным
                    # ордером на актуальный остаток. Пауза уменьшена
                    # 2026-09-09 по просьбе пользователя ("уменьши все
                    # задержки") — этот путь и так срабатывает редко
                    # (после фикса max() по совпадениям symbol выше, см.
                    # комментарий про Gate hedge-mode слоты).
                    await asyncio.sleep(0.3)
                    recheck_contracts = live_contracts  # на случай сбоя перепроверки — не хуже прежнего поведения
                    try:
                        recheck_positions = await exchange.fetch_positions([symbol])
                        recheck_matches = [
                            (p.get("contracts") or 0.0)
                            for p in recheck_positions
                            if p.get("symbol") == symbol
                        ]
                        recheck_contracts = max(recheck_matches) if recheck_matches else 0.0
                    except Exception as exc:
                        print(
                            f"[close] {symbol}: не удалось перепроверить остаток "
                            f"позиции ({exc}) — доверяю первому результату."
                        )

                    if recheck_contracts <= 0:
                        print(
                            f"[close] {symbol}: подтверждено ДВУМЯ проверками — "
                            f"позиции на бирже уже нет, реальный ордер закрытия не требуется."
                        )
                        return {
                            "exchange": exchange_name,
                            "side": close_side,
                            "status": "OK",
                            "order_id": None,
                            "price": None,
                        }
                    print(
                        f"[close] {symbol}: первая проверка показала contracts=0, но "
                        f"повторная — {recheck_contracts} — позиция ЕЩЁ ОТКРЫТА, "
                        f"закрываю реальным ордером на актуальный остаток."
                    )
                    live_contracts = recheck_contracts
                order_amount = live_contracts
            else:
                # Не удалось проверить факт (сеть) — старое поведение как
                # страховка: поднимаем до минимального лота, если нужно.
                min_amount = ((market.get("limits") or {}).get("amount") or {}).get("min")
                if min_amount is not None and order_amount < min_amount:
                    order_amount = min_amount

            close_params = {"reduceOnly": True}
            exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
            if exchange_id == "bitget":
                # См. _ensure_bitget_one_way_mode/_place_single_order — тот
                # же форс classic-эндпоинта вместо UTA при закрытии.
                close_params["uta"] = False

            # См. тот же комментарий в _place_single_order — Hyperliquid
            # требует ЯВНЫЙ price в самом create_order() даже для
            # "market"-ордера (реализован как IOC-лимитка со slippage) —
            # без него ArgumentsRequired. Отдельный fetch_ticker здесь
            # (а не переиспользование open-цены) — при закрытии открывающая
            # цена уже устарела на неопределённое время, нужна СВЕЖАЯ.
            close_kwargs = {}
            if exchange_id == "hyperliquid":
                ticker = await exchange.fetch_ticker(symbol)
                close_price = _extract_ticker_price(ticker)
                if close_price:
                    close_kwargs["price"] = close_price

            order = await exchange.create_order(
                symbol=symbol,
                type="market",
                side=close_side,
                amount=order_amount,
                params=close_params,
                **close_kwargs,
            )

            # См. _place_single_order/_verify_order_filled — та же
            # верификация реального исполнения. Здесь цена вдвойне
            # критична: ложный "OK" на закрытии означает, что бот считает
            # позицию закрытой, хотя она физически осталась открытой —
            # риск остаётся, но НЕВИДИМЫМ для дальнейшего мониторинга.
            order = await _verify_order_filled(exchange, order, symbol, exchange_id, exchange_name)
            filled = order.get("filled") or 0
            status = order.get("status")
            if status == "canceled" or filled <= 0:
                raise ccxt_async.ExchangeError(
                    f"ордер закрытия создан (id={order.get('id')}), но НЕ исполнен "
                    f"биржей (status={status}, filled={filled}) — позиция ВСЁ ЕЩЁ "
                    f"физически открыта!"
                )

            price = await _resolve_fill_price(exchange, order, symbol, exchange_id)
            _learn_fee_from_fill(exchange_id, order)  # реальная комиссия -> кэш ставок (см. _FEE_RATE_CACHE)
            _invalidate_balance(exchange_name)  # маржа изменилась — кэш баланса больше не годится
            if not price:
                # Не все биржи сразу возвращают цену исполнения рыночного
                # ордера в ответе create_order, и повторный запрос ордера
                # (_resolve_fill_price) тоже не помог — подстраховываемся
                # тикером как самым крайним случаем.
                ticker = await exchange.fetch_ticker(symbol)
                price = _extract_ticker_price(ticker)

            return {
                "exchange": exchange_name,
                "side": close_side,
                "status": "OK",
                "order_id": order.get("id"),
                "price": price,
            }
        except UnsupportedExchangeError as exc:
            return {
                "exchange": exchange_name,
                "side": close_side,
                "status": f"EXCHANGE_NOT_SUPPORTED: {exc}",
                "order_id": None,
                "price": None,
            }
        except ExchangeNotConfiguredError as exc:
            return {
                "exchange": exchange_name,
                "side": close_side,
                "status": f"NO_CREDENTIALS_CONFIGURED: {exc}",
                "order_id": None,
                "price": None,
            }
        except Exception as exc:
            # Ошибку закрытия НИКОГДА нельзя тихо проглотить — если не
            # закрылась одна нога, позиция остаётся частично открытой
            # (риск на реальные деньги). main.py обязан явно предупредить
            # об этом в отчёте, а не просто залогировать.
            return {
                "exchange": exchange_name,
                "side": close_side,
                "status": f"ERROR: {exc}",
                "order_id": None,
                "price": None,
            }
        finally:
            # См. _get_ready_client — персистентный клиент не закрываем.
            if exchange is not None and not getattr(exchange, "_is_persistent_trade_client", False):
                await exchange.close()

    async def _close_both_legs_async(
        self,
        coin: str,
        long_exchange: str,
        short_exchange: str,
        long_amount_coin: float,
        short_amount_coin: float,
    ) -> dict:
        # Замер по шагам — см. комментарий у timings в _open_both_legs_async.
        timings: dict = {}
        start_time = time.monotonic()

        long_result, short_result = await asyncio.gather(
            self._close_single_order(long_exchange, coin, "long", long_amount_coin),
            self._close_single_order(short_exchange, coin, "short", short_amount_coin),
        )

        elapsed_ms = round((time.monotonic() - start_time) * 1000, 2)
        timings["orders_ms"] = elapsed_ms
        phase_start = time.monotonic()

        # Комиссии за ВЫХОД — те же рыночные (taker) ордера, что и на
        # входе. Считаем от фактической суммы закрытия (объём × цена
        # выхода), а не от исходных amount_usdt, т.к. цена могла измениться.
        long_fee_rate, short_fee_rate = await asyncio.gather(
            self._get_taker_fee_rate(long_exchange, _build_symbol(long_exchange, coin)),
            self._get_taker_fee_rate(short_exchange, _build_symbol(short_exchange, coin)),
        )
        for leg_result, fee_rate, amount_coin in (
            (long_result, long_fee_rate, long_amount_coin),
            (short_result, short_fee_rate, short_amount_coin),
        ):
            leg_result["taker_fee_rate"] = fee_rate
            leg_result["fee_usdt"] = (
                amount_coin * leg_result["price"] * fee_rate
                if fee_rate is not None and leg_result.get("price")
                else None
            )

        # См. комментарий в _open_both_legs_async: ноги уже закрыты на биржах,
        # ошибка подсчёта замеров не должна ронять результат закрытия.
        try:
            timings["fee_lookup_ms"] = round((time.monotonic() - phase_start) * 1000, 1)
            timings["total_ms"] = _timings_total_ms(timings)
            print(
                f"[timing] {coin.upper()} ЗАКРЫТИЕ: ордера {timings['orders_ms']}мс + "
                f"комиссии {timings['fee_lookup_ms']}мс = {timings['total_ms']}мс всего"
            )
        except Exception as exc:
            timings.setdefault("total_ms", 0.0)
            print(f"[timing] {coin.upper()}: ошибка подсчёта замеров закрытия ({type(exc).__name__}: {exc}) — на сделку НЕ влияет.")

        return {
            "coin": coin.upper(),
            "long": long_result,
            "short": short_result,
            "elapsed_ms": elapsed_ms,
            "timings": timings,
        }

    # -------------------------------------------------------------------
    # close_spread — публичная точка входа для закрытия ранее открытой
    # позиции (вызывается из main.py по сигналу "aligned in" из канала).
    # amount_*_coin ОБЯЗАТЕЛЬНО берутся из данных, сохранённых при
    # открытии (position_store), а не пересчитываются заново — иначе при
    # изменении цены закрылся бы не тот объём, что был открыт.
    # -------------------------------------------------------------------
    def close_spread(
        self,
        coin: str,
        long_exchange: str,
        short_exchange: str,
        long_amount_coin: float,
        short_amount_coin: float,
    ) -> dict:
        # См. _run_coro_blocking — переиспользует персистентные соединения
        # с биржами на живом боте, иначе прежнее поведение (asyncio.run()).
        return _run_coro_blocking(
            self._close_both_legs_async(
                coin, long_exchange, short_exchange, long_amount_coin, short_amount_coin
            )
        )
