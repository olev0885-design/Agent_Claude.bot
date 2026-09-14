# =============================================================================
# private_stream.py — ПРИВАТНЫЕ (авторизованные) WebSocket-потоки биржи:
# собственные ПОЗИЦИИ и собственные ОРДЕРА в реальном времени.
# =============================================================================
# Зачем (добавлено 2026-09-14 по прямой просьбе пользователя: "нельзя ли
# использовать такой же метод как с получением стакана, чтобы обновлять
# данные по открытым ордерам без задержки?"). Это прямое продолжение
# book_stream.py — тот же приём, но для ПРИВАТНЫХ данных, и он закрывает
# два разных узких места:
#
# 1) ПОДТВЕРЖДЕНИЕ ИСПОЛНЕНИЯ ОРДЕРА — самая дорогая оставшаяся часть
#    входа. В trade_tool.py:_verify_order_filled, если биржа не вернула
#    filled прямо в ответе на create_order, код СПИТ и ПЕРЕСПРАШИВАЕТ:
#    sleep(0.2) -> fetch_order -> sleep(0.4) -> fetch_order -> sleep(0.6).
#    Это до 1.2с чистого сна плюс три REST-запроса. Bitget по своей
#    природе НИКОГДА не отдаёт статус синхронно (см. комментарий там же),
#    то есть платит эту цену ВСЕГДА. Реальные замеры orders_ms из журнала
#    сделок: binance/bybit (ответ сразу с filled) 719-1281мс против
#    aster/bybit (лестница отработала) 2172мс — разница почти целиком она.
#    watch_orders даёт то же самое событие ПУШЕМ, обычно за 20-80мс.
#
# 2) СВЕРКА ПОЗИЦИЙ (scanner.py:_reconcile_orphaned_positions) — крутится
#    непрерывно и на КАЖДОМ прогоне дёргает fetch_positions по всем семи
#    биржам. Это постоянная нагрузка на API (которая сама же замедляет
#    ответы бирж и приближает rate limit) ради данных, которые почти
#    всегда не меняются. watch_positions держит актуальный срез в памяти.
#
# -----------------------------------------------------------------------
# ГРАНИЦА БЕЗОПАСНОСТИ — ЧИТАТЬ ПЕРЕД ЛЮБОЙ ПРАВКОЙ
# -----------------------------------------------------------------------
# Поток позиций НИКОГДА не является основанием для ДЕЙСТВИЯ с деньгами.
#
# Причина конкретная. Сверка позиций умеет АВТОМАТИЧЕСКИ ЗАКРЫВАТЬ
# "неучтённую" ногу и делать выводы о "пропавшей" ноге. Если бы поток
# ошибочно отдал пустой список (соединение молча умерло, биржа шлёт
# только дельты, мы пропустили начальный снимок) — бот решил бы, что
# нашей ноги на бирже больше нет, и это стоило бы реальных денег. При
# этом у позиций, в отличие от стакана, НЕТ признака свежести: если
# ничего не менялось, поток законно молчит часами, и "давно не было
# сообщений" не отличить от "соединение сдохло".
#
# Поэтому правило жёсткое: поток отвечает только на вопрос "всё ли
# совпадает с тем, что мы и так знаем". Как только данные потока ведут к
# РАСХОЖДЕНИЮ — вызывающий код обязан перепроверить это по REST и
# действовать ТОЛЬКО по результату REST (см. scanner.py, вызов
# _rest_positions_for). В установившемся режиме (расхождений нет, а это
# подавляющее большинство прогонов) REST не дёргается вообще; в момент
# любой аномалии цена ошибки равна нулю, потому что решает всё равно REST.
#
# Дополнительно: состояние ПЕРЕСЕВАЕТСЯ полным REST-снимком при подписке
# и при каждом переподключении — чтобы дельты ложились на достоверную
# базу, а не на пустоту.
# =============================================================================
import asyncio
import os
import time
from typing import Optional

import ccxt.pro as ccxt_pro

# exchange_name (как его знает бот: "gate", "bitget", ...) -> состояние потока
_STREAMS: dict = {}

# Биржи, на которых подписка УПАЛА при создании клиента (нет ключей, биржи
# нет в ccxt.pro, неверная passphrase). Повторять такую попытку бессмысленно
# — ошибка не «моргающая», а постоянная, и без этого множества вызывающий
# код (сверка позиций крутится непрерывно) пытался бы подписаться заново на
# КАЖДОМ прогоне и залил бы лог одинаковыми сообщениями.
_FAILED: set = set()

# Сколько последних ордеров помним в памяти. Нужен только для сверки
# "исполнился ли ордер, который мы только что отправили" — окно в
# несколько сотен с огромным запасом покрывает любой реальный темп бота,
# а верхняя граница не даёт словарю расти всё время жизни процесса.
_MAX_REMEMBERED_ORDERS = 500

# Сколько подряд неудачных попыток терпим, прежде чем признать, что эта
# биржа поток не отдаёт, и тихо остаться на REST — тот же принцип и то же
# число, что в book_stream.py (там это спасло от бесконечных
# переподключений к MEXC, который фьючерсный стакан не отдаёт вовсе).
_MAX_CONSECUTIVE_FAILURES = 3

# Через сколько секунд после того, как ВСЕ насосы биржи сдались, разрешаем
# подписаться заново. Добавлено 2026-09-15: после сна ноутбука сеть
# возвращается не сразу, три попытки с паузой 2с проходят впустую, и без
# этого биржа оставалась бы на REST до следующего рестарта — то есть
# выигрыш от потоков тихо терялся на часы.
_RETRY_AFTER_GIVE_UP_SECONDS = 600.0


def _enabled() -> bool:
    return os.getenv("PRIVATE_STREAM_ENABLED", "True").lower() == "true"


def _positions_enabled() -> bool:
    return _enabled() and os.getenv("PRIVATE_STREAM_POSITIONS", "True").lower() == "true"


def _orders_enabled() -> bool:
    return _enabled() and os.getenv("PRIVATE_STREAM_ORDERS", "True").lower() == "true"


def _fill_wait_seconds() -> float:
    """Сколько ждать событие исполнения из потока, прежде чем уйти на
    обычную REST-лестницу. Держим КОРОТКИМ: смысл всей затеи — получить
    подтверждение за десятки миллисекунд; если за это время события нет,
    значит поток нам сейчас не помощник, и честнее сразу вернуться к
    проверенному пути, чем ждать и проиграть больше, чем выиграли.

    Значение по умолчанию выбрано ТОЧНО РАВНЫМ первой ступени
    REST-лестницы (0.2с): вызывающий код вычитает уже потраченное из
    первой паузы (см. trade_tool.py:_verify_order_filled), поэтому при
    молчащем потоке суммарное ожидание совпадает с прежним до
    миллисекунд — замерено 1235мс до правки против 1235мс после. Это
    обязательное свойство: ускорение не имеет права оборачиваться
    замедлением на пути реальных денег, даже на десятки миллисекунд."""
    try:
        return float(os.getenv("PRIVATE_STREAM_FILL_WAIT_SECONDS", "0.20"))
    except (TypeError, ValueError):
        return 0.20


# =============================================================================
# ВНУТРЕННЕЕ: насосы (pump) — бесконечно читают поток и обновляют память.
# =============================================================================
async def _prepare_client(exchange, exchange_name: str) -> None:
    """Доводка клиента до состояния, в котором приватный поток вообще
    может подписаться. Сейчас это нужно ровно одной бирже.

    GATE требует НОМЕР АККАУНТА (uid) для приватных каналов WebSocket —
    одних ключа и секрета ей мало: без uid и watch_orders, и
    watch_positions падают с ArgumentsRequired "gate requires uid to
    subscribe" (проверено на боевом аккаунте 2026-09-14). Забираем его
    сами через privateAccountGetDetail, а не просим завести ещё одну
    переменную в .env: лишняя ручная настройка — это лишний способ
    ошибиться при переносе бота, а запрос всё равно разовый, на подписке.

    Ошибки наружу не глушим: вызывающий subscribe() поймает их и честно
    оставит биржу на REST — молчаливо подписаться "наполовину" хуже, чем
    явно не подписаться вовсе."""
    if exchange_name != "gate" or getattr(exchange, "uid", None):
        return
    detail = await exchange.privateAccountGetDetail()
    user_id = (detail or {}).get("user_id")
    if user_id:
        exchange.uid = str(user_id)


async def _seed_positions(state: dict, exchange, exchange_name: str) -> None:
    """Полный REST-снимок позиций как ДОСТОВЕРНАЯ БАЗА под будущие дельты.

    Без этого шага пустой словарь неотличим от "позиций нет": биржа,
    которая шлёт только ИЗМЕНЕНИЯ, после подключения промолчит, и мы
    решили бы, что на бирже пусто, хотя там открыта наша нога. Классы
    ccxt.pro наследуют обычный async-клиент, поэтому REST-вызов здесь
    доступен на том же объекте."""
    params = {"settle": "usdt"} if exchange_name == "gate" else {}
    positions = await exchange.fetch_positions(params=params)
    snapshot = {}
    for p in positions:
        symbol = p.get("symbol")
        if symbol and abs(p.get("contracts") or 0) > 0:
            snapshot[symbol] = p
    state["positions"] = snapshot
    state["seeded"] = True
    state["seeded_at"] = time.monotonic()


async def _positions_pump(exchange_name: str, exchange) -> None:
    failures = 0
    while exchange_name in _STREAMS:
        state = _STREAMS.get(exchange_name)
        if state is None:
            return
        try:
            if not state.get("seeded"):
                await _seed_positions(state, exchange, exchange_name)
            updates = await exchange.watch_positions()
            state = _STREAMS.get(exchange_name)
            if state is None:
                return
            # Кладём ДЕЛЬТЫ поверх снимка: contracts==0 означает, что
            # позиция закрыта — убираем её, иначе она "зависла" бы в
            # памяти навсегда и сверка считала бы её живой.
            positions = state.setdefault("positions", {})
            for p in updates or []:
                symbol = p.get("symbol")
                if not symbol:
                    continue
                if abs(p.get("contracts") or 0) > 0:
                    positions[symbol] = p
                else:
                    positions.pop(symbol, None)
            state["last_update"] = time.monotonic()
            failures = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failures += 1
            state = _STREAMS.get(exchange_name)
            if state is not None:
                # ПЕРЕСЕВ ОБЯЗАТЕЛЕН: пока соединения не было, на бирже
                # могло измениться что угодно, и накопленные дельты уже
                # не описывают реальность. Сбрасываем флаг — следующая
                # итерация возьмёт свежий полный снимок по REST.
                state["seeded"] = False
                if failures >= _MAX_CONSECUTIVE_FAILURES:
                    state["positions_gave_up"] = True
                    state["gave_up_at"] = time.monotonic()
                    print(
                        f"[private-stream] {exchange_name}: поток позиций не поднялся "
                        f"{failures} раз ({type(exc).__name__}) — остаюсь на REST."
                    )
                    return
            print(
                f"[private-stream] {exchange_name}: обрыв потока позиций "
                f"({type(exc).__name__}) — переподключаюсь ({failures}/{_MAX_CONSECUTIVE_FAILURES})."
            )
            await asyncio.sleep(2.0)


async def _orders_pump(exchange_name: str, exchange) -> None:
    failures = 0
    while exchange_name in _STREAMS:
        try:
            updates = await exchange.watch_orders()
            state = _STREAMS.get(exchange_name)
            if state is None:
                return
            orders = state.setdefault("orders", {})
            for order in updates or []:
                order_id = str(order.get("id") or "")
                if not order_id:
                    continue
                orders[order_id] = order
                # Будим тех, кто прямо сейчас ждёт именно этот ордер.
                waiter = state.get("waiters", {}).get(order_id)
                if waiter is not None and not waiter.is_set():
                    waiter.set()
            # Ограничиваем память: словари в Python сохраняют порядок
            # вставки, поэтому самые старые записи — первые ключи.
            while len(orders) > _MAX_REMEMBERED_ORDERS:
                orders.pop(next(iter(orders)))
            state["orders_last_update"] = time.monotonic()
            failures = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failures += 1
            state = _STREAMS.get(exchange_name)
            if failures >= _MAX_CONSECUTIVE_FAILURES:
                if state is not None:
                    state["orders_gave_up"] = True
                    state["gave_up_at"] = time.monotonic()
                print(
                    f"[private-stream] {exchange_name}: поток ордеров не поднялся "
                    f"{failures} раз ({type(exc).__name__}) — остаюсь на REST."
                )
                return
            print(
                f"[private-stream] {exchange_name}: обрыв потока ордеров "
                f"({type(exc).__name__}) — переподключаюсь ({failures}/{_MAX_CONSECUTIVE_FAILURES})."
            )
            await asyncio.sleep(2.0)


# =============================================================================
# ПУБЛИЧНОЕ API
# =============================================================================
async def subscribe(exchange_name: str, build_client) -> None:
    """Поднять приватные потоки для биржи. Идемпотентно.

    build_client — функция без аргументов, возвращающая УЖЕ настроенный
    ccxt.pro-клиент с боевыми ключами. Передаётся снаружи (из
    trade_tool.TradeExecutionTool), чтобы весь разбор учётных данных,
    passphrase, обход подписи Aster и demo-домены жили в ОДНОМ месте и не
    разъезжались с боевым путём ордеров."""
    if not _enabled() or exchange_name in _FAILED:
        return
    existing = _STREAMS.get(exchange_name)
    if existing is not None:
        gave_up_at = existing.get("gave_up_at")
        all_done = all(t.done() for t in existing.get("tasks", [])) if existing.get("tasks") else True
        if gave_up_at and all_done and time.monotonic() - gave_up_at > _RETRY_AFTER_GIVE_UP_SECONDS:
            print(f"[private-stream] {exchange_name}: прошло {_RETRY_AFTER_GIVE_UP_SECONDS:.0f}с после отказа — пробую подписаться снова.")
            await unsubscribe(exchange_name)
        else:
            return
    exchange_id = None
    try:
        exchange = build_client()
        exchange_id = getattr(exchange, "id", exchange_name)
        await _prepare_client(exchange, exchange_name)
        state = {
            "exchange": exchange,
            "positions": {},
            "orders": {},
            "waiters": {},
            "seeded": False,
            "tasks": [],
        }
        _STREAMS[exchange_name] = state

        started = []
        if _positions_enabled() and exchange.has.get("watchPositions"):
            task = asyncio.create_task(_positions_pump(exchange_name, exchange))
            state["positions_task"] = task
            state["tasks"].append(task)
            started.append("позиции")
        else:
            # Явно помечаем: этой биржей поток позиций не поддерживается
            # (реальный случай — MEXC), значит сверка обязана ходить по
            # REST, а не считать пустой словарь достоверным ответом.
            state["positions_gave_up"] = True
        if _orders_enabled() and exchange.has.get("watchOrders"):
            task = asyncio.create_task(_orders_pump(exchange_name, exchange))
            state["orders_task"] = task
            state["tasks"].append(task)
            started.append("ордера")
        else:
            state["orders_gave_up"] = True

        if not started:
            await unsubscribe(exchange_name)
            _FAILED.add(exchange_name)
            print(f"[private-stream] {exchange_name}: приватные потоки не поддерживаются — работаю по REST.")
            return
        print(f"[private-stream] {exchange_name}: подписался ({', '.join(started)}).")
    except Exception as exc:
        _STREAMS.pop(exchange_name, None)
        _FAILED.add(exchange_name)
        print(
            f"[private-stream] {exchange_name}: не удалось подписаться "
            f"({type(exc).__name__}: {exc}) — работаю по REST."
        )


async def unsubscribe(exchange_name: str) -> None:
    state = _STREAMS.pop(exchange_name, None)
    if not state:
        return
    for task in state.get("tasks", []):
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
    try:
        await state["exchange"].close()
    except Exception:
        pass


async def unsubscribe_all() -> None:
    for name in list(_STREAMS):
        await unsubscribe(name)


def positions_ready(exchange_name: str) -> bool:
    """Можно ли вообще опираться на поток позиций этой биржи СЕЙЧАС.

    Требуем именно засеянного (полный REST-снимок получен) и живого
    потока. Намеренно НЕ проверяем свежесть последнего сообщения: позиции
    законно молчат часами, когда ничего не происходит, и трактовать
    тишину как поломку значило бы сводить всю затею на нет."""
    state = _STREAMS.get(exchange_name)
    if not state or state.get("positions_gave_up") or not state.get("seeded"):
        return False
    # Живость проверяем по СВОЕЙ задаче, а не по «хоть одной из потоков».
    # Иначе умерший насос позиций маскировался бы живым насосом ордеров —
    # и сверка считала бы устаревший срез актуальным.
    task = state.get("positions_task")
    return task is not None and not task.done()


def get_positions(exchange_name: str) -> Optional[list]:
    """Текущий срез открытых позиций из потока, либо None — если потоку
    нельзя доверять и вызывающий обязан сходить по REST.

    НАПОМИНАНИЕ (см. границу безопасности вверху файла): полученный
    список годится только для вывода "всё как мы и думали". Любое
    расхождение перепроверяется по REST."""
    if not positions_ready(exchange_name):
        return None
    state = _STREAMS.get(exchange_name)
    return list((state.get("positions") or {}).values())


def get_order(exchange_name: str, order_id: str) -> Optional[dict]:
    state = _STREAMS.get(exchange_name)
    if not state or state.get("orders_gave_up"):
        return None
    return (state.get("orders") or {}).get(str(order_id))


def orders_ready(exchange_name: str) -> bool:
    state = _STREAMS.get(exchange_name)
    if not state or state.get("orders_gave_up"):
        return False
    task = state.get("orders_task")
    return task is not None and not task.done()


async def wait_for_fill(exchange_name: str, order_id: str, timeout: float = None) -> Optional[dict]:
    """Дождаться из потока события об исполнении ордера. Возвращает ордер
    (status=closed или filled>0) либо None, если за отведённое время
    события не было — тогда вызывающий идёт обычным REST-путём.

    Порядок проверок важен: СНАЧАЛА смотрим уже полученное. Исполнение
    часто приходит быстрее, чем мы успеваем сюда дойти (биржа пушит за
    десятки миллисекунд), и ожидание события, которое уже случилось,
    зависло бы ровно на весь таймаут."""
    if not orders_ready(exchange_name):
        return None
    order_id = str(order_id)
    state = _STREAMS.get(exchange_name)
    if state is None:
        return None

    def _filled(order):
        if not order:
            return None
        if order.get("status") == "closed" or (order.get("filled") or 0) > 0:
            return order
        return None

    already = _filled((state.get("orders") or {}).get(order_id))
    if already is not None:
        return already

    waiter = asyncio.Event()
    state.setdefault("waiters", {})[order_id] = waiter
    try:
        deadline = timeout if timeout is not None else _fill_wait_seconds()
        end = time.monotonic() + deadline
        while True:
            remaining = end - time.monotonic()
            if remaining <= 0:
                return None
            try:
                await asyncio.wait_for(waiter.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                return None
            order = _filled((state.get("orders") or {}).get(order_id))
            if order is not None:
                return order
            # Пришло обновление, но ордер ещё не исполнен (например,
            # "open" сразу после создания) — ждём следующего, не выходя:
            # именно ради этого цикл, а не одиночное ожидание.
            waiter.clear()
    finally:
        state.get("waiters", {}).pop(order_id, None)


def active_streams() -> list:
    out = []
    for name, state in _STREAMS.items():
        parts = []
        if positions_ready(name):
            parts.append(f"позиции:{len(state.get('positions') or {})}")
        if orders_ready(name):
            parts.append("ордера")
        out.append(f"{name}({', '.join(parts) or 'нет'})")
    return sorted(out)
