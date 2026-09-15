# =============================================================================
# book_stream.py — ПОТОКОВЫЕ (WebSocket) стаканы для УЖЕ ОТКРЫТЫХ позиций.
# =============================================================================
# Зачем (добавлено 2026-09-14 по прямой просьбе пользователя "есть ли
# улучшения, чтобы уменьшить задержки и улучшить отклик от бирж"):
#
# Мониторинг открытых позиций (scanner.py:_monitor_test_batch) КАЖДЫЙ цикл
# дёргал fetch_order_book по обеим ногам — это REST-запрос, медиана ~560мс
# по замерам [timing], и он же повторялся при финальной проверке прямо
# перед закрытием. То есть:
#   1) решение "спред сошёлся, пора закрывать" принималось по данным,
#      которым уже полсекунды;
#   2) сам цикл мониторинга не мог крутиться быстрее, чем отвечает самая
#      медленная биржа (gate сегодня — до 950мс на запрос);
#   3) мы непрерывно долбили API бирж, что само по себе замедляет ответы и
#      приближает rate limit.
#
# ccxt.pro (уже установлен в составе ccxt, отдельная лицензия не нужна)
# умеет watch_order_book: биржа сама ШЛЁТ обновления стакана по WebSocket,
# библиотека держит актуальную книгу в памяти. Чтение из памяти — 0мс.
#
# ОБЛАСТЬ ПРИМЕНЕНИЯ СОЗНАТЕЛЬНО ОГРАНИЧЕНА открытыми позициями (максимум
# 2 символа на позицию): для ПОИСКА связок подписка не годится — мы заранее
# не знаем, какие монеты понадобятся, а держать сотни подписок на все рынки
# всех бирж — это другой класс задачи. Зато путь ЗАКРЫТИЯ (самый ценный:
# там ловится момент схождения спреда) получает реальное время.
#
# БЕЗОПАСНОСТЬ: это ТОЛЬКО ускорение чтения. Если поток не успел
# подключиться, отдал пустую книгу или отвалился — get_book() возвращает
# None, и вызывающий код честно уходит на обычный REST-запрос (fail-open).
# Никакой торговой логики здесь нет и быть не должно.
# =============================================================================
import asyncio
import os
import time
from collections import deque
from typing import Optional

import ccxt.pro as ccxt_pro

# (exchange_id, symbol) -> {"task": Task, "exchange": client, "book": dict, "ts": monotonic}
_STREAMS: dict = {}

# exchange_id -> monotonic, до которого НЕ пытаемся подписываться на этой
# бирже вовсе. Ставится, когда поток по любой паре биржи сдался после
# _MAX_CONSECUTIVE_FAILURES попыток. Смысл появился вместе с подписками на
# КАНДИДАТОВ (scanner.py:_note_book_candidates, 2026-09-14): пары приходят
# и уходят каждые несколько минут, и без этой паузы MEXC — единственная
# биржа, которая фьючерсный стакан по WebSocket не отдаёт, — на каждого
# нового кандидата снова тратила бы ~6с попыток и три строки в лог.
_EXCHANGE_COOLOFF_UNTIL: dict = {}
_EXCHANGE_COOLOFF_SECONDS = 1800.0


def _enabled() -> bool:
    return os.getenv("BOOK_STREAM_ENABLED", "True").lower() == "true"


def _max_age_seconds() -> float:
    """Насколько свежей должна быть последняя пришедшая книга, чтобы ей
    доверять. Поток шлёт обновления постоянно (обычно десятки раз в
    секунду), поэтому если данных нет дольше этого — соединение, скорее
    всего, молча умерло, и лучше уйти на REST, чем торговать по старому."""
    try:
        return float(os.getenv("BOOK_STREAM_MAX_AGE_SECONDS", "5.0"))
    except (TypeError, ValueError):
        return 5.0


# Сколько подряд неудачных попыток терпим, прежде чем признать, что эта
# биржа потоковый стакан не отдаёт, и тихо остаться на REST. Замер
# 2026-09-14: bitget/gate/bybit/binance подключаются за 2-10с, а mexc
# (фьючерсы) не отвечает вообще ни на одной глубине — без этого предела он
# бы вечно переподключался и засорял лог каждые 2 секунды.
_MAX_CONSECUTIVE_FAILURES = 3


async def _pump(key, exchange, symbol: str) -> None:
    """Бесконечно принимает обновления стакана и складывает последнее в
    _STREAMS. Обрыв WebSocket — норма, переподключаемся. Но если поток не
    поднимается подряд _MAX_CONSECUTIVE_FAILURES раз — сдаёмся молча:
    вызывающий код и так работает по REST, а бесконечные попытки только
    жгут лог и соединения.

    ВАЖНО про limit: НЕ передаём его вообще. Замер 2026-09-14 показал, что
    limit=50 отвергается bitget ("Param error", code 30016 — у неё каналы
    books5/books15/books, полусотни нет), а без параметра все биржи отдают
    свою естественную глубину (bitget 196, gate 100, bybit 50, binance
    700+) — этого с запасом хватает для VWAP на наши $25."""
    failures = 0
    while key in _STREAMS:
        try:
            book = await exchange.watch_order_book(symbol)
            entry = _STREAMS.get(key)
            if entry is None:
                break
            entry["book"] = book
            entry["ts"] = time.monotonic()
            entry["ever_ok"] = True
            # ИСТОРИЯ СЕРЕДИНЫ СТАКАНА (добавлено 2026-09-15 после MTL): каждое
            # обновление — точка (время, mid). По ней recent_move_pct() за 0мс
            # отвечает, насколько цена дёргалась в последние секунды — без
            # единого сетевого запроса и без ожидания. Глубина 600 точек с
            # запасом покрывает 10-15с даже при десятках обновлений в секунду.
            try:
                bids, asks = book.get("bids"), book.get("asks")
                if bids and asks and bids[0] and asks[0]:
                    entry.setdefault("mids", deque(maxlen=600)).append(
                        (entry["ts"], (bids[0][0] + asks[0][0]) / 2.0)
                    )
            except Exception:
                pass
            failures = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failures += 1
            entry = _STREAMS.get(key)
            ever_ok = bool(entry and entry.get("ever_ok"))
            # То же правило, что в private_stream: сдаёмся только если поток
            # НИ РАЗУ не работал (биржа не отдаёт стакан по WS — mexc).
            # Работавший поток при обрыве сети переподключается бесконечно
            # с растущей паузой — иначе одно моргание Wi-Fi лишало монитор
            # потокового стакана на 30 минут.
            if failures >= _MAX_CONSECUTIVE_FAILURES and not ever_ok:
                print(
                    f"[book-stream] {key[0]}/{symbol}: поток не поднялся {failures} раз "
                    f"({type(exc).__name__}) — остаюсь на REST для этой пары."
                )
                if entry is not None:
                    entry["gave_up"] = True
                _EXCHANGE_COOLOFF_UNTIL[key[0]] = time.monotonic() + _EXCHANGE_COOLOFF_SECONDS
                return
            delay = min(2.0 * (2 ** max(failures - 1, 0)), 60.0)
            print(f"[book-stream] {key[0]}/{symbol}: обрыв потока ({type(exc).__name__}) — переподключаюсь через {delay:.0f}с (попытка {failures}).")
            await asyncio.sleep(delay)


async def subscribe(exchange_id: str, symbol: str, config: dict) -> None:
    """Подписаться на стакан. Идемпотентно — повторный вызов по той же паре
    ничего не делает. config — тот же словарь параметров клиента, что и у
    REST-клиента бота (ключи не нужны для публичного стакана, но сеть/
    таймауты/опции должны совпадать)."""
    if not _enabled():
        return
    key = (exchange_id, symbol)
    if key in _STREAMS:
        return
    if time.monotonic() < _EXCHANGE_COOLOFF_UNTIL.get(exchange_id, 0.0):
        return  # биржа недавно доказала, что поток не отдаёт — тихо остаёмся на REST
    cls = getattr(ccxt_pro, exchange_id, None)
    if cls is None:
        return  # биржа не поддерживается ccxt.pro — молча работаем по REST
    try:
        exchange = cls(config)
        _STREAMS[key] = {"exchange": exchange, "book": None, "ts": 0.0, "task": None}
        _STREAMS[key]["task"] = asyncio.create_task(_pump(key, exchange, symbol))
        print(f"[book-stream] подписался на {exchange_id}/{symbol}")
    except Exception as exc:
        _STREAMS.pop(key, None)
        print(f"[book-stream] не удалось подписаться на {exchange_id}/{symbol}: {type(exc).__name__}: {exc}")


async def unsubscribe(exchange_id: str, symbol: str) -> None:
    """Отписаться (позиция закрыта — поток больше не нужен)."""
    entry = _STREAMS.pop((exchange_id, symbol), None)
    if not entry:
        return
    task = entry.get("task")
    if task:
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
    try:
        await entry["exchange"].close()
    except Exception:
        pass
    print(f"[book-stream] отписался от {exchange_id}/{symbol}")


def get_book(exchange_id: str, symbol: str) -> Optional[dict]:
    """Последний стакан из потока или None, если потока нет / он молчит
    дольше допустимого / книга пустая. None = вызывающий код идёт по REST."""
    entry = _STREAMS.get((exchange_id, symbol))
    if not entry:
        return None
    book = entry.get("book")
    if not book or not book.get("bids") or not book.get("asks"):
        return None
    if time.monotonic() - entry.get("ts", 0.0) > _max_age_seconds():
        return None
    return book


def recent_move_pct(exchange_id: str, symbol: str, window_seconds: float = 10.0) -> Optional[float]:
    """Размах середины стакана за последние window_seconds, в процентах от
    последней цены: (max − min) / last × 100. None — если истории нет или
    точек меньше двух (поток только поднялся) — вызывающий код трактует
    None как «нет данных», а не как «спокойно».

    Зачем (реальный случай MTL 2026-09-15): оценка PnL перед закрытием была
    верна для стакана, которому были доли секунды, но за секунду между
    решением и исполнением bitget провалился на 2% (обвал на выплате
    funding в 00:00 UTC), и нога закрылась на дне фитиля: ожидали +0.25,
    получили −0.28. Фильтровать это задержкой (ждать подтверждения)
    пользователь запретил — «1.5 секунды это очень много». Взгляд НАЗАД
    на уже полученные обновления стоит 0мс и ловит ту же бурю: если цена
    прошла процент за десять секунд, сейчас не время ни входить, ни
    выходить по прибыли."""
    entry = _STREAMS.get((exchange_id, symbol))
    if not entry:
        return None
    mids = entry.get("mids")
    if not mids or len(mids) < 2:
        return None
    cutoff = time.monotonic() - window_seconds
    window = [m for ts, m in mids if ts >= cutoff]
    if len(window) < 2:
        return None
    last = window[-1]
    if not last:
        return None
    return (max(window) - min(window)) / last * 100.0


def active_streams() -> list:
    return sorted(f"{ex}/{sym}" for ex, sym in _STREAMS)
