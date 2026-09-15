# =============================================================================
# price_stream.py — ПОТОКОВЫЕ ЛУЧШИЕ ЦЕНЫ (bid/ask) ПО «ГОРЯЧЕМУ СПИСКУ» МОНЕТ.
# =============================================================================
# Зачем (добавлено 2026-09-15 по прямой просьбе пользователя: "улучшить
# сканер и сделать вход по стакану, чтобы успевать на любую сделку"):
#
# REST-сканер опрашивает тикеры циклом (3–8с с сервера в Токио, 14с было с
# ноутбука) и сравнивает `last` — цену ПОСЛЕДНЕЙ СДЕЛКИ, а не то, по чему
# мы исполнимся. Отсюда две потери: спред, живущий несколько секунд, мы
# видим на излёте или не видим; и фантомные спреды тонких монет (BASECAT,
# BONER, INDEX — тикер +3…5%, стакан −20…+0.5%).
#
# Здесь биржа сама пушит лучшие bid/ask по монете при каждом изменении.
# Бот держит их в памяти, и на КАЖДОЕ обновление сканер пересчитывает
# исполнимый спред монеты между всеми парами бирж (bid там, где продаём,
# минус ask там, где покупаем) — см. scanner.py:_on_price_update. Порог
# пройден — кандидат уходит в СУЩЕСТВУЮЩУЮ цепочку входа (проверка глубины
# по стакану, история схождения, гейты перед ордером) немедленно, не
# дожидаясь цикла. Обнаружение: ~4с в среднем -> десятки миллисекунд.
#
# ПОЧЕМУ «ГОРЯЧИЙ СПИСОК», А НЕ ВСЁ. Замер на сервере 2026-09-15: подписка
# на всю вселенную (906 монет на >=2 биржах, ~4400 подписок) через ccxt.pro
# — ~480 сообщений/с и 100% одного ядра (разбор сообщений в ccxt ~1мс
# каждое, один поток); bitget отвергал пакеты по 50 подписок. Горячий список
# 150 монет × 3 биржи — 318 изменений/с и 78% ядра. Поэтому: подписываемся
# только на монеты, которые REST-сканер видел вблизи порога входа недавно
# (scanner.py:_note_hot_coins), пакетами по PRICE_STREAM_BATCH символов,
# не больше PRICE_STREAM_HOT_MAX_PER_EXCHANGE на биржу. Спред почти никогда
# не прыгает с нуля до порога мгновенно — он проходит порог−1% несколькими
# циклами раньше и попадает сюда до того, как пробьёт порог.
#
# ОБЛАСТЬ ДОВЕРИЯ: это данные ТОЛЬКО ДЛЯ ОБНАРУЖЕНИЯ. Ни один ордер не
# ставится по этим ценам — после триггера кандидат проходит те же четыре
# гейта, что и связка от REST-сканера. Ошибка здесь стоит одного запроса
# стакана, не денег. Fail-open: если поток биржи не поднялся — эта биржа
# просто не участвует в событийном обнаружении, REST-сканер её покрывает.
# =============================================================================
import asyncio
import os
import time
from typing import Callable, Optional

import ccxt.pro as ccxt_pro

# exchange_name -> {"exchange": client, "batches": {batch_id: {"symbols": set, "task": Task, "ok": bool}},
#                   "prices": {symbol: {"bid","ask","ts"}}, "ever_ok": bool, "gave_up": bool}
_STREAMS: dict = {}
_FAILED: set = set()
_ON_UPDATE: list = []  # callbacks(exchange_name, symbol)

_MAX_CONSECUTIVE_FAILURES = 3
_RECONNECT_BACKOFF_MAX = 60.0


def _enabled() -> bool:
    return os.getenv("PRICE_STREAM_ENABLED", "True").lower() == "true"


def _batch_size() -> int:
    try:
        return max(1, int(os.getenv("PRICE_STREAM_BATCH", "20")))
    except (TypeError, ValueError):
        return 20


def _max_age_seconds() -> float:
    """Возраст цены, после которого ей не верим. bid/ask приходят ТОЛЬКО при
    изменении — у тихой монеты законно нет обновлений минутами, поэтому
    порог заметно больше, чем у стакана открытой позиции; плюс отдельная
    проверка живости пакета (см. get)."""
    try:
        return float(os.getenv("PRICE_STREAM_MAX_AGE_SECONDS", "90"))
    except (TypeError, ValueError):
        return 90.0


def register_on_update(callback: Callable[[str, str], None]) -> None:
    if callback not in _ON_UPDATE:
        _ON_UPDATE.append(callback)


def _reconnect_delay(failures: int) -> float:
    return min(2.0 * (2 ** max(failures - 1, 0)), _RECONNECT_BACKOFF_MAX)


async def _pump(exchange_name: str, batch_id: int) -> None:
    state = _STREAMS.get(exchange_name)
    if state is None:
        return
    exchange = state["exchange"]
    failures = 0
    while True:
        state = _STREAMS.get(exchange_name)
        batch = state.get("batches", {}).get(batch_id) if state else None
        if batch is None:
            return
        symbols = sorted(batch["symbols"])
        if not symbols:
            return
        try:
            # mexc (свопы) отвечает NotSupported на watch_bids_asks, хотя
            # флаг has стоит — при первом таком ответе переключаем биржу на
            # watch_tickers навсегда (см. except ниже).
            if exchange.has.get("watchBidsAsks") and not state.get("use_tickers"):
                data = await exchange.watch_bids_asks(symbols)
            else:
                data = await exchange.watch_tickers(symbols)
            now = time.monotonic()
            batch["last_frame"] = now
            batch["ok"] = True
            state["ever_ok"] = True
            failures = 0
            prices = state.setdefault("prices", {})
            for sym, t in (data.items() if isinstance(data, dict) else []):
                if sym not in batch["symbols"]:
                    continue
                bid, ask = t.get("bid"), t.get("ask")
                if not bid or not ask:
                    continue
                prev = prices.get(sym)
                if prev and prev["bid"] == bid and prev["ask"] == ask:
                    continue
                prices[sym] = {"bid": bid, "ask": ask, "ts": now}
                for cb in _ON_UPDATE:
                    try:
                        cb(exchange_name, sym)
                    except Exception as exc:
                        print(f"[price-stream] callback упал: {type(exc).__name__}: {exc}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if type(exc).__name__ == "NotSupported" and not state.get("use_tickers"):
                state["use_tickers"] = True
                print(f"[price-stream] {exchange_name}: bid/ask потоком не поддерживается — переключаюсь на тикеры.")
                continue
            failures += 1
            batch["ok"] = False
            if failures >= _MAX_CONSECUTIVE_FAILURES and not state.get("ever_ok"):
                state["gave_up"] = True
                print(f"[price-stream] {exchange_name}: поток цен не поднялся {failures} раз ({type(exc).__name__}) — биржа не участвует в событийном обнаружении.")
                return
            delay = _reconnect_delay(failures)
            print(f"[price-stream] {exchange_name} пакет {batch_id}: обрыв ({type(exc).__name__}) — переподключаюсь через {delay:.0f}с.")
            await asyncio.sleep(delay)


async def sync(exchange_name: str, wanted_symbols: set, config: dict) -> None:
    """Привести подписки биржи к wanted_symbols. Добавление — в пакет со
    свободным местом (пакет перезапускается с новым списком); удаление —
    ЛЕНИВОЕ: символ остаётся в пакете до перезапуска этого пакета по другой
    причине, а пакет без нужных символов снимается. Так смена горячего
    списка не роняет все соединения биржи разом."""
    if not _enabled() or exchange_name in _FAILED:
        return
    state = _STREAMS.get(exchange_name)
    if state is None:
        if not wanted_symbols:
            return
        cls = getattr(ccxt_pro, config.get("_ccxt_id", exchange_name), None)
        if cls is None:
            _FAILED.add(exchange_name)
            return
        try:
            client_cfg = {k: v for k, v in config.items() if not k.startswith("_")}
            exchange = cls(client_cfg)
            if not (exchange.has.get("watchBidsAsks") or exchange.has.get("watchTickers")):
                _FAILED.add(exchange_name)
                print(f"[price-stream] {exchange_name}: нет потоковых bid/ask в ccxt.pro — только REST.")
                return
            state = {"exchange": exchange, "batches": {}, "prices": {}, "ever_ok": False, "gave_up": False, "next_id": 0}
            _STREAMS[exchange_name] = state
        except Exception as exc:
            _FAILED.add(exchange_name)
            print(f"[price-stream] {exchange_name}: не удалось создать клиент ({type(exc).__name__}: {exc}).")
            return
    if state.get("gave_up"):
        return

    # ТОЛЬКО СИМВОЛЫ, КОТОРЫЕ БИРЖА ЗНАЕТ: один чужой символ в пакете роняет
    # подписку всего пакета (BadSymbol) — проверено 2026-09-15 на тесте с
    # INDEX, которого нет на bybit. Рынки грузим один раз на клиент.
    exchange = state["exchange"]
    if not state.get("markets_loaded"):
        try:
            await exchange.load_markets()
            state["markets_loaded"] = True
        except Exception as exc:
            print(f"[price-stream] {exchange_name}: не удалось загрузить рынки ({type(exc).__name__}) — подписки отложены.")
            return
    unknown = {s for s in wanted_symbols if s not in exchange.markets}
    if unknown:
        wanted_symbols = wanted_symbols - unknown

    batches = state["batches"]
    have = set().union(*(b["symbols"] for b in batches.values())) if batches else set()
    to_add = sorted(wanted_symbols - have)
    size = _batch_size()

    # снять пакеты, где не осталось ни одного нужного символа
    for bid_, b in list(batches.items()):
        if not (b["symbols"] & wanted_symbols):
            t = b.get("task")
            if t:
                t.cancel()
            batches.pop(bid_, None)
            for sym in b["symbols"]:
                state["prices"].pop(sym, None)

    changed = set()
    for sym in to_add:
        target = None
        for bid_, b in batches.items():
            if len(b["symbols"]) < size:
                target = bid_
                break
        if target is None:
            target = state["next_id"]
            state["next_id"] += 1
            batches[target] = {"symbols": set(), "task": None, "ok": False, "last_frame": 0.0}
        batches[target]["symbols"].add(sym)
        changed.add(target)

    for bid_ in changed:
        b = batches[bid_]
        if b.get("task"):
            b["task"].cancel()
        b["task"] = asyncio.create_task(_pump(exchange_name, bid_))
    if to_add:
        print(f"[price-stream] {exchange_name}: +{len(to_add)} монет, всего подписано {sum(len(b['symbols']) for b in batches.values())} в {len(batches)} пакетах.")


def get(exchange_name: str, symbol: str) -> Optional[dict]:
    """Свежие bid/ask или None (нет данных / устарели / пакет мёртв)."""
    state = _STREAMS.get(exchange_name)
    if not state:
        return None
    p = state.get("prices", {}).get(symbol)
    if not p:
        return None
    if time.monotonic() - p["ts"] > _max_age_seconds():
        return None
    # живость пакета, в котором лежит символ: последний кадр не старше 120с
    for b in state.get("batches", {}).values():
        if symbol in b["symbols"]:
            if not b.get("ok") or time.monotonic() - b.get("last_frame", 0.0) > 120.0:
                return None
            break
    return p


def symbols_for(exchange_name: str) -> set:
    state = _STREAMS.get(exchange_name)
    if not state:
        return set()
    return set().union(*(b["symbols"] for b in state["batches"].values())) if state["batches"] else set()


def stats() -> dict:
    out = {}
    for name, st in _STREAMS.items():
        out[name] = {
            "symbols": sum(len(b["symbols"]) for b in st["batches"].values()),
            "batches": len(st["batches"]),
            "alive": sum(1 for b in st["batches"].values() if b.get("ok")),
            "gave_up": st.get("gave_up", False),
        }
    return out
