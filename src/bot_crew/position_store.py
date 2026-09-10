# =============================================================================
# position_store.py — учёт ОТКРЫТЫХ позиций бота между сигналом "открыть"
# (Spread-сигнал) и сигналом "закрыть" ("aligned in").
# =============================================================================
# Зачем это нужно: канал присылает сигнал на открытие и сигнал на закрытие
# ОТДЕЛЬНЫМИ сообщениями, разнесёнными по времени (см. main.py). Чтобы при
# получении "#TMX aligned in ..." бот знал, ЧТО именно закрывать (какие
# биржи, какой объём в монете на каждой ноге, по какой цене вошли — для
# расчёта PnL), эти данные нужно где-то хранить между сообщениями.
#
# Хранилище — простой JSON-файл в корне проекта (open_positions.json,
# добавлен в .gitignore). Этого достаточно для одного работающего процесса
# бота; если процесс перезапустят — данные не потеряются (в отличие от
# хранения только в памяти).
# =============================================================================
import json
import os
from datetime import datetime, timezone
from typing import Optional

_STORE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "open_positions.json",
)


def _load() -> dict:
    if not os.path.exists(_STORE_PATH):
        return {}
    try:
        with open(_STORE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        # Повреждённый/пустой файл — не роняем бота, просто считаем, что
        # открытых позиций сейчас не знаем (пусто).
        return {}


def _save(positions: dict) -> None:
    with open(_STORE_PATH, "w", encoding="utf-8") as f:
        json.dump(positions, f, ensure_ascii=False, indent=2)


def record_open(coin: str, position: dict) -> None:
    """Сохраняет данные ОТКРЫТОЙ позиции по монете (перезаписывает, если
    по этой монете уже что-то было записано — актуальным считается
    последний открытый спред)."""
    positions = _load()
    position = dict(position)
    position["opened_at"] = datetime.now(timezone.utc).isoformat()
    positions[coin.upper()] = position
    _save(positions)


def get_position(coin: str) -> Optional[dict]:
    """Возвращает данные открытой позиции по монете или None, если сейчас
    по ней ничего не открыто (бот её не отслеживает)."""
    return _load().get(coin.upper())


def pop_position(coin: str) -> Optional[dict]:
    """Достаёт данные позиции по монете и УДАЛЯЕТ её из хранилища (вызывать
    сразу после успешного закрытия, чтобы повторный "aligned in" по той же
    монете не пытался закрыть уже закрытую позицию)."""
    positions = _load()
    position = positions.pop(coin.upper(), None)
    if position is not None:
        _save(positions)
    return position


def list_positions() -> dict:
    """Все текущие открытые позиции — полезно для отчёта/отладки."""
    return _load()


def busy_exchanges() -> set:
    """Множество бирж (имена в нижнем регистре), на которых ПРЯМО СЕЙЧАС
    есть открытая нога хотя бы одной позиции — по явной просьбе
    пользователя 2026-09-07: "не больше 1 открытого ордера на бирже,
    допустим на mexc и gate открыты по ноге — не открываем новую ногу
    пока эта не закроется, но можно открывать на других биржах". Снижает
    риск каскада — одна плохая сделка на бирже не делит margin-пул с
    другой открытой позицией на ТОЙ ЖЕ бирже под cross margin (реальный
    инцидент 2026-09-06: BONER утащил за собой здоровый BP на Gate).
    Используется ОБОИМИ путями входа (сканер и сигналы канала) как единый
    источник правды — см. вызовы в scanner.py/main.py."""
    exchanges: set = set()
    for position in list_positions().values():
        long_ex = position.get("long_exchange")
        short_ex = position.get("short_exchange")
        if long_ex:
            exchanges.add(long_ex.lower())
        if short_ex:
            exchanges.add(short_ex.lower())
    return exchanges
