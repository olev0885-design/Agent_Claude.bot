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
