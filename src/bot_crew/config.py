# =============================================================================
# config.py — типизированное чтение конфигурации авто-торговли сканера
# (scanner.py) из переменных окружения (.env). Вынесено из scanner.py в
# отдельный модуль, чтобы конфигурация была одним понятным местом, а не
# разбросанными по коду os.getenv(...) вызовами.
# =============================================================================
import os
from dataclasses import dataclass


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


@dataclass(frozen=True)
class AutoTradeConfig:
    """Настройки авто-торговли сканера (scanner.py) — читаются ОДИН РАЗ
    при создании FundingScanner; чтобы применить изменения из .env, бота
    нужно перезапустить (как и остальные настройки проекта)."""

    # Главный выключатель — без него сканер только шлёт алерты, ничего не
    # открывает сам, даже если остальные поля ниже заданы.
    enabled: bool
    # Размер ОДНОЙ ноги сделки в USDT для авто-найденных связок — ОТДЕЛЬНЫЙ
    # от TRADE_SIZE_USDT (тот используется для сигналов Telegram-канала).
    # Разделены сознательно: сканер сам находит связки без внешней курации
    # канала, разумно держать под это отдельный (обычно меньший) лимит.
    amount_usdt: float
    # Минимальный спред (%), при котором сканер РЕАЛЬНО открывает сделку —
    # ОТДЕЛЬНО от SCANNER_SPREAD_THRESHOLD_PERCENT (порог для алерта).
    # Обычно имеет смысл держать выше порога алерта: видеть в Telegram
    # больше находок, чем реально торговать.
    min_spread_percent: float
    # Максимум ОДНОВРЕМЕННО открытых позиций (всего, не только у сканера —
    # считаются и позиции, открытые сигналами канала) — простой лимит на
    # общий риск бота.
    max_open_positions: int


def load_auto_trade_config() -> AutoTradeConfig:
    """Читает AUTO_TRADE_* / MAX_OPEN_POSITIONS из окружения. Вызывать
    ПОСЛЕ load_dotenv() (см. main.py:main())."""
    return AutoTradeConfig(
        enabled=_get_bool_env("AUTO_TRADE_ENABLED", False),
        amount_usdt=_get_float_env("AUTO_TRADE_AMOUNT_USDT", 6.0),
        min_spread_percent=_get_float_env("AUTO_TRADE_MIN_SPREAD", 3.0),
        max_open_positions=_get_int_env("MAX_OPEN_POSITIONS", 5),
    )


@dataclass(frozen=True)
class TestBatchConfig:
    """Настройки режима "тестовой серии" (см. test_batch.py, scanner.py) —
    сканер открывает РОВНО `size` сделок фиксированным объёмом, затем
    переключается на мониторинг/закрытие ТОЛЬКО этих позиций (новые связки
    не ищет), пока не закроет все — после чего шлёт сводный отчёт."""

    # Главный выключатель. Независим от AUTO_TRADE_ENABLED — тестовая серия
    # сама управляет входом/выходом по своим собственным порогам ниже, не
    # полагаясь на общие настройки авто-торговли.
    enabled: bool
    # Сколько сделок должно быть открыто РОВНО за эту серию.
    size: int
    # Размер ОДНОЙ ноги в USDT для тестовых сделок серии.
    amount_usdt: float
    # Минимальный спред (%) входа — вход только при spread >= min_spread_pct.
    min_spread_pct: float
    # Диапазон спреда (%) для закрытия — закрываем, когда ТЕКУЩИЙ спред
    # позиции попадает В ЭТОТ диапазон (не обязательно полностью сошёлся
    # к close_spread_pct, достаточно быть "не дальше" close_spread_max_pct)
    # И суммарный PnL уже положительный (см. _monitor_test_batch в
    # scanner.py) — по явному уточнению пользователя 2026-09-06: "не
    # обязательно сходиться именно на close_spread_pct, главное чтобы
    # выигрышная нога перекрывала проигрышную". close_spread_pct остаётся
    # нижней границей диапазона (обычно уже недостижим для крупных
    # спредов входа вроде 13-18%, но полезен как знак "почти идеально
    # сошлось").
    close_spread_pct: float
    close_spread_max_pct: float
    # Пауза (сек) между проверками цен в РЕЖИМЕ МОНИТОРИНГА — короче
    # обычного SCANNER_INTERVAL_SECONDS, чтобы закрытие по цели не
    # запаздывало (пока идёт поиск новых связок, используется обычный
    # интервал сканера — до заполнения серии мониторить особо нечего).
    monitor_interval_seconds: float
    # Биржи, которым разрешено участвовать в тестовой серии — НЕЗАВИСИМЫЙ
    # список от DEMO_TRADING_SUPPORTED (trade_tool.py): тот описывает, для
    # каких бирж CCXT умеет КОДОМ переключить обычный ключ в demo-режим
    # (bybit/bitget/gate). Здесь же — какие ключи вы САМИ подтвердили как
    # заведомо виртуальные (например, отдельный demo-аккаунт MEXC — CCXT
    # не умеет туда переключать программно, но если ключ УЖЕ привязан к
    # demo-счёту биржи, переключать и не нужно, обычный вызов API и так
    # уйдёт на виртуальные деньги). По умолчанию — то же, что
    # DEMO_TRADING_SUPPORTED; добавляйте биржу сюда, только когда сами
    # проверили, что связанный с ней ключ ведёт на виртуальный счёт.
    safe_exchanges: frozenset


def load_test_batch_config() -> TestBatchConfig:
    """Читает TEST_BATCH_* из окружения. Вызывать ПОСЛЕ load_dotenv()."""
    return TestBatchConfig(
        enabled=_get_bool_env("TEST_BATCH_MODE", False),
        size=_get_int_env("TEST_BATCH_SIZE", 15),
        amount_usdt=_get_float_env("TEST_BATCH_AMOUNT_USDT", 10.0),
        min_spread_pct=_get_float_env("TEST_BATCH_MIN_SPREAD", 5.0),
        close_spread_pct=_get_float_env("TEST_BATCH_CLOSE_SPREAD", 0.2),
        close_spread_max_pct=_get_float_env("TEST_BATCH_CLOSE_SPREAD_MAX", 1.0),
        monitor_interval_seconds=_get_float_env("TEST_BATCH_MONITOR_INTERVAL_SECONDS", 10.0),
        safe_exchanges=frozenset(
            name.strip().lower()
            for name in os.getenv("TEST_BATCH_SAFE_EXCHANGES", "bybit,bitget,gate").split(",")
            if name.strip()
        ),
    )
