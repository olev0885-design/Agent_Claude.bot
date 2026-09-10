# =============================================================================
# skyspreads_signals.py — ВТОРОЙ источник сигналов, помимо собственного
# сканера (scanner.py) и основного Telegram-канала (main.py:open_signal).
# По прямой просьбе пользователя 2026-09-09: у него есть доступ к каналу
# внешнего сервиса SkySpreads.net (сканер спредов на 18+ биржах), сообщения
# оттуда приходят в текстовом виде, детерминированного формата — разбираем
# их регулярками (LLM не нужен, формат стабильный, в отличие от исходного
# "полу-хаотичного" канала, для которого и создавался BotCrew.parse_signal).
#
# Пример реального сообщения (прислано пользователем 2026-09-09):
#   🔥 Futures Spread 10.26%
#   🟦 #H100
#   📌 H100USDT
#
#   🟢 Long  BITGET  | $2.6670 | +0.0000% | 📦$267K | $71K
#   🔴 Short LIGHTER | $2.9405 | -0.0478% | 📦? | $110K
#
#   📤 BITGET: D:❌ W:❌ | LIGHTER: NO SPOT
#
#   📊 All Exchanges
#   BITGET   |       2.6670 | +0.0000% | 📦$267K | $71K | D:❌ W:❌
#   ...
#
# ВАЖНО: SkySpreads мониторит 18+ бирж, многие из которых (LIGHTER — DEX,
# XT, BITUNIX, OKX, BINANCE, OURBIT, TOOBIT, PHEMEX, BINGX, PIONEX, ...) мы
# НЕ поддерживаем (см. _EXCHANGE_NAME_MAP ниже — только те 6, что реально
# настроены в trade_tool.py). Сигналы с чужими биржами тихо пропускаются —
# это НЕ ошибка, а норма (большинство сигналов канала не наши).
# =============================================================================
import os
import re

from bot_crew import trade_executor
from bot_crew import position_store
from bot_crew import blocked_coins_store

# Только те биржи, которыми бот реально умеет торговать (см. trade_tool.py).
# SkySpreads иногда сокращает "hyperliquid" как "hliquid" — оба варианта
# сюда, на всякий случай.
_EXCHANGE_NAME_MAP = {
    "gate": "gate",
    "mexc": "mexc",
    "bitget": "bitget",
    "aster": "aster",
    "hyperliquid": "hyperliquid",
    "hliquid": "hyperliquid",
    "bybit": "bybit",
    # Добавлена 2026-09-09 после пополнения фьючерсного кошелька KuCoin
    # ($8 USDT) — см. .env. Канал называет её "KUCOIN" (видел в живом
    # трафике, пример WAVES: "Long BYBIT | Short COINEX" рядом с KUCOIN
    # в "All Exchanges") — совпадает с exchange_name, который ждёт
    # trade_tool.py (алиас kucoin -> kucoinfutures там же).
    "kucoin": "kucoin",
    # Добавлена 2026-09-09 после пополнения фьючерсного кошелька Binance
    # ($8 USDT) — см. .env. Канал называет её "BINANCE".
    "binance": "binance",
}

_SPREAD_TYPE_RE = re.compile(
    r"(Futures\s*↔\s*Spot|Futures/Spot|Futures|Spot|Funding|Fair|DEX)\s+Spread\s+([\d.]+)\s*%",
    re.IGNORECASE,
)
_COIN_HASHTAG_RE = re.compile(r"#([A-Za-z0-9]{2,20})")
_LONG_RE = re.compile(r"Long\s+(\S+)\s*\|\s*\$?([\d,]+\.?\d*)", re.IGNORECASE)
_SHORT_RE = re.compile(r"Short\s+(\S+)\s*\|\s*\$?([\d,]+\.?\d*)", re.IGNORECASE)


def _normalize_exchange(name: str):
    return _EXCHANGE_NAME_MAP.get((name or "").strip().lower())


def classify_skyspreads_signal(text: str) -> str:
    """Возвращает 'OPEN' или 'UNKNOWN'. У SkySpreads нет отдельных
    CLOSE-сообщений (в отличие от "aligned in" исходного канала) — закрытие
    открытых через этот источник позиций делает наш собственный
    _monitor_test_batch в scanner.py, он не зависит от того, откуда позиция
    была открыта (см. position_store — источник-агностичен).

    ВАЖНО (см. живые примеры канала 2026-09-09): помимо "Futures Spread"
    канал шлёт ещё "DEX Spread" (CEX↔DEX, формат "CEX X|$.. / DEX Y|$..",
    БЕЗ Long/Short — этот тип и так не пройдёт регулярку ниже), новости
    (📰), алерты резкого движения цены (🔴 COIN | Exchange, Change: N% in
    Xm — тоже без Long/Short) и "✅ Aligned" (закрытие DEX-пары). Мы торгуем
    ТОЛЬКО фьючерсами/перпетуалами — если в будущем у "Spot Spread" или
    "Funding Spread" тоже появится Long/Short-разметка, это НЕ должно
    считаться нашим сигналом (спот мы не торгуем) — поэтому проверяем
    именно spread_type == 'futures', а не просто наличие Long/Short."""
    if not text:
        return "UNKNOWN"
    type_match = _SPREAD_TYPE_RE.search(text)
    if not type_match or type_match.group(1).strip().lower() != "futures":
        return "UNKNOWN"
    if _LONG_RE.search(text) and _SHORT_RE.search(text):
        return "OPEN"
    return "UNKNOWN"


def parse_skyspreads_signal(text: str) -> dict:
    """Разбирает сообщение регулярками (без LLM — формат SkySpreads
    стабильный и полностью структурированный, в отличие от исходного
    канала). Возвращает dict с сырыми (ещё НЕ нормализованными к нашим
    именам бирж) полями — нормализация и все проверки входа делаются в
    open_skyspreads_signal()."""
    type_match = _SPREAD_TYPE_RE.search(text or "")
    coin_match = _COIN_HASHTAG_RE.search(text or "")
    long_match = _LONG_RE.search(text or "")
    short_match = _SHORT_RE.search(text or "")

    return {
        "spread_type": type_match.group(1) if type_match else None,
        "spread_percent": float(type_match.group(2)) if type_match else None,
        "coin": coin_match.group(1).upper() if coin_match else None,
        "long_exchange_raw": long_match.group(1) if long_match else None,
        "long_price": float(long_match.group(2).replace(",", "")) if long_match else None,
        "short_exchange_raw": short_match.group(1) if short_match else None,
        "short_price": float(short_match.group(2).replace(",", "")) if short_match else None,
    }


def open_skyspreads_signal(raw_text: str, notifier=None):
    """Разбирает и открывает сделку по сигналу SkySpreads — те же гейты
    входа, что и у open_signal() в main.py (единообразно, независимо от
    источника связки: excluded_coins, автоблокировка, лимит 1 нога на
    биржу, лимит открытых позиций, порог спреда), ПЛЮС дополнительная
    проверка: обе биржи сигнала должны входить в реально настроенные
    (_EXCHANGE_NAME_MAP) — SkySpreads мониторит 18+ бирж, мы торгуем на 6.

    Возвращает str-отчёт при попытке открытия, либо None — если сигнал
    заведомо не наш (неподдерживаемая биржа с любой стороны) и НЕ должен
    засорять уведомления пользователя молчаливым "пропущено"."""
    parsed = parse_skyspreads_signal(raw_text)
    coin = parsed.get("coin")
    spread_percent = parsed.get("spread_percent")
    spread_type = (parsed.get("spread_type") or "").strip().lower()

    if spread_type != "futures":
        # Мы торгуем ТОЛЬКО фьючерсами/перпетуалами — Spot/Funding/Fair/DEX
        # спреды (даже если когда-нибудь обзаведутся Long/Short-разметкой)
        # не наш формат исполнения, см. classify_skyspreads_signal.
        return None

    if not coin or not parsed.get("long_exchange_raw") or not parsed.get("short_exchange_raw"):
        return None  # не распарсилось — не наш формат, тихо пропускаем

    long_exchange = _normalize_exchange(parsed["long_exchange_raw"])
    short_exchange = _normalize_exchange(parsed["short_exchange_raw"])
    if not long_exchange or not short_exchange:
        # Одна (или обе) биржи не из нашего списка — это НОРМА (большинство
        # сигналов SkySpreads не про наши 6 бирж), не ошибка. Печатаем в
        # консоль для видимости, но НЕ шлём пользователю уведомление —
        # иначе он получит спам на каждый "чужой" сигнал.
        print(
            f"[skyspreads] #{coin}: биржа(и) не поддерживаются ботом "
            f"({parsed['long_exchange_raw']}/{parsed['short_exchange_raw']}) — сигнал пропущен."
        )
        return None

    # Явный маркер "мусорного" совпадения тикеров (тот же класс проблемы,
    # что и SCANNER_MAX_SANE_SPREAD_PERCENT в scanner.py) — SkySpreads
    # мониторит спот+фьючерсы+DEX сразу, там регулярно бывают спреды в
    # сотни процентов из-за разных активов под одним тикером на разных
    # биржах, а не реальный арбитраж.
    max_sane = float(os.getenv("SCANNER_MAX_SANE_SPREAD_PERCENT", "50.0"))
    if spread_percent is not None and spread_percent > max_sane:
        print(
            f"[skyspreads] #{coin}: спред {spread_percent}% выше разумного "
            f"порога {max_sane}% — похоже на несовпадение тикеров, сигнал пропущен."
        )
        return None

    excluded_coins = {
        name.strip().upper()
        for name in os.getenv("SCANNER_EXCLUDED_COINS", "").split(",")
        if name.strip()
    }
    if coin.upper() in excluded_coins:
        return f"[SkySpreads] Монета #{coin} в списке исключённых (SCANNER_EXCLUDED_COINS) — сигнал пропущен."

    for bad_exchange in (long_exchange, short_exchange):
        if blocked_coins_store.is_coin_exchange_excluded(coin, bad_exchange):
            return (
                f"[SkySpreads] Сигнал #{coin}: монета исключена именно для биржи "
                f"{bad_exchange} (SCANNER_EXCLUDED_COIN_EXCHANGE_PAIRS) — сигнал пропущен."
            )

    if blocked_coins_store.is_blocked(coin):
        info = blocked_coins_store.get_block_info(coin) or {}
        return (
            f"[SkySpreads] Монета #{coin} АВТОМАТИЧЕСКИ ЗАБЛОКИРОВАНА после повторных "
            f"реальных откатов (последняя причина: {(info.get('reasons') or ['?'])[-1]}) — "
            f"сигнал пропущен до ручной разблокировки."
        )

    min_spread = float(os.getenv("TEST_BATCH_MIN_SPREAD", os.getenv("AUTO_TRADE_MIN_SPREAD", "3.0")))
    if spread_percent is not None and spread_percent < min_spread:
        return (
            f"[SkySpreads] Сигнал #{coin}: спред {spread_percent}% ниже порога входа "
            f"{min_spread}% — сделка не открывается."
        )

    busy = position_store.busy_exchanges()
    if long_exchange in busy or short_exchange in busy:
        return (
            f"[SkySpreads] Сигнал #{coin}: на бирже "
            f"{long_exchange if long_exchange in busy else short_exchange} уже есть "
            f"открытая нога другой позиции (лимит — 1 нога на биржу) — сигнал пропущен."
        )

    if len(position_store.list_positions()) >= 2:
        return (
            f"[SkySpreads] Сигнал #{coin}: уже есть 2 открытые позиции — сигнал пропущен."
        )

    return trade_executor.open_structured_signal(
        coin, long_exchange, short_exchange,
        spread_percent=spread_percent,
        notifier=notifier,
    )
