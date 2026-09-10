# =============================================================================
# blocked_coins_store.py — АВТОМАТИЧЕСКАЯ блокировка монет после нескольких
# неудачных попыток открытия (одна нога открылась, вторая — нет, сработал
# откат) — по явной просьбе пользователя 2026-09-09: "если монету не
# получается купить 1-3 раза, то не нужно ей спамить. Просто выведи мне
# ошибку и пока я не решу саму проблему — не давай ордерам открываться."
#
# Реальный повод: 4STOCK открывался и откатывался на Aster 12 РАЗ ПОДРЯД
# (реальные деньги на комиссиях/проскальзывании каждый раз) из-за
# постоянной ошибки MEXC "Contract not activated" на другой ноге — бот
# продолжал пытаться снова и снова, не "запоминая" исход.
#
# В ОТЛИЧИЕ от SCANNER_EXCLUDED_COINS (ручной список в .env, требует
# рестарта) — это АВТОМАТИЧЕСКАЯ, персистентная (переживает рестарт)
# блокировка по ФАКТУ повторных РЕАЛЬНЫХ откатов (не путать с VWAP
# pre-check отменами — те НЕ тратят денег и НЕ считаются здесь, см.
# record_rollback_failure вызывается только из ветки rollback в
# trade_tool.py, не из cancelled-веток). Хранится в JSON-файле (как
# position_store.py) — блокировка снимается ТОЛЬКО вручную (clear_block),
# когда пользователь решил исходную проблему.
# =============================================================================
import json
import os
from datetime import datetime, timezone
from typing import Optional

_STORE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "blocked_coins.json",
)
_ENV_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    ".env",
)

# После скольких РЕАЛЬНЫХ откатов подряд монета блокируется автоматически.
FAILURE_THRESHOLD = int(os.getenv("BLOCKED_COINS_FAILURE_THRESHOLD", "2"))

# Некоторые ошибки биржи ЗАВЕДОМО не исправятся повторной попыткой — это не
# временный сбой сети/ликвидности, а структурное ограничение конкретного
# аккаунта под конкретный контракт (например, "Contract not activated" на
# MEXC — контракт просто не включён для торговли на этом аккаунте). По
# прямой просьбе пользователя 2026-09-09: "если выскакивает проблема
# contract not activated - то можешь засовывать монету в исключения" —
# для таких ошибок монета уходит в постоянный список SCANNER_EXCLUDED_COINS
# (.env) сразу после ПЕРВОГО отката с такой причиной, не дожидаясь
# FAILURE_THRESHOLD (реальный повод: BUILD откатывался на Aster 12 раз
# подряд из-за именно этой ошибки на второй ноге, прежде чем это заметили).
_PERMANENT_ERROR_MARKERS = ("contract not activated",)


def is_permanent_error(reason: str) -> bool:
    reason_lower = (reason or "").lower()
    return any(marker in reason_lower for marker in _PERMANENT_ERROR_MARKERS)


def auto_exclude_coin(coin: str) -> bool:
    """Добавляет монету в SCANNER_EXCLUDED_COINS — и в файл .env (переживает
    рестарт), и сразу в os.environ текущего процесса (main.py читает список
    динамически через os.getenv на каждый сигнал — там подействует
    немедленно). scanner.py кэширует список в __init__, так что там
    подействует только после рестарта — до этого момента монету всё равно
    держит закрытой обычная блокировка is_blocked(), так что дыры нет.
    Возвращает True, если монета была добавлена только что (раньше в
    списке не было)."""
    coin = coin.upper()
    try:
        with open(_ENV_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except OSError:
        return False

    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("SCANNER_EXCLUDED_COINS="):
            _, _, value = stripped.partition("=")
            existing = [c.strip().upper() for c in value.split(",") if c.strip()]
            if coin in existing:
                os.environ["SCANNER_EXCLUDED_COINS"] = ",".join(existing)
                return False
            existing.append(coin)
            new_value = ",".join(existing)
            lines[i] = f"SCANNER_EXCLUDED_COINS={new_value}\n"
            with open(_ENV_PATH, "w", encoding="utf-8") as f:
                f.writelines(lines)
            os.environ["SCANNER_EXCLUDED_COINS"] = new_value
            return True

    # Строки SCANNER_EXCLUDED_COINS= в .env не нашлось — дописываем в конец.
    lines.append(f"\nSCANNER_EXCLUDED_COINS={coin}\n")
    with open(_ENV_PATH, "w", encoding="utf-8") as f:
        f.writelines(lines)
    os.environ["SCANNER_EXCLUDED_COINS"] = coin
    return True


# =============================================================================
# ТОЧЕЧНОЕ исключение "монета+биржа" — по прямой просьбе пользователя
# 2026-09-09: "убери дельту из исключений общих и добавь в исключение
# только для биржи мекс". В отличие от SCANNER_EXCLUDED_COINS (монета
# запрещена ВООБЩЕ, на всех биржах) — здесь монета торгуется нормально
# везде, КРОМЕ конкретной биржи (например, DELTA была структурно плохой
# именно на связке через MEXC, но не обязательно на других биржах).
# Формат в .env: "МОНЕТА:биржа,МОНЕТА2:биржа2" (биржа — нижним регистром,
# как в SCANNER_EXCHANGES/TEST_BATCH_SAFE_EXCHANGES).
# =============================================================================
def parse_coin_exchange_pairs(raw: str) -> set:
    pairs = set()
    for item in (raw or "").split(","):
        item = item.strip()
        if not item or ":" not in item:
            continue
        coin, _, exchange = item.partition(":")
        coin = coin.strip().upper()
        exchange = exchange.strip().lower()
        if coin and exchange:
            pairs.add((coin, exchange))
    return pairs


def is_coin_exchange_excluded(coin: str, exchange: str) -> bool:
    """Читает SCANNER_EXCLUDED_COIN_EXCHANGE_PAIRS заново на каждый вызов
    (не кэшируется) — так же, как SCANNER_EXCLUDED_COINS читается в
    main.py/skyspreads_signals.py. Для горячего цикла сканера (scanner.py,
    сравнение КАЖДОЙ пары бирж) используйте parse_coin_exchange_pairs()
    ОДИН раз и держите результат в self, не вызывайте эту обёртку в цикле."""
    pairs = parse_coin_exchange_pairs(os.getenv("SCANNER_EXCLUDED_COIN_EXCHANGE_PAIRS", ""))
    return (coin.upper(), (exchange or "").strip().lower()) in pairs


def _load() -> dict:
    if not os.path.exists(_STORE_PATH):
        return {}
    try:
        with open(_STORE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _save(data: dict) -> None:
    with open(_STORE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def record_rollback_failure(coin: str, reason: str) -> bool:
    """Записывает ОДИН реальный откат (нога открылась, вторая — нет) по
    этой монете. Возвращает True, если монета ТОЛЬКО ЧТО достигла порога
    и теперь заблокирована (вызывающий код должен явно уведомить
    пользователя именно в этом случае — см. trade_tool.py)."""
    data = _load()
    coin = coin.upper()
    entry = data.get(coin, {"count": 0, "reasons": [], "blocked": False})
    was_blocked = entry.get("blocked", False)
    entry["count"] += 1
    entry["reasons"] = (entry.get("reasons") or [])[-4:] + [reason]  # последние 5, не растим файл бесконечно
    entry["last_failure_at"] = datetime.now(timezone.utc).isoformat()
    entry["blocked"] = entry["count"] >= FAILURE_THRESHOLD
    data[coin] = entry
    _save(data)
    return entry["blocked"] and not was_blocked  # True только В МОМЕНТ блокировки


def is_blocked(coin: str) -> bool:
    entry = _load().get(coin.upper())
    return bool(entry and entry.get("blocked"))


def get_block_info(coin: str) -> Optional[dict]:
    return _load().get(coin.upper())


def clear_block(coin: str) -> bool:
    """Снимает блокировку И сбрасывает счётчик неудач — вызывать, когда
    пользователь подтвердил, что причина устранена (например, поменяли
    API-ключ или монету восстановили на бирже)."""
    data = _load()
    coin = coin.upper()
    if coin in data:
        del data[coin]
        _save(data)
        return True
    return False


def list_blocked() -> dict:
    return {k: v for k, v in _load().items() if v.get("blocked")}
