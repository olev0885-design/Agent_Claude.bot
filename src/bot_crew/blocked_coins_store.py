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
    """УСТАРЕЛО (оставлено для обратной совместимости, не вызывается из
    trade_tool.py начиная с 2026-09-11 — см. auto_exclude_coin_on_exchange
    ниже): добавляет монету в SCANNER_EXCLUDED_COINS ЦЕЛИКОМ, на ВСЕХ
    биржах. По прямой просьбе пользователя 2026-09-11 ("если появляется
    contract not activated — исключай его на той бирже, где появилась
    ошибка") глобальное исключение заменено на точечное — "contract not
    activated" почти всегда означает проблему с КОНКРЕТНЫМ аккаунтом на
    КОНКРЕТНОЙ бирже (контракт не включён именно там), а не с монетой как
    таковой — на других биржах она вполне может нормально торговаться."""
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


def auto_exclude_coin_on_exchange(coin: str, exchange: str) -> bool:
    """Добавляет пару (МОНЕТА:биржа) в SCANNER_EXCLUDED_COIN_EXCHANGE_PAIRS
    — и в файл .env (переживает рестарт), и сразу в os.environ текущего
    процесса (main.py/skyspreads_signals.py читают эту переменную
    динамически через is_coin_exchange_excluded() на каждый сигнал — там
    подействует немедленно). scanner.py кэширует разобранный набор пар в
    __init__ (self.excluded_coin_exchange_pairs), так что для НЕГО
    подействует только после рестарта — до этого момента монету на этой
    бирже всё равно продолжает ловить обычная блокировка is_blocked()
    (см. record_rollback_failure/FAILURE_THRESHOLD), так что дыры нет.

    По прямой просьбе пользователя 2026-09-11 ("исключай его на той
    бирже, где появилась ошибка") — ТОЧЕЧНОЕ исключение вместо
    auto_exclude_coin() (который банил монету на ВСЕХ биржах сразу):
    "contract not activated" — это ограничение конкретного аккаунта на
    конкретной бирже, монета вполне может нормально торговаться на
    остальных семи. Возвращает True, если пара была добавлена только что
    (раньше в списке не было)."""
    coin = coin.upper()
    exchange = (exchange or "").strip().lower()
    if not exchange:
        return False
    try:
        with open(_ENV_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except OSError:
        return False

    pair_str = f"{coin}:{exchange}"
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("SCANNER_EXCLUDED_COIN_EXCHANGE_PAIRS="):
            _, _, value = stripped.partition("=")
            existing = [p.strip() for p in value.split(",") if p.strip()]
            existing_pairs = parse_coin_exchange_pairs(value)
            if (coin, exchange) in existing_pairs:
                os.environ["SCANNER_EXCLUDED_COIN_EXCHANGE_PAIRS"] = ",".join(existing)
                return False
            existing.append(pair_str)
            new_value = ",".join(existing)
            lines[i] = f"SCANNER_EXCLUDED_COIN_EXCHANGE_PAIRS={new_value}\n"
            with open(_ENV_PATH, "w", encoding="utf-8") as f:
                f.writelines(lines)
            os.environ["SCANNER_EXCLUDED_COIN_EXCHANGE_PAIRS"] = new_value
            return True

    # Строки SCANNER_EXCLUDED_COIN_EXCHANGE_PAIRS= в .env не нашлось —
    # дописываем в конец.
    lines.append(f"\nSCANNER_EXCLUDED_COIN_EXCHANGE_PAIRS={pair_str}\n")
    with open(_ENV_PATH, "w", encoding="utf-8") as f:
        f.writelines(lines)
    os.environ["SCANNER_EXCLUDED_COIN_EXCHANGE_PAIRS"] = pair_str
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


# ОШИБКИ БАЛАНСА/МАРЖИ — НЕ ВИНА МОНЕТЫ (добавлено 2026-09-15). Реальный
# случай: CVC и LSK — самые частые связки ночи (1038 и 540 раз выше порога
# входа) — молча пропускались, потому что 13.09 были заблокированы после
# двух откатов «INSUFFICIENT_AVAILABLE» / «Margin is insufficient». Это
# состояние СЧЁТА в тот момент (денег на бирже не хватало), а не свойство
# монеты; счёт давно пополнен, перед ордером теперь стоит balance-guard, а
# блок висел бы вечно. Такие причины в счётчик откатов не идут.
_BALANCE_ERROR_MARKERS = (
    "insufficient_available", "insufficient available", "margin is insufficient",
    "insufficient margin", "insufficient balance", "not enough", "code\":-2019", "code\":\"40762",
)


def _is_balance_error(reason: str) -> bool:
    low = (reason or "").lower()
    return any(m.lower() in low for m in _BALANCE_ERROR_MARKERS)


def _block_ttl_hours() -> float:
    """Срок жизни блокировки по откатам. Раньше блок был ВЕЧНЫМ до ручной
    отмены — и стухшая двухдневная проблема резала лучшие связки ночи
    (см. _BALANCE_ERROR_MARKERS). Точечные исключения «монета:биржа» за
    «contract not activated» живут отдельно, в .env, и на них TTL не
    распространяется — там причина действительно постоянная."""
    try:
        return float(os.getenv("BLOCKED_COINS_TTL_HOURS", "24"))
    except (TypeError, ValueError):
        return 24.0


def record_rollback_failure(coin: str, reason: str) -> bool:
    """Записывает ОДИН реальный откат (нога открылась, вторая — нет) по
    этой монете. Возвращает True, если монета ТОЛЬКО ЧТО достигла порога
    и теперь заблокирована (вызывающий код должен явно уведомить
    пользователя именно в этом случае — см. trade_tool.py)."""
    if _is_balance_error(reason):
        print(
            f"[blocked-coins] {coin.upper()}: откат из-за нехватки баланса/маржи — это состояние "
            f"счёта, а не монеты; в счётчик блокировки НЕ записываю."
        )
        return False
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
    if not (entry and entry.get("blocked")):
        return False
    # Срок жизни блокировки — см. _block_ttl_hours.
    last = entry.get("last_failure_at")
    if last:
        try:
            age_hours = (datetime.now(timezone.utc) - datetime.fromisoformat(last)).total_seconds() / 3600.0
        except ValueError:
            age_hours = 0.0
        if age_hours > _block_ttl_hours():
            clear_block(coin)
            print(f"[blocked-coins] {coin.upper()}: блокировка старше {_block_ttl_hours():.0f}ч — снята автоматически.")
            return False
    return True


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
