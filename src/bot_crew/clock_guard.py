# =============================================================================
# clock_guard.py — контроль СИСТЕМНЫХ ЧАСОВ против времени бирж.
# =============================================================================
# Зачем (добавлено 2026-09-14 после РЕАЛЬНОГО инцидента): после очередного
# запуска бот выглядел живым — сканер крутился, heartbeat шёл, ошибок в
# смысле Traceback не было, — но НИ ОДИН подписанный запрос не проходил.
# Все семь бирж отвечали одно и то же разными словами: binance -1021
# "outside of the recvWindow", bybit 10002 "check your server timestamp",
# bitget 40008 "Request timestamp expired", gate REQUEST_EXPIRED, mexc
# 700003, aster -1000 "Your device time must match the actual time".
# Причина — часы ПК отставали от реального времени на 13 320 секунд
# (3ч42м): служба времени Windows оказалась остановлена, и никто часы не
# выравнивал. В таком состоянии бот не способен ни открыть, ни — что
# опаснее — ЗАКРЫТЬ позицию, и при этом внешне ничем не отличается от
# исправного. Это худший класс поломки: тихий и полный.
#
# Что делает модуль: спрашивает ПУБЛИЧНОЕ (без подписи, работает при любых
# часах) серверное время у нескольких бирж и сравнивает с локальным.
# Берём медиану по нескольким биржам, а не одну — чтобы единичный
# медленный ответ или сбой одной биржи не дал ложную тревогу.
#
# Что делает вызывающий код (scanner.py): при расхождении больше порога —
# громкий алерт в Telegram (владельцу И обоим получателям отчётов, потому
# что это остановка торговли, а не техническая мелочь) и ЗАПРЕТ НОВЫХ
# ВХОДОВ до исправления. Мониторинг открытых позиций не отключаем: он
# всё равно не сможет ничего закрыть (подписи не пройдут), но пусть хотя
# бы честно логирует попытки — молчание здесь хуже шума.
#
# Починить часы сам бот НЕ МОЖЕТ (нужны права администратора на
# w32tm/Set-Date) и не должен пытаться: менять системное время из
# торгового процесса — плохая идея сама по себе.
# =============================================================================
import asyncio
import os
import statistics
import time
from typing import Optional

import ccxt.async_support as ccxt_async

# Биржи с быстрым публичным fetch_time. Достаточно трёх — важна не полнота,
# а устойчивость медианы к одному выпавшему ответу.
_REFERENCE_EXCHANGES = ("binanceusdm", "bybit", "bitget")


def max_offset_seconds() -> float:
    """Допустимое расхождение. Самое строгое окно у бирж — bybit recv_window
    5с и binance recvWindow 5с (по умолчанию). Порог 3.5с оставляет запас
    до этой границы, но не ловит шум измерения: первый прогон 2026-09-14
    с порогом 2.0с дал ЛОЖНУЮ тревогу на 2.1с при реальном смещении 0.4с
    — замер шёл одновременно с подключением семи бирж, и раздутый
    round-trip исказил оценку."""
    try:
        return float(os.getenv("CLOCK_MAX_OFFSET_SECONDS", "3.5"))
    except (TypeError, ValueError):
        return 3.5


# Сколько замеров делаем на каждую биржу за одну проверку. Из них берётся
# тот, у которого НАИМЕНЬШИЙ round-trip: чем быстрее пришёл ответ, тем
# меньше сетевая задержка исказила оценку смещения — это стандартный приём
# NTP. Один замер под нагрузкой (старт бота, семь параллельных
# подключений) легко даёт погрешность в секунды, см. комментарий выше.
_SAMPLES_PER_EXCHANGE = 3


async def _one(exchange_id: str) -> Optional[float]:
    """Смещение по одной бирже: server_time - local_time, в секундах.
    Локальное время берём как середину между отправкой и получением,
    чтобы вычесть сетевую задержку (иначе к смещению прибавилась бы
    половина round-trip). Из нескольких замеров возвращаем тот, у которого
    round-trip минимален — он точнее всех остальных."""
    exchange = getattr(ccxt_async, exchange_id)({"timeout": 5000})
    best_offset, best_rtt = None, None
    try:
        for _ in range(_SAMPLES_PER_EXCHANGE):
            try:
                sent = time.time()
                server_ms = await exchange.fetch_time()
                received = time.time()
            except Exception:
                continue
            if not server_ms:
                continue
            rtt = received - sent
            if best_rtt is None or rtt < best_rtt:
                best_rtt = rtt
                best_offset = server_ms / 1000.0 - (sent + received) / 2.0
        return best_offset
    finally:
        try:
            await exchange.close()
        except Exception:
            pass


async def measure_offset() -> Optional[float]:
    """Медианное смещение локальных часов относительно бирж (секунды,
    положительное = биржи впереди, т.е. наши часы ОТСТАЮТ). None — если
    ни одна из опорных бирж не ответила (сеть лежит) — это НЕ повод
    считать часы сбитыми, вызывающий код должен трактовать None как
    «неизвестно», а не как «плохо»."""
    results = await asyncio.gather(*(_one(x) for x in _REFERENCE_EXCHANGES))
    offsets = [r for r in results if r is not None]
    if not offsets:
        return None
    return statistics.median(offsets)


def describe(offset: float) -> str:
    """Человекочитаемое описание для лога/Telegram."""
    direction = "ОТСТАЮТ" if offset > 0 else "СПЕШАТ"
    hours = abs(offset) / 3600
    if hours >= 1:
        magnitude = f"{abs(offset):.0f} с ({hours:.2f} ч)"
    else:
        magnitude = f"{abs(offset):.1f} с"
    return f"системные часы {direction} от времени бирж на {magnitude}"
