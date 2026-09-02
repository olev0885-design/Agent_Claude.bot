# =============================================================================
# notifier.py — МГНОВЕННЫЕ Telegram-уведомления о торговых событиях.
# =============================================================================
# Проблема, которую решает этот модуль: раньше отчёт формировался и
# отправлялся ОДНИМ куском в самом конце open_signal()/close_signal() (см.
# main.py) — если где-то по пути (расчёт комиссий, запись в position_store
# и т.п.) вылетало необработанное исключение, отправка в Telegram просто не
# наступала, хотя ордера на биржах уже могли уйти. TelegramNotifier вызывается
# СРАЗУ после того, как ордера реально отправлены на биржи — ДО какой-либо
# дальнейшей обработки, которая теоретически может упасть.
#
# Второй нюанс: реальная торговая логика (open_signal/close_signal в
# main.py) выполняется НЕ в том потоке, где крутится asyncio event loop
# Telethon-клиента, а в отдельном потоке пула (см. loop.run_in_executor(...)
# в listen()) — потому что сама она делает блокирующие вызовы asyncio.run().
# Из обычного потока нельзя просто "await client.send_message(...)" — нет
# работающего event loop. Поэтому TelegramNotifier планирует отправку через
# asyncio.run_coroutine_threadsafe() в ЦИКЛ, где живёт клиент — это работает
# безопасно из любого потока (и из основного тоже) и не блокирует вызывающий
# код (fire-and-forget: не ждём результата отправки).
# =============================================================================

import asyncio
from datetime import datetime, timezone


def _now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _fmt_price(value) -> str:
    if value is None:
        return "н/д"
    try:
        return f"{float(value):.6g}"
    except (TypeError, ValueError):
        return str(value)


def _fmt_spread(value) -> str:
    if value is None:
        return "н/д"
    try:
        return f"{float(value):.2f}%"
    except (TypeError, ValueError):
        # LLM иногда мог вернуть строку уже со знаком "%" — не портим её.
        text = str(value)
        return text if text.endswith("%") else f"{text}%"


def _fmt_pnl_amount(value) -> str:
    if value is None:
        return "н/д"
    try:
        return f"{float(value):+.4f} USDT"
    except (TypeError, ValueError):
        return str(value)


def _fmt_pnl_percent(value) -> str:
    if value is None:
        return "н/д"
    try:
        return f"{float(value):+.2f}%"
    except (TypeError, ValueError):
        return str(value)


class TelegramNotifier:
    """Отправляет мгновенные уведомления о торговых событиях в Saved
    Messages того же Telegram-аккаунта, которым залогинен Telethon-клиент
    (см. client.start(...) в main.py:listen()).

    Безопасен для вызова из ЛЮБОГО потока: методы notify_* — синхронные и
    НЕ блокируют вызывающий код — отправка лишь планируется в event loop
    клиента (fire-and-forget) через run_coroutine_threadsafe. Ошибки самой
    отправки (сеть, флуд-лимит Telegram и т.п.) перехватываются внутри и
    никогда не попадают в торговую логику — сбой уведомления не должен
    ронять бота.
    """

    def __init__(self, client, loop: asyncio.AbstractEventLoop):
        self._client = client
        self._loop = loop

    def _dispatch(self, text: str) -> None:
        try:
            asyncio.run_coroutine_threadsafe(self._send(text), self._loop)
        except Exception as exc:
            # Не удалось даже ЗАПЛАНИРОВАТЬ отправку (например, цикл уже
            # остановлен) — просто логируем в консоль, торговую логику это
            # никак не должно затронуть.
            print(f"[notifier] Не удалось запланировать Telegram-уведомление: {exc}")

    async def _send(self, text: str) -> None:
        try:
            await self._client.send_message("me", text)
        except Exception as exc:
            print(f"[notifier] Не удалось отправить Telegram-уведомление: {exc}")

    # -------------------------------------------------------------------
    # 🟢 ВХОД В СДЕЛКУ
    # -------------------------------------------------------------------
    def notify_open(
        self,
        *,
        symbol: str,
        long_exchange: str,
        long_price,
        short_exchange: str,
        short_price,
        spread=None,
    ) -> None:
        text = (
            "🟢 **ВХОД В СДЕЛКУ (Arbitrage / Funding)**\n"
            f"• **Монета:** {symbol}\n"
            f"• **Long биржа:** {long_exchange} (Цена: {_fmt_price(long_price)})\n"
            f"• **Short биржа:** {short_exchange} (Цена: {_fmt_price(short_price)})\n"
            f"• **Текущий спред/фандинг:** {_fmt_spread(spread)}\n"
            f"• **Время:** {_now_str()}"
        )
        self._dispatch(text)

    # -------------------------------------------------------------------
    # 🔴 ЗАКРЫТИЕ СДЕЛКИ
    # -------------------------------------------------------------------
    def notify_close(self, *, symbol: str, reason: str, pnl_amount, pnl_percent) -> None:
        text = (
            "🔴 **ЗАКРЫТИЕ СДЕЛКИ**\n"
            f"• **Монета:** {symbol}\n"
            f"• **Причина закрытия:** {reason}\n"
            f"• **Итоговый PnL:** {_fmt_pnl_amount(pnl_amount)} ({_fmt_pnl_percent(pnl_percent)})\n"
            f"• **Время:** {_now_str()}"
        )
        self._dispatch(text)

    # -------------------------------------------------------------------
    # ⚠️ ОШИБКА ИСПОЛНЕНИЯ
    # -------------------------------------------------------------------
    def notify_error(self, *, symbol: str, error_message: str) -> None:
        text = (
            "⚠️ **ОШИБКА ИСПОЛНЕНИЯ**\n"
            f"• **Монета:** {symbol}\n"
            f"• **Детали:** {error_message}"
        )
        self._dispatch(text)
