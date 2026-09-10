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
from html import escape as _html_escape


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

    # Сентинел, отличающий "parse_mode не задан явно" (тогда Telethon сам
    # использует markdown по умолчанию клиента — как было раньше, чтобы не
    # сломать **жирный** текст в notify_open/close/error) от "передать
    # None намеренно" — сам Telethon внутри тоже использует пустой tuple
    # () как дефолт своего send_message(), а не None, ровно по этой же
    # причине (None у него означает "выключить парсинг совсем").
    _NO_PARSE_MODE = object()

    def _dispatch(self, text: str, parse_mode=_NO_PARSE_MODE, recipients=("me",)) -> None:
        try:
            asyncio.run_coroutine_threadsafe(self._send(text, parse_mode, recipients), self._loop)
        except Exception as exc:
            # Не удалось даже ЗАПЛАНИРОВАТЬ отправку (например, цикл уже
            # остановлен) — просто логируем в консоль, торговую логику это
            # никак не должно затронуть.
            print(f"[notifier] Не удалось запланировать Telegram-уведомление: {exc}")

    async def _send(self, text: str, parse_mode=_NO_PARSE_MODE, recipients=("me",)) -> None:
        kwargs = {} if parse_mode is self._NO_PARSE_MODE else {"parse_mode": parse_mode}
        # Каждому получателю — отдельная попытка: сбой отправки одному
        # (например, @Depositik ещё не писал боту/аккаунту первым, и
        # Telegram не резолвит username без этого) не должен мешать
        # отправке остальным.
        for recipient in recipients:
            try:
                await self._client.send_message(recipient, text, **kwargs)
            except Exception as exc:
                print(f"[notifier] Не удалось отправить Telegram-уведомление ({recipient}): {exc}")

    # -------------------------------------------------------------------
    # 🟢 ВХОД В СДЕЛКУ — дублируется в @Depositik (по явной просьбе
    # пользователя 2026-09-06: "как только заходим в ордер — отправляй всю
    # информацию по нему в Telegram на @Depositik") в ДОПОЛНЕНИЕ к обычной
    # отправке в Saved Messages ("me"), а не вместо неё.
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
        self._dispatch(text, recipients=("me", "@Depositik"))

    # -------------------------------------------------------------------
    # 🔴 ЗАКРЫТИЕ СДЕЛКИ — дублируется в @Depositik (та же просьба
    # пользователя 2026-09-07, что и для ВХОДА в сделку: "по закрытию
    # ордера тоже отправляй всю информацию в @Depositik").
    # -------------------------------------------------------------------
    def notify_close(self, *, symbol: str, reason: str, pnl_amount, pnl_percent) -> None:
        text = (
            "🔴 **ЗАКРЫТИЕ СДЕЛКИ**\n"
            f"• **Монета:** {symbol}\n"
            f"• **Причина закрытия:** {reason}\n"
            f"• **Итоговый PnL:** {_fmt_pnl_amount(pnl_amount)} ({_fmt_pnl_percent(pnl_percent)})\n"
            f"• **Время:** {_now_str()}"
        )
        self._dispatch(text, recipients=("me", "@Depositik"))

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

    # -------------------------------------------------------------------
    # ⚡ AUTO-TRADE: сканер начинает открывать сделку (см. scanner.py) —
    # отправляется ДО вызова execute_arbitrage_trade, чтобы вы видели факт
    # попытки входа сразу, не дожидаясь результата исполнения ордеров
    # (тот придёт отдельным notify_open/notify_error чуть позже).
    # -------------------------------------------------------------------
    def notify_auto_trade_trigger(self, *, symbol: str, spread_percent, amount_usdt: float) -> None:
        text = (
            f"⚡ [AUTO-TRADE] Открываю сделку по #{symbol} "
            f"| Spread: {_fmt_spread(spread_percent)} | Объём: ${amount_usdt:.2f}"
        )
        self._dispatch(text)

    # -------------------------------------------------------------------
    # 🔍 НАЙДЕНА СВЯЗКА (сканер рынка, scanner.py) — HTML с моноширинным
    # блоком <pre>, поэтому явно просим parse_mode='html' (а не дефолтный
    # markdown, который используют остальные notify_* методы выше).
    # -------------------------------------------------------------------
    def notify_scan_alert(self, html: str) -> None:
        self._dispatch(html, parse_mode="html")

    # -------------------------------------------------------------------
    # 🔴 [POSITION CLOSED] — закрытие ОДНОЙ сделки тестовой серии
    # (test_batch.py/scanner.py). Дельта-нейтральная связка почти всегда
    # даёт ОДНУ ногу в плюс, ДРУГУЮ в минус — формат явно показывает обе
    # ноги по отдельности (с пометкой "Выигрышная"/"Проигрышная"), чтобы
    # было видно, как плюс перекрыл минус, а не только итоговую сумму.
    # -------------------------------------------------------------------
    def notify_test_position_closed(
        self,
        *,
        symbol: str,
        long_exchange: str,
        short_exchange: str,
        entry_spread_pct: float,
        exit_spread_pct: float,
        long_pnl,
        short_pnl,
        pnl_amount: float,
        pnl_percent: float,
        closed_count: int,
        total_count: int,
    ) -> None:
        def _leg_line(label: str, exchange: str, pnl) -> str:
            if pnl is None:
                return f"• {label} ({exchange.upper()}): н/д"
            outcome = "Выигрышная нога" if pnl >= 0 else "Проигрышная нога"
            return f"• {label} ({exchange.upper()}): {pnl:+.2f}$ ({outcome})"

        body = (
            f"🔴 [POSITION CLOSED] #{symbol}\n"
            f"{_leg_line('Long', long_exchange, long_pnl)}\n"
            f"{_leg_line('Short', short_exchange, short_pnl)}\n"
            f"• Чистая прибыль (Total PnL): {pnl_amount:+.2f}$ ({pnl_percent:+.2f}%) "
            f"— вход по спреду {entry_spread_pct:.2f}%, закрытие по {exit_spread_pct:.2f}%\n"
            f"• Статус тестов: [{closed_count} / {total_count} closed]"
        )
        html = f"<pre>{_html_escape(body)}</pre>"
        self._dispatch(html, parse_mode="html", recipients=("me", "@Depositik"))

    # -------------------------------------------------------------------
    # 📊 ФИНАЛЬНЫЙ ОТЧЁТ ПО ТЕСТОВОЙ СЕРИИ — когда все N сделок закрыты
    # (test_batch.py:TestBatchTracker.summary()).
    # -------------------------------------------------------------------
    def notify_test_batch_summary(
        self,
        *,
        total_count: int,
        total_pnl: float,
        avg_holding_seconds: float,
        wins: int,
        losses: int,
        win_rate_pct: float,
    ) -> None:
        avg_minutes, avg_seconds = divmod(int(avg_holding_seconds), 60)
        avg_hours, avg_minutes = divmod(avg_minutes, 60)
        holding_str = f"{avg_hours}ч {avg_minutes}м {avg_seconds}с"
        body = (
            f"📊 ТЕСТОВАЯ СЕРИЯ ЗАВЕРШЕНА — {total_count}/{total_count} сделок закрыто\n"
            f"\n"
            f"Общий PnL: {total_pnl:+.4f}$\n"
            f"Среднее время удержания: {holding_str}\n"
            f"Прибыльных: {wins} ({win_rate_pct:.1f}%)\n"
            f"Убыточных: {losses} ({100 - win_rate_pct:.1f}%)"
        )
        html = f"🏁 <b>Итоговый отчёт тестовой серии</b>\n<pre>{_html_escape(body)}</pre>"
        self._dispatch(html, parse_mode="html")
