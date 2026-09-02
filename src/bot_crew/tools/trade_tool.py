# =============================================================================
# trade_tool.py — КАСТОМНЫЙ ИНСТРУМЕНТ (Tool) для агента trade_executor.
# =============================================================================
# Здесь реализовано АСИНХРОННОЕ одновременное исполнение двух встречных
# ордеров (LONG на одной бирже, SHORT на другой) через библиотеку CCXT.
# Асинхронность (asyncio.gather) нужна, чтобы отправить оба ордера
# "одновременно" — с минимальной задержкой между ними, а не по очереди.
# =============================================================================

# --- Импорты стандартной библиотеки Python ----------------------------------
import asyncio  # Модуль для асинхронного программирования (корутины, gather)
import os       # Для чтения переменных окружения (ключи API бирж)
import time     # Для замера реальной задержки между исполнением ордеров
from typing import Optional, Type  # Для аннотаций типов

# --- Импорты сторонних библиотек ---------------------------------------------
# ccxt.async_support — асинхронная версия CCXT (в отличие от обычного ccxt,
# методы здесь — корутины: их нужно вызывать через "await").
import ccxt.async_support as ccxt_async
from crewai.tools import BaseTool   # Базовый класс для всех инструментов CrewAI
from pydantic import BaseModel, Field  # Для строгой схемы входных аргументов


# =============================================================================
# АЛИАСЫ БИРЖ: канал сигналов иногда называет биржу не так, как она
# называется в CCXT (например, "HLIQUID" вместо "hyperliquid"). Ключи —
# названия из канала В НИЖНЕМ РЕГИСТРЕ, значения — реальные ID классов CCXT.
# =============================================================================
EXCHANGE_ALIASES = {
    "hliquid": "hyperliquid",
    "hyperliquid": "hyperliquid",
}

# Валюта, в которой котируется перпетуал-контракт (unified CCXT symbol —
# "COIN/QUOTE:QUOTE"). У подавляющего большинства бирж канала это USDT, но
# у Hyperliquid контракты котируются в USDC — если строить символ как
# "COIN/USDT:USDT" на Hyperliquid, CCXT не найдёт такой рынок вообще
# (BadSymbol), и ЛЮБОЙ запрос (цена, комиссия, ордер) на этой бирже упадёт.
# Проверено 2026-08-28: load_markets() на всех остальных биржах канала
# (bybit/mexc/bitget/gate/bingx/aster) показывает именно "*/USDT:USDT".
EXCHANGE_QUOTE_CURRENCY = {
    "hyperliquid": "USDC",
}


def _build_symbol(exchange_name: str, coin: str) -> str:
    """CCXT unified symbol перпетуал-контракта для конкретной биржи (см.
    EXCHANGE_QUOTE_CURRENCY выше — у разных бирж разная валюта котировки)."""
    exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
    quote = EXCHANGE_QUOTE_CURRENCY.get(exchange_id, "USDT")
    return f"{coin.upper()}/{quote}:{quote}"


# Значение переменной окружения считается НЕ настоящим ключом (плейсхолдером
# из .env.example), если оно пустое или начинается с "your_" — так помечены
# все примеры-заглушки в .env.example (your_exchange_1_api_key и т.п.).
def _looks_like_placeholder(value: str) -> bool:
    return not value or value.strip().lower().startswith("your_")


# Кэш процесса: биржи (по имени), для которых уже подтверждён one_way_mode
# (см. _ensure_bitget_one_way_mode) — чтобы не дёргать лишний API-запрос
# перед КАЖДЫМ ордером, если это уже было сделано в текущем запуске бота.
_position_mode_synced: set[str] = set()


class UnsupportedExchangeError(Exception):
    """Биржа названа в сигнале, но её нет в CCXT (например, OURBIT)."""


class ExchangeNotConfiguredError(Exception):
    """Биржа поддерживается CCXT, но для неё не заданы API-ключи в .env."""


# =============================================================================
# СХЕМА ВХОДНЫХ АРГУМЕНТОВ ИНСТРУМЕНТА
# =============================================================================
# CrewAI использует Pydantic-модель, чтобы LLM понимала, какие именно
# аргументы и в каком формате нужно передать при вызове инструмента.
class TradeToolInput(BaseModel):
    """Схема аргументов, которые LLM должна передать в trade_tool."""

    # Тикер монеты, например "BTC". Указывается без "/USDT".
    coin: str = Field(..., description="Тикер монеты, например 'BTC' или 'ETH'")

    # Название биржи (как в CCXT, например 'binance'), где открываем LONG.
    long_exchange: str = Field(
        ..., description="Название биржи для открытия LONG-позиции (например 'binance')"
    )

    # Название биржи (как в CCXT, например 'bybit'), где открываем SHORT.
    short_exchange: str = Field(
        ..., description="Название биржи для открытия SHORT-позиции (например 'bybit')"
    )


# =============================================================================
# КЛАСС ИНСТРУМЕНТА TradeExecutionTool
# =============================================================================
class TradeExecutionTool(BaseTool):
    # name и description — LLM видит их в промпте, чтобы понять, ЗАЧЕМ нужен
    # этот инструмент и когда его вызывать.
    name: str = "trade_tool"
    description: str = (
        "Одновременно открывает LONG-позицию на одной фьючерсной бирже и "
        "SHORT-позицию на другой бирже по заданной монете, используя "
        "асинхронное исполнение CCXT для минимизации задержки между ногами "
        "арбитражного спреда. Принимает: coin, long_exchange, short_exchange."
    )
    # args_schema связывает инструмент со схемой Pydantic выше — CrewAI
    # автоматически валидирует аргументы перед вызовом _run().
    args_schema: Type[BaseModel] = TradeToolInput

    # -------------------------------------------------------------------
    # _run — точка входа, которую вызывает CrewAI, когда LLM решает
    # использовать инструмент. CrewAI ожидает СИНХРОННЫЙ метод, поэтому
    # внутри мы запускаем асинхронную логику через asyncio.run().
    # -------------------------------------------------------------------
    def _run(self, coin: str, long_exchange: str, short_exchange: str) -> str:
        # asyncio.run() создаёт новый event loop, выполняет корутину
        # _execute_spread_async() до конца и возвращает её результат.
        return asyncio.run(
            self._execute_spread_async(coin, long_exchange, short_exchange)
        )

    # -------------------------------------------------------------------
    # _build_exchange_client — вспомогательная функция: создаёт объект
    # клиента CCXT для указанной биржи, подставляя API-ключи из .env.
    # -------------------------------------------------------------------
    def _build_exchange_client(self, exchange_name: str):
        # Приводим имя к ID класса CCXT: сначала смотрим алиасы (например,
        # "hliquid" -> "hyperliquid"), иначе просто берём имя в нижнем
        # регистре как есть (для большинства бирж оно и есть ID CCXT).
        exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())

        # Не все биржи из канала сигналов есть в CCXT (например, OURBIT).
        # Явно проверяем поддержку и кидаем понятную ошибку вместо
        # AttributeError где-то в недрах getattr().
        if not hasattr(ccxt_async, exchange_id):
            raise UnsupportedExchangeError(
                f"Биржа '{exchange_name}' не поддерживается библиотекой CCXT"
            )

        # getattr(ccxt_async, "bybit") эквивалентно ccxt_async.bybit —
        # так мы динамически получаем класс биржи по её строковому имени.
        exchange_class = getattr(ccxt_async, exchange_id)

        api_key, api_secret = self._get_exchange_credentials(exchange_name)
        if _looks_like_placeholder(api_key) or _looks_like_placeholder(api_secret):
            raise ExchangeNotConfiguredError(
                f"Для биржи '{exchange_name}' не заданы API-ключи в .env "
                f"(ожидались {exchange_name.upper()}_API_KEY / "
                f"{exchange_name.upper()}_API_SECRET)"
            )

        # defaultType: у CCXT это НЕ везде "future" — MEXC, ASTER, BINGX и
        # Hyperliquid поддерживают только "swap" (перпетуал-контракты под
        # другим именем в CCXT); "future" на них падает с ошибкой. "swap"
        # поддерживают вообще все биржи канала, поэтому используем его
        # универсально, а не "future" (как было раньше — рабочим это было
        # только для Bybit/Bitget/Gate, случайно).
        config = {
            "enableRateLimit": True,  # Автоматически соблюдать лимиты биржи
            "options": {"defaultType": "swap"},
        }

        if exchange_id == "aster":
            # Aster — DEX, авторизуется НЕ через apiKey/secret, а через
            # приватный ключ EVM-кошелька ('privateKey' в CCXT).
            # Договорённость для этого проекта: ASTER_API_KEY хранит адрес
            # кошелька, ASTER_API_SECRET — приватный ключ.
            #
            # ИЗВЕСТНОЕ ОГРАНИЧЕНИЕ (проверено 2026-08-27): если ASTER_API_KEY/
            # SECRET — это ключи отдельного "агент-кошелька" (Aster создаёт
            # такие с ограниченными правами для API, отдельно от основного
            # аккаунта с балансом) — этот код НЕ заработает. Реализация
            # aster.sign() в CCXT всегда вычисляет "user" в запросе из
            # переданного privateKey (см. self.eth_get_address_from_private_key
            # в исходнике ccxt/async_support/aster.py), полностью игнорируя
            # walletAddress, переданный при создании клиента. Из-за этого
            # API ищет аккаунт по адресу АГЕНТА, а не основного кошелька, и
            # отвечает "No aster user found". Обходной путь потребовал бы
            # руками подменять приватные поля self.options CCXT
            # (cachedWalletAddress/privateKeyHashForCachedWalletAddress) —
            # решили этого не делать (см. обсуждение с пользователем).
            # Работает только если ASTER_API_SECRET — приватный ключ САМОГО
            # основного аккаунта (не агента).
            config["walletAddress"] = api_key
            config["privateKey"] = api_secret
        else:
            config["apiKey"] = api_key
            config["secret"] = api_secret

            # Некоторым биржам (например, Bitget) кроме key/secret нужен
            # ещё "passphrase" (задаётся при создании ключа в личном
            # кабинете) — CCXT ожидает его в поле "password". Если
            # <БИРЖА>_PASSPHRASE не задан — просто не передаём это поле.
            passphrase = os.getenv(f"{exchange_name.upper()}_PASSPHRASE", "")
            if passphrase:
                config["password"] = passphrase

        return exchange_class(config)

    @staticmethod
    async def _ensure_bitget_one_way_mode(exchange, exchange_name: str, symbol: str) -> None:
        # Bitget-специфичный фикс ошибки 40774 "The order type for
        # unilateral position must also be the unilateral position type."
        # Весь остальной код бота (buy=long, sell=short, reduceOnly=закрыть,
        # без posSide/tradeSide) реализован под one_way_mode ("unilateral",
        # как сам Bitget называет его в тексте ошибки) — а НОВЫЕ аккаунты
        # Bitget по умолчанию создаются в hedge_mode (двусторонние позиции),
        # из-за чего наш "однобокий" запрос ордера конфликтует с настройкой
        # аккаунта и биржа отклоняет ордер целиком. Явно переключаем
        # аккаунт в one_way_mode ОДИН РАЗ за запуск процесса (см.
        # _position_mode_synced) перед первым же ордером на этой бирже —
        # идемпотентно, если уже стоит нужный режим, Bitget просто вернёт
        # успех повторно.
        cache_key = exchange_name.lower()
        if cache_key in _position_mode_synced:
            return
        try:
            await exchange.set_position_mode(False, symbol)
            _position_mode_synced.add(cache_key)
        except Exception as exc:
            # Не получилось переключить режим (например, уже есть открытые
            # хедж-позиции на аккаунте) — не роняем размещение ордера из-за
            # этого, он просто провалится тем же понятным кодом 40774, что
            # и раньше, если режимы действительно несовместимы.
            print(f"[bitget] Не удалось выставить one_way_mode: {exc}")

    @staticmethod
    def _get_exchange_credentials(exchange_name: str) -> tuple[str, str]:
        """Читает API-ключ/секрет биржи из переменных окружения по её
        имени: для 'bybit' это BYBIT_API_KEY / BYBIT_API_SECRET, для
        'mexc' — MEXC_API_KEY / MEXC_API_SECRET и т.д. Так поддерживается
        любое количество бирж (сигналы канала используют больше двух),
        а не только жёстко заданная пара EXCHANGE_1/EXCHANGE_2, как было
        раньше (та схема к тому же содержала баг: любая третья биржа
        молча получала ключи от EXCHANGE_2)."""
        prefix = exchange_name.upper()
        api_key = os.getenv(f"{prefix}_API_KEY", "")
        api_secret = os.getenv(f"{prefix}_API_SECRET", "")
        return api_key, api_secret

    # -------------------------------------------------------------------
    # _place_single_order — открывает ОДИН рыночный ордер на одной бирже.
    # Вынесено в отдельную корутину, чтобы обе ноги сделки (long/short)
    # можно было запустить параллельно через asyncio.gather().
    # -------------------------------------------------------------------
    async def _place_single_order(
        self, exchange_name: str, coin: str, side: str, amount_usdt: float, leverage: int
    ) -> dict:
        # Символ строим ЗДЕСЬ (не принимаем готовым снаружи) — у каждой
        # ноги может быть своя биржа с СВОЕЙ валютой котировки (см.
        # _build_symbol/EXCHANGE_QUOTE_CURRENCY: Hyperliquid — USDC,
        # остальные — USDT). Общий символ на обе ноги был бы неверен, если
        # long и short — на разных по этому признаку биржах.
        symbol = _build_symbol(exchange_name, coin)

        # exchange создаём ВНУТРИ try (а не до него), т.к. сама сборка
        # клиента может упасть с UnsupportedExchangeError (биржа из
        # сигнала не поддерживается CCXT, например OURBIT) или
        # ExchangeNotConfiguredError (нет ключей в .env) — такой отказ
        # должен вернуться как обычная строка отчёта, а не уронить
        # asyncio.gather() и вторую ногу сделки вместе с ней.
        exchange = None
        try:
            # DRY_RUN=True — режим "сухого прогона": ордер НЕ отправляется
            # на биржу, а только эмулируется. Это безопасный режим по
            # умолчанию для тестирования пайплайна без реальных денег —
            # поэтому в dry-run НЕ требуем настоящих API-ключей (проверяем
            # только, что биржа вообще поддерживается CCXT), иначе
            # тестирование было бы невозможно без реальных ключей.
            dry_run = os.getenv("DRY_RUN", "True").lower() == "true"

            if dry_run:
                exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
                if not hasattr(ccxt_async, exchange_id):
                    raise UnsupportedExchangeError(
                        f"Биржа '{exchange_name}' не поддерживается библиотекой CCXT"
                    )
                # Эмулируем небольшую сетевую задержку, как будто реально
                # сходили на биржу, чтобы замер задержки был реалистичным.
                await asyncio.sleep(0.05)
                return {
                    "exchange": exchange_name,
                    "side": side,
                    "status": "DRY_RUN_OK",
                    "order_id": "dry-run-simulated",
                    "price": None,
                }

            # Боевой режим: собираем реальный клиент CCXT — это упадёт
            # с UnsupportedExchangeError, если биржи нет в CCXT, или с
            # ExchangeNotConfiguredError, если для неё не заданы ключи.
            exchange = self._build_exchange_client(exchange_name)

            # Выставляем кредитное плечо перед открытием позиции. MEXC —
            # особый случай: её set_leverage() требует ЯВНО указать
            # openType (1=isolated/2=cross) и positionType (1=long/2=short)
            # параметрами, иначе кидает ArgumentsRequired (проверено на
            # реальном боевом ордере 2026-08-28 — без этого сделка на MEXC
            # не открывается вообще). Используем ISOLATED margin (openType=1)
            # — риск ограничен именно этой позицией, не затрагивает маржу
            # других открытых сделок бота.
            leverage_params = {}
            exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
            if exchange_id == "mexc":
                leverage_params = {
                    "openType": 1,  # 1 = isolated margin
                    "positionType": 1 if side == "long" else 2,
                }
            elif exchange_id == "bitget":
                # См. _ensure_bitget_one_way_mode — иначе ордер падает с
                # 40774 (несовпадение hedge_mode/one_way_mode аккаунта).
                await self._ensure_bitget_one_way_mode(exchange, exchange_name, symbol)
            await exchange.set_leverage(leverage, symbol, leverage_params)

            # amount в базовой валюте считаем грубо: сумма_в_USDT / цена.
            # Для точного расчёта в реальном боте нужно сначала запросить
            # актуальную цену через fetch_ticker(), здесь — упрощённая версия.
            ticker = await exchange.fetch_ticker(symbol)
            price = ticker["last"]
            amount_in_coin = amount_usdt / price

            # create_order(symbol, type, side, amount) — универсальный метод
            # CCXT: type="market" значит рыночный ордер (по текущей цене),
            # side="buy" открывает LONG, side="sell" открывает SHORT.
            order = await exchange.create_order(
                symbol=symbol,
                type="market",
                side="buy" if side == "long" else "sell",
                amount=amount_in_coin,
            )

            return {
                "exchange": exchange_name,
                "side": side,
                "status": "OK",
                "order_id": order.get("id"),
                "price": order.get("price") or price,
            }
        except UnsupportedExchangeError as exc:
            # Биржа названа в сигнале, но её нет в CCXT (например, OURBIT).
            # Это штатный, ожидаемый отказ, а не программная ошибка —
            # возвращаем отдельный статус, чтобы агент честно отразил его
            # в отчёте, а не выдал за успех и не за "непонятную ошибку".
            return {
                "exchange": exchange_name,
                "side": side,
                "status": f"EXCHANGE_NOT_SUPPORTED: {exc}",
                "order_id": None,
                "price": None,
            }
        except ExchangeNotConfiguredError as exc:
            # Биржа поддерживается CCXT, но для неё не заданы ключи в .env.
            # Тоже штатный отказ (не программная ошибка) — только в
            # боевом режиме, т.к. в DRY_RUN ключи не требуются вовсе.
            return {
                "exchange": exchange_name,
                "side": side,
                "status": f"NO_CREDENTIALS_CONFIGURED: {exc}",
                "order_id": None,
                "price": None,
            }
        except Exception as exc:
            # Любую ошибку биржи (недостаточно средств, неверный символ и
            # т.д.) перехватываем и возвращаем как часть отчёта, а не роняем
            # весь процесс — вторая нога спреда должна успеть исполниться
            # независимо от результата первой.
            return {
                "exchange": exchange_name,
                "side": side,
                "status": f"ERROR: {exc}",
                "order_id": None,
                "price": None,
            }
        finally:
            # Обязательно закрываем HTTP-сессию биржи, чтобы не было утечек
            # соединений (это требование асинхронной версии CCXT). exchange
            # может остаться None, если сборка клиента упала раньше, чем
            # его создала (например, UnsupportedExchangeError) — тогда
            # закрывать нечего.
            if exchange is not None:
                await exchange.close()

    @staticmethod
    def _is_exchange_supported(exchange_name: str) -> bool:
        """Проверяет поддержку биржи в CCXT БЕЗ создания клиента —
        используется для предварительной проверки ОБЕИХ ног ДО отправки
        любого ордера (см. комментарий в _execute_spread_async)."""
        exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
        return hasattr(ccxt_async, exchange_id)

    @classmethod
    def _is_exchange_configured(cls, exchange_name: str) -> bool:
        """Проверяет, заданы ли для биржи реальные (не плейсхолдерные)
        API-ключи в .env — по переменным <БИРЖА>_API_KEY/_API_SECRET."""
        api_key, api_secret = cls._get_exchange_credentials(exchange_name)
        return not _looks_like_placeholder(api_key) and not _looks_like_placeholder(api_secret)

    @staticmethod
    async def _get_taker_fee_rate(exchange_name: str, symbol: str) -> Optional[float]:
        """Возвращает реальную ставку taker-комиссии биржи для символа
        (например, 0.00055 = 0.055%) — берётся напрямую с биржи через
        публичный (не требующий ключей) метод load_markets(), а не из
        усреднённых цифр "из интернета": у разных аккаунтов бывают разные
        тарифы/скидки (VIP-уровень, скидка за токен биржи и т.п.), и это
        видно только через API конкретного аккаунта/символа.
        Открывающие и закрывающие ордера в этом боте — рыночные (type=
        'market'), а market-ордера почти на всех биржах исполняются как
        taker (снимают ликвидность из стакана), поэтому именно taker-ставка
        релевантна для отчёта, а не maker."""
        exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
        if not hasattr(ccxt_async, exchange_id):
            return None

        exchange_class = getattr(ccxt_async, exchange_id)
        # Публичный клиент БЕЗ ключей — market fee rate это открытые данные
        # (список торговых пар и их условий), авторизация не нужна.
        exchange = exchange_class({"enableRateLimit": True, "options": {"defaultType": "swap"}})
        try:
            await exchange.load_markets()
            market = exchange.markets.get(symbol)
            return market.get("taker") if market else None
        except Exception:
            # Не удалось получить ставку (сеть, символ не найден и т.п.) —
            # не роняем весь отчёт из-за этого, просто вернём None, и
            # main.py покажет "неизвестно" вместо процента.
            return None
        finally:
            await exchange.close()

    # -------------------------------------------------------------------
    # _open_both_legs_async — ЯДРО открытия спреда: одновременно запускает
    # LONG- и SHORT-ордера через asyncio.gather() и замеряет реальную
    # задержку между началом и завершением обеих операций. Возвращает
    # СТРУКТУРИРОВАННЫЙ словарь (не текст) — его использует и LLM-обёртка
    # _execute_spread_async (форматирует в текст для агента), и публичный
    # open_spread() (для прямого вызова из Python-кода бота, в обход LLM —
    # см. main.py: цена входа/order_id нужны боту для учёта позиции и
    # расчёта PnL при закрытии, а доверять эти числа генерации текста
    # языковой моделью для реальных денег не стоит).
    # -------------------------------------------------------------------
    async def _open_both_legs_async(
        self, coin: str, long_exchange: str, short_exchange: str
    ) -> dict:
        amount_usdt = float(os.getenv("TRADE_SIZE_USDT", "100"))
        leverage = int(os.getenv("TRADE_LEVERAGE", "3"))
        dry_run = os.getenv("DRY_RUN", "True").lower() == "true"

        # ВАЖНО: проверяем ОБЕ ноги ДО того, как отправлять ХОТЬ ОДИН
        # ордер. Иначе при DRY_RUN=False возможна ситуация: LONG-нога
        # улетает на поддерживаемую/сконфигурированную биржу, а SHORT-нога
        # тут же отваливается (биржи нет в CCXT ИЛИ для неё не заданы
        # ключи) — и мы остаёмся с НЕЗАХЕДЖИРОВАННОЙ позицией на реальные
        # деньги. Обе корутины в asyncio.gather() стартуют конкурентно,
        # поэтому такую проверку нельзя переложить на except-блок внутри
        # них — к этому моменту уже поздно, ордер мог уйти на биржу.
        unsupported = [
            name for name in (long_exchange, short_exchange)
            if not self._is_exchange_supported(name)
        ]
        # Ключи проверяем ТОЛЬКО в боевом режиме — DRY_RUN намеренно не
        # требует настоящих API-ключей ни для одной биржи.
        not_configured = [
            name for name in (long_exchange, short_exchange)
            if name not in unsupported and not dry_run and not self._is_exchange_configured(name)
        ]
        if unsupported or not_configured:
            reasons = []
            if unsupported:
                reasons.append(
                    f"биржа(и) {', '.join(unsupported)} не поддерживается(-ются) CCXT"
                )
            if not_configured:
                reasons.append(
                    f"для биржи(биржи) {', '.join(not_configured)} не заданы API-ключи в .env"
                )
            return {
                "coin": coin.upper(),
                "cancelled": True,
                "reason": "; ".join(reasons),
                "long": None,
                "short": None,
                "elapsed_ms": None,
            }

        start_time = time.monotonic()  # Засекаем момент старта обеих корутин

        # asyncio.gather() запускает обе корутины ОДНОВРЕМЕННО (конкурентно)
        # и ждёт завершения обеих — это и есть "минимальная задержка между
        # ордерами", о которой говорится в задаче агента.
        long_result, short_result = await asyncio.gather(
            self._place_single_order(long_exchange, coin, "long", amount_usdt, leverage),
            self._place_single_order(short_exchange, coin, "short", amount_usdt, leverage),
        )

        elapsed_ms = round((time.monotonic() - start_time) * 1000, 2)

        # amount_in_coin нужен боту позже, чтобы ЗАКРЫТЬ ровно тот же объём
        # (см. close_spread). В DRY_RUN и при ошибке цены нет — тогда
        # рассчитывать нечего, амаунт останется None (main.py в этом
        # случае просто не заведёт позицию под учёт).
        for leg_result in (long_result, short_result):
            leg_result["amount_usdt"] = amount_usdt
            leg_result["amount_coin"] = (
                amount_usdt / leg_result["price"] if leg_result.get("price") else None
            )

        # Комиссии за вход. Ордера рыночные (type="market") — на
        # подавляющем большинстве бирж это taker-исполнение, поэтому берём
        # taker-ставку. Запрашиваем ОБЕ ставки параллельно — это публичные
        # данные, ключи не нужны, работает и в DRY_RUN.
        long_fee_rate, short_fee_rate = await asyncio.gather(
            self._get_taker_fee_rate(long_exchange, _build_symbol(long_exchange, coin)),
            self._get_taker_fee_rate(short_exchange, _build_symbol(short_exchange, coin)),
        )
        for leg_result, fee_rate in ((long_result, long_fee_rate), (short_result, short_fee_rate)):
            leg_result["taker_fee_rate"] = fee_rate
            leg_result["fee_usdt"] = amount_usdt * fee_rate if fee_rate is not None else None

        return {
            "coin": coin.upper(),
            "cancelled": False,
            "reason": None,
            "long": long_result,
            "short": short_result,
            "elapsed_ms": elapsed_ms,
            "leverage": leverage,
        }

    # -------------------------------------------------------------------
    # _execute_spread_async — обёртка вокруг _open_both_legs_async,
    # форматирующая результат в ТЕКСТ для LLM-агента (см. _run/BaseTool).
    # -------------------------------------------------------------------
    async def _execute_spread_async(
        self, coin: str, long_exchange: str, short_exchange: str
    ) -> str:
        result = await self._open_both_legs_async(coin, long_exchange, short_exchange)

        if result["cancelled"]:
            return (
                f"Монета: {result['coin']}\n"
                f"Сделка НЕ открыта ни на одной ноге: {result['reason']}. "
                f"Открытие только одной ноги спреда создало бы "
                f"незахеджированную позицию, поэтому обе ноги отменены."
            )

        long_result, short_result = result["long"], result["short"]
        return (
            f"Монета: {result['coin']}\n"
            f"LONG на {long_exchange}: {long_result['status']} "
            f"(order_id={long_result['order_id']}, price={long_result['price']})\n"
            f"SHORT на {short_exchange}: {short_result['status']} "
            f"(order_id={short_result['order_id']}, price={short_result['price']})\n"
            f"Задержка между ногами спреда: {result['elapsed_ms']} мс"
        )

    # -------------------------------------------------------------------
    # open_spread — публичная точка входа для ПРЯМОГО вызова из Python
    # (main.py), в обход LLM/CrewAI. Возвращает структурированный словарь
    # (entry price, order_id, amount_coin на каждую ногу) — именно эти
    # данные main.py сохраняет в position_store для последующего закрытия
    # и расчёта PnL.
    # -------------------------------------------------------------------
    def open_spread(self, coin: str, long_exchange: str, short_exchange: str) -> dict:
        return asyncio.run(self._open_both_legs_async(coin, long_exchange, short_exchange))

    # -------------------------------------------------------------------
    # _close_single_order — закрывает ОДНУ ногу ранее открытой позиции:
    # ордер в СТОРОНУ, ПРОТИВОПОЛОЖНУЮ входу (LONG закрывается SELL,
    # SHORT закрывается BUY), с флагом reduceOnly (там, где биржа его
    # поддерживает) — это гарантирует, что ордер именно закрывает
    # существующую позицию, а не случайно открывает новую в другую сторону.
    # -------------------------------------------------------------------
    async def _close_single_order(
        self, exchange_name: str, coin: str, original_side: str, amount_in_coin: float
    ) -> dict:
        symbol = _build_symbol(exchange_name, coin)  # см. _place_single_order — своя валюта котировки на биржу
        close_side = "sell" if original_side == "long" else "buy"
        exchange = None
        try:
            dry_run = os.getenv("DRY_RUN", "True").lower() == "true"

            if dry_run:
                exchange_id = EXCHANGE_ALIASES.get(exchange_name.lower(), exchange_name.lower())
                if not hasattr(ccxt_async, exchange_id):
                    raise UnsupportedExchangeError(
                        f"Биржа '{exchange_name}' не поддерживается библиотекой CCXT"
                    )
                await asyncio.sleep(0.05)
                return {
                    "exchange": exchange_name,
                    "side": close_side,
                    "status": "DRY_RUN_OK",
                    "order_id": "dry-run-close-simulated",
                    "price": None,
                }

            exchange = self._build_exchange_client(exchange_name)

            order = await exchange.create_order(
                symbol=symbol,
                type="market",
                side=close_side,
                amount=amount_in_coin,
                params={"reduceOnly": True},
            )

            price = order.get("price")
            if not price:
                # Не все биржи сразу возвращают цену исполнения рыночного
                # ордера в ответе create_order — подстраховываемся тикером.
                ticker = await exchange.fetch_ticker(symbol)
                price = ticker["last"]

            return {
                "exchange": exchange_name,
                "side": close_side,
                "status": "OK",
                "order_id": order.get("id"),
                "price": price,
            }
        except UnsupportedExchangeError as exc:
            return {
                "exchange": exchange_name,
                "side": close_side,
                "status": f"EXCHANGE_NOT_SUPPORTED: {exc}",
                "order_id": None,
                "price": None,
            }
        except ExchangeNotConfiguredError as exc:
            return {
                "exchange": exchange_name,
                "side": close_side,
                "status": f"NO_CREDENTIALS_CONFIGURED: {exc}",
                "order_id": None,
                "price": None,
            }
        except Exception as exc:
            # Ошибку закрытия НИКОГДА нельзя тихо проглотить — если не
            # закрылась одна нога, позиция остаётся частично открытой
            # (риск на реальные деньги). main.py обязан явно предупредить
            # об этом в отчёте, а не просто залогировать.
            return {
                "exchange": exchange_name,
                "side": close_side,
                "status": f"ERROR: {exc}",
                "order_id": None,
                "price": None,
            }
        finally:
            if exchange is not None:
                await exchange.close()

    async def _close_both_legs_async(
        self,
        coin: str,
        long_exchange: str,
        short_exchange: str,
        long_amount_coin: float,
        short_amount_coin: float,
    ) -> dict:
        start_time = time.monotonic()

        long_result, short_result = await asyncio.gather(
            self._close_single_order(long_exchange, coin, "long", long_amount_coin),
            self._close_single_order(short_exchange, coin, "short", short_amount_coin),
        )

        elapsed_ms = round((time.monotonic() - start_time) * 1000, 2)

        # Комиссии за ВЫХОД — те же рыночные (taker) ордера, что и на
        # входе. Считаем от фактической суммы закрытия (объём × цена
        # выхода), а не от исходных amount_usdt, т.к. цена могла измениться.
        long_fee_rate, short_fee_rate = await asyncio.gather(
            self._get_taker_fee_rate(long_exchange, _build_symbol(long_exchange, coin)),
            self._get_taker_fee_rate(short_exchange, _build_symbol(short_exchange, coin)),
        )
        for leg_result, fee_rate, amount_coin in (
            (long_result, long_fee_rate, long_amount_coin),
            (short_result, short_fee_rate, short_amount_coin),
        ):
            leg_result["taker_fee_rate"] = fee_rate
            leg_result["fee_usdt"] = (
                amount_coin * leg_result["price"] * fee_rate
                if fee_rate is not None and leg_result.get("price")
                else None
            )

        return {
            "coin": coin.upper(),
            "long": long_result,
            "short": short_result,
            "elapsed_ms": elapsed_ms,
        }

    # -------------------------------------------------------------------
    # close_spread — публичная точка входа для закрытия ранее открытой
    # позиции (вызывается из main.py по сигналу "aligned in" из канала).
    # amount_*_coin ОБЯЗАТЕЛЬНО берутся из данных, сохранённых при
    # открытии (position_store), а не пересчитываются заново — иначе при
    # изменении цены закрылся бы не тот объём, что был открыт.
    # -------------------------------------------------------------------
    def close_spread(
        self,
        coin: str,
        long_exchange: str,
        short_exchange: str,
        long_amount_coin: float,
        short_amount_coin: float,
    ) -> dict:
        return asyncio.run(
            self._close_both_legs_async(
                coin, long_exchange, short_exchange, long_amount_coin, short_amount_coin
            )
        )
