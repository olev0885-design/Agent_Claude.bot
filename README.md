# Bot Crew — арбитраж фьючерсных спредов на CrewAI

Мульти-агентный торговый бот на **CrewAI**: один агент парсит сигналы из
Telegram, второй — асинхронно исполняет арбитражную сделку сразу на двух
биржах через **CCXT**.

## Структура проекта

```
Agent_Claude.bot/
├── .env.example              # шаблон переменных окружения (ключи API)
├── .gitignore                # исключает .env, venv/, *.session и т.д.
├── requirements.txt          # зависимости проекта
├── pyproject.toml            # делает пакет "bot_crew" импортируемым
└── src/
    └── bot_crew/
        ├── config/
        │   ├── agents.yaml   # описание ролей агентов (signal_parser, trade_executor)
        │   └── tasks.yaml    # описание задач (parse_signal_task, execute_trade_task)
        ├── tools/
        │   └── trade_tool.py # асинхронный инструмент исполнения ордеров через CCXT
        ├── crew.py           # сборка Agent + Task -> Crew (декораторы @CrewBase)
        └── main.py           # точка входа, запуск Crew на тестовом сигнале
```

## Установка

```bash
# 1. Создать и активировать виртуальное окружение
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # Linux/macOS

# 2. Установить зависимости
pip install -r requirements.txt

# 3. Установить сам пакет bot_crew в editable-режиме
#    (нужно, чтобы работал импорт "from bot_crew.crew import BotCrew")
pip install -e .

# 4. Скопировать шаблон переменных окружения и вписать свои ключи
copy .env.example .env        # Windows
# cp .env.example .env        # Linux/macOS
```

Откройте `.env` и заполните:
- `ANTHROPIC_API_KEY` (или `OPENAI_API_KEY`) — ключ для LLM, который "думает" за агентов;
- `TELEGRAM_API_ID` / `TELEGRAM_API_HASH` / `TELEGRAM_PHONE` — для Telethon;
- `<БИРЖА>_API_KEY` / `<БИРЖА>_API_SECRET` (например, `BYBIT_API_KEY`, `MEXC_API_KEY`) — API-ключи бирж, на которых торгуете, без прав на вывод средств! (полный список см. в `.env.example`; биржа OURBIT не поддерживается CCXT в принципе);
- `DRY_RUN=True` — оставьте `True`, пока не протестируете пайплайн полностью.

## Запуск (тестовый прогон)

```bash
python -m bot_crew.main
```

Скрипт прогонит через Crew пример сигнала:

```
🚨 Новый сигнал!
Coin: BTC
LONG binance / SHORT bybit
Цены синхронизированы: aligned in ✅
```

Агент `signal_parser` извлечёт JSON `{coin, long_exchange, short_exchange,
is_aligned}`, а агент `trade_executor` вызовет `trade_tool` и (в режиме
`DRY_RUN=True`) выведет симулированный отчёт об исполнении без реальных
ордеров на бирже.

## Как это устроено

1. **signal_parser** (config/agents.yaml) — с помощью регулярных выражений
   вычленяет из сырого текста Telegram монету, биржи для LONG/SHORT и флаг
   `is_aligned`.
2. **trade_executor** (config/agents.yaml) — получает JSON от парсера и
   вызывает инструмент `trade_tool` (tools/trade_tool.py), который через
   `asyncio.gather()` **одновременно** отправляет ордера на обе биржи,
   минимизируя задержку между ногами спреда.
3. **crew.py** связывает агентов, задачи и LLM в единый объект `Crew`,
   который выполняет задачи строго последовательно (`Process.sequential`):
   сначала парсинг, потом исполнение.
4. **main.py** — точка входа: подгружает `.env`, собирает `Crew` и
   запускает её на входном тексте сигнала (`kickoff`).

## Следующие шаги (не реализовано в этой заготовке)

- Подключить `telethon` для live-прослушивания Telegram-канала
  (заготовка-скелет уже есть в комментарии внутри `main.py`).
- Добавить более точный расчёт объёма позиции (запрос цены и шага лота
  через `fetch_market` перед созданием ордера).
- Логирование сделок в файл/БД для последующего анализа P&L.

## Безопасность

- `.env` никогда не коммитится (см. `.gitignore`).
- API-ключи бирж создавайте **без права вывода средств** (Withdraw).
- Перед реальной торговлей обязательно протестируйте с `DRY_RUN=True`.
