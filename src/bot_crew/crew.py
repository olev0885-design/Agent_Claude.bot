# =============================================================================
# crew.py — ГЛАВНЫЙ ФАЙЛ СБОРКИ КОМАНДЫ АГЕНТОВ (Crew).
# =============================================================================
# Здесь мы связываем воедино:
#   - агентов, описанных в config/agents.yaml
#   - задачи, описанные в config/tasks.yaml
#   - инструменты (tools/trade_tool.py)
# и собираем из них объект Crew, который CrewAI умеет запускать методом
# .kickoff(). Используется декоративный ("аннотационный") стиль CrewAI:
# класс, помеченный @CrewBase, автоматически подхватывает YAML-конфиги.
# =============================================================================

# --- Импорты стандартной библиотеки -----------------------------------------
import json  # Разбор JSON-ответа signal_parser в parse_signal()
import os  # Нужен, чтобы прочитать переменные окружения (ключ LLM и т.д.)

# --- Импорты CrewAI -----------------------------------------------------------
# Agent, Task, Crew, Process — базовые "кирпичики" фреймворка CrewAI:
#   Agent   — один "работник" со своей ролью и LLM
#   Task    — одна задача, которую выполняет конкретный агент
#   Crew    — команда агентов + список задач + порядок их выполнения
#   Process — режим выполнения задач (sequential — по очереди,
#             hierarchical — через агента-менеджера)
from crewai import Agent, Crew, Process, Task

# CrewBase и декораторы agent/task/crew/before_kickoff — это "магия"
# CrewAI, которая автоматически:
#   - читает config/agents.yaml и config/tasks.yaml
#   - подставляет их содержимое в self.agents_config / self.tasks_config
#   - собирает итоговые объекты Agent/Task/Crew по методам, помеченным
#     соответствующими декораторами
from crewai.project import CrewBase, agent, crew, task
from crewai import LLM  # Обёртка CrewAI над LiteLLM для выбора модели

# --- Наш собственный инструмент -----------------------------------------------
# Импортируем инструмент, который написали в tools/trade_tool.py.
from bot_crew.tools.trade_tool import TradeExecutionTool


# =============================================================================
# КЛАСС BotCrew — описание всей "команды" агентов и задач.
# =============================================================================
# Декоратор @CrewBase превращает обычный класс в "сборщик" CrewAI:
# он автоматически ищет файлы config/agents.yaml и config/tasks.yaml
# ОТНОСИТЕЛЬНО РАСПОЛОЖЕНИЯ этого файла (crew.py) и загружает их
# содержимое в атрибуты self.agents_config и self.tasks_config.
@CrewBase
class BotCrew:
    """Класс, описывающий команду агентов для арбитража фьючерсных спредов."""

    # Пути к конфигурационным YAML-файлам (стандартное соглашение CrewAI).
    agents_config = "config/agents.yaml"
    tasks_config = "config/tasks.yaml"

    # -------------------------------------------------------------------
    # __init__ — инициализация: здесь мы один раз создаём LLM и
    # инструмент, чтобы переиспользовать их во всех агентах.
    # -------------------------------------------------------------------
    def __init__(self) -> None:
        # LLM(...) — создаём объект языковой модели, которую будут
        # использовать агенты для рассуждений. Модель и провайдер
        # читаются из переменной окружения, чтобы легко переключаться
        # между Claude/OpenAI без изменения кода.
        self.llm = LLM(
            # "anthropic/claude-sonnet-4-5" — формат LiteLLM: "провайдер/модель".
            # При желании поменяйте на "openai/gpt-4o" и используйте
            # OPENAI_API_KEY вместо ANTHROPIC_API_KEY.
            model=os.getenv("CREW_LLM_MODEL", "anthropic/claude-sonnet-4-5"),
            temperature=0.1,  # Низкая температура — меньше "творчества",
            # больше предсказуемости. Критично для торгового бота: нам
            # нужен строгий детерминированный JSON, а не "фантазии" LLM.
            # max_tokens ограничивает длину ОТВЕТА модели. По умолчанию
            # CrewAI запрашивает очень большой лимит (десятки тысяч токенов),
            # который некоторые провайдеры (например, OpenRouter на низком
            # балансе) отклоняют с ошибкой 402 "недостаточно кредитов".
            # Ответы агентов здесь короткие (JSON/текстовый отчёт), поэтому
            # 4000 с запасом достаточно; настраивается через .env при нужде.
            max_tokens=int(os.getenv("CREW_LLM_MAX_TOKENS", "4000")),
        )

        # Создаём ОДИН инстанс инструмента для сделок — он будет передан
        # агенту trade_executor (см. метод trade_executor() ниже).
        self.trade_tool = TradeExecutionTool()

    # =====================================================================
    # ОПРЕДЕЛЕНИЕ АГЕНТОВ
    # =====================================================================
    # Декоратор @agent говорит CrewAI: "это метод, который создаёт и
    # возвращает объект Agent". Имя метода (signal_parser) ДОЛЖНО совпадать
    # с ключом в agents.yaml, чтобы self.agents_config['signal_parser']
    # подставился автоматически через **self.agents_config['signal_parser'].
    @agent
    def signal_parser(self) -> Agent:
        return Agent(
            # config=... разворачивает словарь {role, goal, backstory}
            # из agents.yaml прямо в аргументы конструктора Agent.
            config=self.agents_config["signal_parser"],
            # verbose=True — агент печатает в консоль ход своих рассуждений
            # (полезно для отладки, в продакшене можно выключить).
            verbose=True,
            # llm — какую языковую модель использует именно этот агент.
            llm=self.llm,
            # allow_delegation=False — агент НЕ может перепоручать задачу
            # другим агентам. Для парсера это не нужно: он должен сам
            # извлечь данные, а не спрашивать у кого-то ещё.
            allow_delegation=False,
        )

    @agent
    def trade_executor(self) -> Agent:
        return Agent(
            config=self.agents_config["trade_executor"],
            verbose=True,
            llm=self.llm,
            allow_delegation=False,
            # tools=[...] — список инструментов, которые доступны ЭТОМУ
            # агенту. Именно trade_tool даёт агенту "руки", чтобы реально
            # исполнить сделку через CCXT (см. tools/trade_tool.py).
            tools=[self.trade_tool],
        )

    # =====================================================================
    # ОПРЕДЕЛЕНИЕ ЗАДАЧ
    # =====================================================================
    # Аналогично агентам: декоратор @task подхватывает описание задачи
    # из tasks.yaml по имени метода.
    @task
    def parse_signal_task(self) -> Task:
        return Task(
            config=self.tasks_config["parse_signal_task"],
            # agent=... явно привязывает задачу к конкретному агенту-
            # исполнителю (self.signal_parser() вызывает метод выше).
            agent=self.signal_parser(),
        )

    @task
    def execute_trade_task(self) -> Task:
        return Task(
            config=self.tasks_config["execute_trade_task"],
            agent=self.trade_executor(),
            # context=[...] — передаёт результат parse_signal_task как
            # входной контекст для execute_trade_task. Дублирует поле
            # "context" из tasks.yaml — CrewAI поддерживает оба варианта,
            # но явное указание здесь надёжнее (меньше "магии").
            context=[self.parse_signal_task()],
        )

    # =====================================================================
    # СБОРКА ИТОГОВОЙ КОМАНДЫ (Crew)
    # =====================================================================
    # Декоратор @crew помечает метод, который собирает всё воедино:
    # список агентов + список задач + процесс их выполнения.
    @crew
    def crew(self) -> Crew:
        return Crew(
            # self.agents и self.tasks — списки, которые CrewAI сам
            # автоматически заполняет объектами из методов, помеченных
            # @agent и @task (порядок методов в классе сохраняется).
            agents=self.agents,
            tasks=self.tasks,
            # Process.sequential — задачи выполняются строго по очереди:
            # сначала parse_signal_task, потом execute_trade_task
            # (в том порядке, в котором задачи объявлены в классе).
            process=Process.sequential,
            verbose=True,
        )

    # =====================================================================
    # parse_signal — ТОЛЬКО парсинг OPEN-сигнала через LLM, БЕЗ исполнения
    # сделки через LLM-агента trade_executor.
    # =====================================================================
    # В main.py реальное открытие ноги делается напрямую через
    # TradeExecutionTool.open_spread() (обычный Python-вызов), а не через
    # LLM-агента: цену входа, объём в монете и order_id для реальных денег
    # надёжнее взять из фактического ответа CCXT, чем доверить их
    # генерации текста языковой моделью. LLM здесь используется только
    # там, где она действительно нужна — вытащить структурированные поля
    # из полу-хаотичного текста сообщения канала.
    def parse_signal(self, raw_signal_text: str) -> dict:
        """Запускает ОДИН agent/task (signal_parser/parse_signal_task) и
        возвращает распарсенный JSON как dict. При ошибке разбора JSON
        возвращает dict с coin/long_exchange/short_exchange = None и
        ключом "_raw_llm_output" с сырым ответом модели для отладки."""
        parser_crew = Crew(
            agents=[self.signal_parser()],
            tasks=[self.parse_signal_task()],
            process=Process.sequential,
            verbose=True,
        )
        result = parser_crew.kickoff(inputs={"raw_signal_text": raw_signal_text})

        raw = str(result).strip()
        # LLM иногда оборачивает JSON в ```json ... ``` несмотря на явную
        # инструкцию не делать этого в tasks.yaml — на всякий случай
        # срезаем такую обёртку перед json.loads.
        if raw.startswith("```"):
            raw = raw.strip("`")
            if raw.lower().startswith("json"):
                raw = raw[4:]
            raw = raw.strip()

        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {
                "coin": None,
                "long_exchange": None,
                "short_exchange": None,
                "_raw_llm_output": raw,
            }
