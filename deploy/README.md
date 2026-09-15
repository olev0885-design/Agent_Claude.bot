# Переезд бота на сервер — чеклист

Цель: бот работает на VPS в Токио 24/7, не зависит от ноутбука; задержка до бирж
падает с ~300 мс (Европа) до ~5–70 мс.

Базовая линия с ноутбука (2026-09-15, `deploy/check_server.py`): RTT 288–399 мс до
всех бирж; вход в сделку 450–1000 мс.

---

## Шаг 1. Арендовать VPS (делает владелец)

- Регион: **Токио**. Запасной вариант — Сингапур (если Binance не пустит из Японии,
  это покажет `check_server.py` на шаге 3).
- Размер: 2 vCPU, 2–4 ГБ RAM, 40+ ГБ диск. Ubuntu **24.04 LTS**.
- Провайдеры с Токио: Vultr, DigitalOcean, AWS Lightsail, Linode/Akamai — $6–12/мес.
  Hetzner НЕ подходит (только Европа).
- Нужен **статический IPv4** (обычно по умолчанию).
- SSH-доступ по ключу. Передать мне: IP, пользователя (root или sudo), ключ или
  добавить мой публичный ключ.

## Шаг 2. Установка (делаю я, ~10 минут)

```bash
ssh root@<IP>
curl -fsSL https://raw.githubusercontent.com/olev0885-design/Agent_Claude.bot/main/deploy/install.sh -o install.sh
bash install.sh
```

Ставит Python 3.12, chrony (часы), пользователя `bot`, код из GitHub в
`/opt/agent-claude-bot`, зависимости ровно тех версий, что на ноутбуке
(`requirements-lock.txt`), systemd-службу и cron для синхронизации с GitHub.
Службу **не запускает**.

## Шаг 3. Секреты и проверка (делаю я)

С ноутбука, напрямую по SSH — **не через git**:

```bash
scp .env bot_session.session skyspreads_session.session root@<IP>:/opt/agent-claude-bot/
ssh root@<IP> 'chown bot:bot /opt/agent-claude-bot/.env /opt/agent-claude-bot/*.session && chmod 600 /opt/agent-claude-bot/.env /opt/agent-claude-bot/*.session'
```

Затем на сервере:

```bash
sudo -u bot /opt/agent-claude-bot/venv/bin/python /opt/agent-claude-bot/deploy/check_server.py
```

Скрипт проверяет часы, публичный доступ ко всем биржам (гео-блокировки), приватный
доступ с ключами **с нового IP**, и задержки. Запускать бота можно только при
«ВСЁ В ПОРЯДКЕ».

Если на бирже включён whitelist IP для API-ключа — добавить IP сервера (и после
переезда убрать IP домашней сети). Это и безопаснее.

## Шаг 4. Ключ git для авто-синхронизации (делаю я)

На сервере cron раз в час пушит изменения (`tools_git_sync.py`). Для push нужен
доступ к репозиторию: deploy key с правом записи или токен. Сгенерировать на
сервере `ssh-keygen -t ed25519` под пользователем `bot`, публичный ключ добавить в
GitHub → Settings → Deploy keys (Allow write access), remote переключить на ssh.

## Шаг 5. Переключение (делаю я, при ПУСТОМ учёте)

Правило: **два бота одновременно не работают никогда** — одни ключи, один учёт,
они будут мешать друг другу (сверка одного закроет ноги другого как «сирот»).

1. Дождаться, когда на ноутбуке нет открытых позиций (`tools_status.py`).
2. Остановить бота на ноутбуке.
3. Скопировать на сервер актуальные `blocked_coins.json`, `trade_ledger.jsonl`
   (история сделок — для дневных отчётов), `daily_report_state.json`.
4. На сервере: `systemctl start agent-claude-bot`, затем
   `journalctl -u agent-claude-bot -f` и `tools_status.py` — убедиться:
   потоки подписались, часы ок, сверка идёт, в Telegram пришло «Бот перезапущен».
5. На ноутбуке бота больше не запускать. Планировщик Windows
   (`AgentClaudeBot-ReminderWS`) отключить.

## Шаг 6. Первые сутки

- Сравнить `[timing]` реальных сделок с ноутбуком (450–1000 мс) — ожидаем
  100–300 мс.
- `tools_timing_report.py` — сводка по журналу.
- Telegram-уведомления идут как раньше (владелец, @Depositik, @G_Pobedonosec).

---

## Что где на сервере

| Что | Где |
|---|---|
| код | `/opt/agent-claude-bot` (git, ветка main) |
| логи бота | `/opt/agent-claude-bot/listen.log`, `journalctl -u agent-claude-bot` |
| служба | `systemctl status|start|stop|restart agent-claude-bot` |
| статус одной командой | `sudo -u bot venv/bin/python tools_status.py` |
| секреты | `.env`, `*.session` — права 600, владелец `bot`, НЕ в git |
| часы | `timedatectl` / `chronyc tracking` |

## Что НЕ переезжает

- `watchdog.ps1` — заменён `Restart=always` в systemd.
- Планировщик Windows — заменён cron.
- Блокировка сна (`SetThreadExecutionState`) — серверу не нужна, код сам
  пропускает её вне Windows.

---

## Выполнено 2026-09-15 — сервер в работе

- **Сервер**: Kamatera, зона AS-TY (Токио), IP `45.130.167.115`, Ubuntu 24.04.4, 2 vCPU Type B, 4 GB, 20 GB. Имя `agentser`.
- **Доступ**: только по SSH-ключу (`~/.ssh/agent_claude_bot_ed25519` на ноутбуке), root-пароль заменён на случайный (`~/.ssh/agent_claude_bot_root_pw.txt`, только локально), вход по паролю отключён.
- **Бот**: служба `agent-claude-bot` (enabled, active), код `/opt/agent-claude-bot`, пользователь `bot`. Секреты и учёт перенесены по scp. Бот на ноутбуке остановлен и больше не запускается.
- **Замер из Токио**: binance 9 мс (приватный; было 469 с ноутбука), gate 59 (288), bitget 78 (309), binance публичный 96 (291), hyperliquid 133 (340), mexc 156 (399), bybit 180 (381). Часы 0.0 с. Гео-блокировок нет (в т.ч. OKX, KuCoin 52 мс, BingX, HTX). Цикл сканера 3–8 с вместо 14.
- **Claude Code + Remote Control**: установлен под `bot` (v2.1.272), логин olev0885@gmail.com, сессия «Agent Bot Tokyo» в tmux (`sudo -u bot tmux attach -t claude`), служба `claude-remote` поднимает её при загрузке. Доступ: claude.ai/code или приложение Claude → Code.
- **Код**: правки делаются на ноутбуке и пушатся в GitHub (часовой sync); сервер делает `git pull --ff-only` в :23 каждого часа (cron `bot`). Перезапуск службы после правок — вручную: `systemctl restart agent-claude-bot`.
- **sudo для `bot`**: только `systemctl start|stop|restart|status agent-claude-bot` и `journalctl -u agent-claude-bot`.
