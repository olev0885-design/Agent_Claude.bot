#!/usr/bin/env bash
# =============================================================================
# deploy/install.sh — установка бота на чистый Ubuntu 24.04 (VPS в Токио).
# =============================================================================
# Запуск от root ОДИН раз:   bash install.sh
# Что делает:
#   1. системные пакеты, Python 3.12, git, chrony (точные часы);
#   2. пользователь `bot` без прав root — бот НЕ работает от root;
#   3. клон репозитория в /opt/agent-claude-bot, venv, зависимости
#      из deploy/requirements-lock.txt (ровно те версии, что на ноутбуке);
#   4. systemd-служба (автостарт при загрузке, авторестарт при падении);
#   5. cron: синхронизация с GitHub раз в час (tools_git_sync.py).
# Что НЕ делает (сознательно): не копирует .env и Telegram-сессии — они
# передаются отдельно по scp (см. README.md), их нет и не должно быть в git.
# =============================================================================
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/olev0885-design/Agent_Claude.bot.git}"
APP_DIR="/opt/agent-claude-bot"
APP_USER="bot"

echo "== 1/5 системные пакеты =="
apt-get update -y
apt-get install -y --no-install-recommends \
    python3.12 python3.12-venv python3-pip git curl chrony ca-certificates

# Часы: chrony держит расхождение с NTP в миллисекундах. Биржи отвергают
# подписанные запросы при расхождении >5с — на ноутбуке это уже стоило
# нам вечера (см. clock_guard.py).
systemctl enable --now chrony
timedatectl set-ntp true || true

echo "== 2/5 пользователь =="
id -u "$APP_USER" >/dev/null 2>&1 || useradd --system --create-home --shell /bin/bash "$APP_USER"

echo "== 3/5 код и зависимости =="
if [ ! -d "$APP_DIR/.git" ]; then
    git clone "$REPO_URL" "$APP_DIR"
fi
chown -R "$APP_USER:$APP_USER" "$APP_DIR"
sudo -u "$APP_USER" bash -c "
    cd '$APP_DIR'
    python3.12 -m venv venv
    ./venv/bin/pip install --upgrade pip wheel
    ./venv/bin/pip install -r deploy/requirements-lock.txt
    # пакет bot_crew (src/) — editable, как на ноутбуке: python -m bot_crew.main
    ./venv/bin/pip install -e . --no-deps
"

echo "== 4/5 systemd =="
install -m 0644 "$APP_DIR/deploy/agent-claude-bot.service" /etc/systemd/system/agent-claude-bot.service
systemctl daemon-reload
systemctl enable agent-claude-bot
# НЕ стартуем автоматически: сначала .env, сессии и check_server.py (README).

echo "== 5/5 cron: синхронизация с GitHub раз в час (:23) =="
# Ключ git с правом push кладётся отдельно (README, шаг 4).
( sudo -u "$APP_USER" crontab -l 2>/dev/null | grep -v tools_git_sync ; \
  echo "23 * * * * cd $APP_DIR && PYTHONIOENCODING=utf-8 ./venv/bin/python tools_git_sync.py >> git_sync.log 2>&1" ) \
  | sudo -u "$APP_USER" crontab -

cat <<EOF

Установка завершена. Дальше — по deploy/README.md:
  - скопировать .env и *.session в $APP_DIR (scp), права 600, владелец $APP_USER
  - sudo -u $APP_USER $APP_DIR/venv/bin/python $APP_DIR/deploy/check_server.py
  - systemctl start agent-claude-bot   (ТОЛЬКО когда бот на ноутбуке остановлен)
EOF
