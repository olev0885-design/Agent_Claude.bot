# =============================================================================
# tools_git_sync.py — безопасная синхронизация репозитория с GitHub.
# =============================================================================
# Добавлено 2026-09-14 по прямой просьбе пользователя: "каждый час обновляй
# себя в репозитории гитхаба". Это НЕ слепой `git add -A && git push`:
# перед коммитом содержимое индекса проверяется на признаки утечки
# секретов, и при ЛЮБОМ совпадении синхронизация останавливается с
# ненулевым кодом — лучше пропустить час, чем выложить ключ от биржи.
#
# Что проверяется (только ДОБАВЛЕННЫЕ строки в staged-диффе):
#   - ключи вида sk-..., длинные hex-строки (секреты бирж, приватные ключи),
#   - EVM-адреса/ключи 0x + 40/64 hex (Aster/Hyperliquid),
#   - токены Telegram-ботов, телефонные номера,
#   - присваивания api_key/secret/passphrase с непустым литералом.
# Плюс жёсткая проверка, что .env, *.session, журнал сделок и состояние
# позиций НЕ попали в индекс независимо от .gitignore.
#
# Запуск:  venv/Scripts/python.exe tools_git_sync.py ["сообщение коммита"]
# Коды выхода: 0 — запушено или нечего коммитить; 2 — найден секрет;
#              3 — запретный файл в индексе; 1 — ошибка git.
# =============================================================================
import re
import subprocess
import sys
from datetime import datetime, timezone

FORBIDDEN_TRACKED = re.compile(
    r"(^|/)(\.env|\.env\..*|.*\.session|.*\.session-journal|trade_ledger\.jsonl|"
    r"open_positions\.json|blocked_coins\.json|daily_report_state\.json|restart_note\.txt|.*\.log)$"
)

SECRET_PATTERNS = [
    ("ключ sk-...", re.compile(r"sk-[A-Za-z0-9_-]{16,}")),
    ("hex-секрет 32+", re.compile(r"(?<![A-Za-z0-9])[a-fA-F0-9]{32,}(?![A-Za-z0-9])")),
    ("EVM адрес/ключ", re.compile(r"0x[a-fA-F0-9]{40}(?:[a-fA-F0-9]{24})?")),
    ("токен Telegram-бота", re.compile(r"\b\d{8,10}:[A-Za-z0-9_-]{35}\b")),
    ("телефон", re.compile(r"\+\d{10,14}\b")),
    ("присваивание секрета", re.compile(
        r"(?i)(api[_-]?key|api[_-]?secret|secret|passphrase|private[_-]?key|password)\s*[:=]\s*['\"][^'\"\s]{8,}['\"]"
    )),
]

# Строки, которые ПОХОЖИ на секрет, но заведомо им не являются — чтобы не
# останавливать синхронизацию на ложных срабатываниях. Только явные случаи.
ALLOWLIST = [
    re.compile(r"Co-Authored-By"),
    re.compile(r"noreply@anthropic\.com"),
    re.compile(r"(?i)your[_-]?(api[_-]?key|secret|passphrase)"),  # плейсхолдеры в .env.example
    re.compile(r"(?i)xxx+|placeholder|example"),
]


def git(*args, check=True) -> str:
    result = subprocess.run(
        ["git", *args], capture_output=True, text=True, encoding="utf-8", errors="replace"
    )
    if check and result.returncode != 0:
        print(f"[git-sync] git {' '.join(args)} упал: {result.stderr.strip()}")
        sys.exit(1)
    return result.stdout


def main() -> int:
    # Авто-подхват и новых, и изменённых, и удалённых файлов — .gitignore
    # отвечает за то, чего тут быть не должно, а FORBIDDEN_TRACKED ниже —
    # страховка на случай, если .gitignore кто-то сломал.
    git("add", "-A")
    staged = [line for line in git("diff", "--cached", "--name-only").splitlines() if line.strip()]
    if not staged:
        print("[git-sync] изменений нет — коммитить нечего.")
        return 0

    forbidden = [f for f in staged if FORBIDDEN_TRACKED.search(f)]
    if forbidden:
        print("[git-sync] СТОП: в индекс попали запретные файлы — синхронизация отменена:")
        for f in forbidden:
            print(f"    {f}")
        git("reset", "-q")
        return 3

    diff = git("diff", "--cached", "-U0")
    added = [l[1:] for l in diff.splitlines() if l.startswith("+") and not l.startswith("+++")]
    hits = []
    for line in added:
        if any(a.search(line) for a in ALLOWLIST):
            continue
        for label, pattern in SECRET_PATTERNS:
            if pattern.search(line):
                hits.append((label, line.strip()[:120]))
                break
    if hits:
        print(f"[git-sync] СТОП: найдено {len(hits)} строк, похожих на секреты — синхронизация отменена:")
        for label, line in hits[:10]:
            print(f"    [{label}] {line}")
        git("reset", "-q")
        return 2

    message = sys.argv[1] if len(sys.argv) > 1 else (
        f"Auto-sync {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC\n\n"
        f"Файлы: {', '.join(staged[:12])}{' …' if len(staged) > 12 else ''}\n\n"
        f"Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
    )
    subprocess.run(
        ["git", "-c", "core.safecrlf=false", "commit", "-q", "-F", "-"],
        input=message, text=True, encoding="utf-8", check=True,
    )
    git("push", "-q", "origin", "main")
    sha = git("rev-parse", "--short", "HEAD").strip()
    print(f"[git-sync] запушено {sha}: {len(staged)} файл(ов) — {', '.join(staged[:6])}{' …' if len(staged) > 6 else ''}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
