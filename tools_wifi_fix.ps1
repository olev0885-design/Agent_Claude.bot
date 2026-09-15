# =============================================================================
# tools_wifi_fix.ps1 — настройки Wi-Fi-адаптера ноутбука под торгового бота.
# =============================================================================
# Зачем (2026-09-15): за сутки бот поймал сотни сетевых ошибок (до 140 за
# час) — все семь бирж разом, то есть проблема на нашей стороне. Диагноз:
#   - один SSID (FitelFibra_2G_2428) вещает в ОБОИХ диапазонах, ноутбук
#     прыгает между точками 2.4 ГГц (…cf:49) и 5 ГГц (…cf:4c) — каждый
#     прыжок это секунды без сети;
#   - «Агрессивность роуминга» = «Средн.-мин.» — адаптер регулярно
#     сканирует эфир и уходит с канала на десятки миллисекунд;
#   - драйвер MediaTek MT7921 от апреля 2024 — у чипа известная история
#     обрывов, исправленная в более новых драйверах;
#   - энергосбережение адаптера — Windows может «усыплять» карту.
#
# Что делает скрипт (нужны права администратора — сам их запросит):
#   1. Роуминг -> «Выключено»: адаптер держится за текущую точку и не
#      сканирует эфир в поиске «лучшей». Для ноутбука, который стоит на
#      столе рядом с роутером, это правильно.
#   2. Запрещает Windows отключать адаптер для экономии энергии.
#   3. Схема электропитания: беспроводной адаптер — «Максимальная
#      производительность» и от сети, и от батареи.
# Что НЕ может сделать скрипт: разделить диапазоны на роутере (это в
# настройках роутера — выключить Smart Connect / band steering и дать
# 5 ГГц отдельное имя) и обновить драйвер (ASUS support / Windows Update
# -> необязательные обновления). Оба стоит сделать руками.
#
# Запуск: правой кнопкой -> «Выполнить с помощью PowerShell», или из
# консоли администратора: powershell -ExecutionPolicy Bypass -File tools_wifi_fix.ps1
# =============================================================================
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# --- самоповышение до администратора ---
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "Нужны права администратора — запрашиваю..." -ForegroundColor Yellow
    Start-Process powershell -Verb RunAs -ArgumentList "-NoProfile -ExecutionPolicy Bypass -NoExit -File `"$PSCommandPath`""
    exit
}

$adapter = Get-NetAdapter -Physical | Where-Object { $_.MediaType -like "*802.11*" -and $_.Status -eq "Up" } | Select-Object -First 1
if (-not $adapter) { Write-Host "Wi-Fi адаптер не найден (не подключён?)" -ForegroundColor Red; exit 1 }
$name = $adapter.Name
Write-Host "Адаптер: $($adapter.InterfaceDescription) [$name]" -ForegroundColor Cyan

# --- 1. роуминг ---
$roam = Get-NetAdapterAdvancedProperty -Name $name | Where-Object { $_.DisplayName -match "роуминг|Roam" } | Select-Object -First 1
if ($roam) {
    $off = ($roam.ValidDisplayValues | Where-Object { $_ -match "Выключено|Disabled|Lowest|Мин" } | Select-Object -First 1)
    Write-Host "1. Роуминг: было «$($roam.DisplayValue)» -> ставлю «$off»"
    Set-NetAdapterAdvancedProperty -Name $name -DisplayName $roam.DisplayName -DisplayValue $off
} else { Write-Host "1. Свойство роуминга не найдено — пропускаю" -ForegroundColor Yellow }

# --- 2. энергосбережение адаптера ---
try {
    Disable-NetAdapterPowerManagement -Name $name -AllowComputerToTurnOffDevice -ErrorAction Stop
    Write-Host "2. Отключение адаптера для экономии энергии — ЗАПРЕЩЕНО"
} catch {
    # запасной путь — реестр (PnPCapabilities=24 = не отключать устройство)
    $key = Get-ChildItem "HKLM:\SYSTEM\CurrentControlSet\Control\Class\{4d36e972-e325-11ce-bfc1-08002be10318}" |
        Where-Object { (Get-ItemProperty $_.PSPath -ErrorAction SilentlyContinue).DriverDesc -eq $adapter.InterfaceDescription } | Select-Object -First 1
    if ($key) { Set-ItemProperty -Path $key.PSPath -Name PnPCapabilities -Value 24 -Type DWord; Write-Host "2. Энергосбережение адаптера отключено через реестр (вступит в силу после перезагрузки)" }
    else { Write-Host "2. Не удалось отключить энергосбережение: $($_.Exception.Message)" -ForegroundColor Yellow }
}

# --- 3. схема питания: беспроводной адаптер = максимальная производительность ---
$sub = "19cbb8fa-5279-450e-9fac-8a3d5fedd0c1"; $setting = "12bbebe6-58d6-4636-95bb-3217ef867c1a"
powercfg /setacvalueindex SCHEME_CURRENT $sub $setting 0 | Out-Null
powercfg /setdcvalueindex SCHEME_CURRENT $sub $setting 0 | Out-Null
powercfg /setactive SCHEME_CURRENT | Out-Null
Write-Host "3. Схема питания: Wi-Fi — максимальная производительность (сеть и батарея)"

Write-Host ""
Write-Host "Готово. Итог:" -ForegroundColor Green
Get-NetAdapterAdvancedProperty -Name $name | Where-Object { $_.DisplayName -match "роуминг|Roam" } | ForEach-Object { "  роуминг: " + $_.DisplayValue }
Write-Host ""
Write-Host "Осталось руками:" -ForegroundColor Yellow
Write-Host "  - на роутере выключить Smart Connect / band steering и дать 5 ГГц отдельное имя (подключиться только к нему)"
Write-Host "  - обновить драйвер MediaTek MT7921 (ASUS Support / Windows Update -> необязательные обновления)"
Write-Host "  - идеально: кабель"
