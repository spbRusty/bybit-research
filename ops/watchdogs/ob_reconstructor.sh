#!/bin/bash
# Рестарт ob_reconstructor, если лог не растёт: WS может замолчать, не разрывая
# соединение, и сборщик стоит молча (было 2026-11-23 и 2026-11-25).
log=/home/vlad/Документы/построение/collector/logs/ob_reconstructor.log
age=$(( $(date +%s) - $(stat -c %Y "$log") ))
if [ "$age" -gt 600 ]; then
    echo "ob_reconstructor silent ${age}s, restarting"
    systemctl --user restart bybit_ob_reconstructor
fi
