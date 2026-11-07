#!/bin/bash
# Переход в папку со скриптом
cd "$(dirname "$0")"

echo "=========================================================="
echo "🚀 Запуск Yandex Turbo Downloader..."
echo "=========================================================="

# Проверка наличия Python 3
if ! command -v python3 &> /dev/null; then
    echo "❌ Ошибка: Python 3 не найден на вашем Mac."
    echo "Установите Python с официального сайта: https://www.python.org"
    echo
    read -n 1 -s -r -p "Нажмите любую клавишу для выхода..."
    exit 1
fi

# Проверка и установка requests при первом запуске
python3 -c "import requests, socks" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "📦 Установка необходимого модуля requests (только при 1-м запуске)..."
    python3 -m pip install requests PySocks --quiet
fi

# Запуск приложения
python3 app.py
