@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo ==========================================================
echo 🚀 Запуск Yandex Turbo Downloader...
echo ==========================================================

where python >nul 2>nul
if %errorlevel% neq 0 (
    echo ❌ Ошибка: Python не найден в системе.
    echo Скачайте и установите Python (обязательно отметьте "Add python.exe to PATH"):
    echo https://www.python.org/downloads/
    echo.
    pause
    exit /b 1
)

python -c "import requests, socks" >nul 2>nul
if %errorlevel% neq 0 (
    echo 📦 Установка необходимого модуля requests (только при 1-м запуске)...
    python -m pip install requests PySocks --quiet
)

python app.py
pause
