@echo off
cd /d "%~dp0"
echo ================================
echo  chatgptwebapp2api starting...
echo  Base URL: http://localhost:8100/v1
echo  Band karne ke liye: Ctrl+C
echo ================================
python -m chatgptwebapp2api
pause
