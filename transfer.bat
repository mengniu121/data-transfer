@echo off
setlocal EnableDelayedExpansion

REM Check if parameter is provided
REM Execute Python script
echo Starting migration process...
python main3.py "%~1"

REM Check execution result
if errorlevel 1 (
    echo Error: Migration process failed
    pause
    exit /b 1
) else (
    echo Migration process completed successfully
    pause
    exit /b 0
)

endlocal
