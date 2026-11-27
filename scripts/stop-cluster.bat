@echo off
REM Kills every node and the router.
cd /d "%~dp0.."
for %%f in (run\*.pid) do for /f %%p in (%%f) do taskkill /F /PID %%p >nul 2>&1
del /q run\*.pid 2>nul
echo cluster stopped (you can close the windows)
