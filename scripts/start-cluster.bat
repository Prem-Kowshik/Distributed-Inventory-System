@echo off
REM Opens one console window per server: 9 storage nodes + router.
cd /d "%~dp0.."
if not exist out call build.bat
for /f "tokens=1,3" %%a in (config\cluster.conf) do (
  if "%%a"=="node" start "node-%%b" cmd /k scripts\start-node.bat %%b
)
timeout /t 2 /nobreak >nul
start "router" cmd /k scripts\start-router.bat
timeout /t 4 /nobreak >nul
call scripts\client.bat -c STATUS
