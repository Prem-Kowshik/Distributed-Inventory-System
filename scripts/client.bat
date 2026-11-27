@echo off
REM Client. No args = interactive.  -c "CMD" = one command.  -f file = command file.  loadgen [sec] [ops/s].  bench N THREADS CMD
cd /d "%~dp0.."
java -Xmx64m -cp out com.inventory.client.Client %*
