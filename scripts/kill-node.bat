@echo off
REM Hard-kills a node or the router (like a power failure).  usage: scripts\kill-node.bat m1   (or: router)
cd /d "%~dp0.."
if not exist run\%1.pid (echo no pid file for %1 & exit /b 1)
set /p PID=<run\%1.pid
taskkill /F /PID %PID%
del run\%1.pid
