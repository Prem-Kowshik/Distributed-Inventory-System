@echo off
REM Restarts a node (or router) in a new window.  usage: scripts\restart-node.bat m1   (or: router)
cd /d "%~dp0.."
if "%1"=="router" (start "router" cmd /k scripts\start-router.bat) else (start "node-%1" cmd /k scripts\start-node.bat %1)
