@echo off
REM Starts the web demo console (http://localhost:8080). Builds first if needed.
REM   usage: scripts\web.bat [httpPort]
cd /d "%~dp0\.."
if not exist out\com\inventory\web\WebConsole.class call build.bat
set PORT=%1
if "%PORT%"=="" set PORT=8080
java -Xmx128m -cp out com.inventory.web.WebConsole config\cluster.conf %PORT%
