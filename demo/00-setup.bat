@echo off
REM Wipes data, builds, starts the cluster windows and loads the catalogue.
cd /d "%~dp0.."
call scripts\reset.bat
call build.bat
call scripts\start-cluster.bat
call scripts\client.bat -f demo\seed.txt
