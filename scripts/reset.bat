@echo off
REM Stops the cluster and wipes all data, logs and pid files.
cd /d "%~dp0.."
call scripts\stop-cluster.bat
if exist data rmdir /s /q data
if exist logs rmdir /s /q logs
if exist run rmdir /s /q run
echo cluster data wiped
