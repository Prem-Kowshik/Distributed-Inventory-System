@echo off
REM Simulates a torn WAL write on a STOPPED node.  usage: scripts\corrupt-wal.bat d2
cd /d "%~dp0.."
<nul set /p ="999	9	9	RESTOCK LAPTOP 50" >> data\%1\wal.log
echo appended a torn record to data\%1\wal.log
