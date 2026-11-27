@echo off
REM Starts the router / 2PC coordinator in this window.
cd /d "%~dp0.."
title router
java -Xms16m -Xmx96m -XX:+UseSerialGC -cp out com.inventory.router.Router config\cluster.conf
