@echo off
REM Starts ONE storage node in this window.   usage: scripts\start-node.bat m1
cd /d "%~dp0.."
title node-%1
java -Xms16m -Xmx96m -XX:+UseSerialGC -cp out com.inventory.node.StorageNode config\cluster.conf %1
