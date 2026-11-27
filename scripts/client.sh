#!/usr/bin/env bash
# Client. No args = interactive shell.  -c "CMD" = one command.  -f file = command file.  loadgen [sec] [ops/s]
source "$(dirname "$0")/env.sh"
exec java -Xmx64m -XX:TieredStopAtLevel=1 -cp out com.inventory.client.Client "$@"
