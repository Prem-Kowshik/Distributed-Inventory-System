# Helpers shared by the demo scripts (bash). Requires the cluster started with scripts/start-cluster.sh
source "$(dirname "${BASH_SOURCE[0]}")/../scripts/env.sh"
C()     { java -Xmx64m -XX:TieredStopAtLevel=1 -cp out com.inventory.client.Client "$@" 2>/dev/null; }
title() { echo; echo "======================================================================"; echo "  $*"; echo "======================================================================"; }
step()  { echo; echo ">>> $*"; }
run()   { echo "\$ $*"; C -c "$*" | sed 's/^/  /'; }
pause() { [ -n "$NOPAUSE" ] && return 0; read -rp "    [Enter to continue] " _ || true; }
wait_s(){ echo "    ...waiting $1 s"; sleep "$1"; }
primary_of() { C -c STATUS | awk -v w="$1" '$1==w && $2 ~ /\*$/ {sub(/\*$/,"",$2); print $2}'; }
backups_of() { C -c STATUS | awk -v w="$1" '$1==w && $2 !~ /\*$/ && $3=="UP" {print $2}'; }
mark()  { for id in "$@"; do wc -l < logs/$id.log > run/.mark.$id; done; }
# logs <id> [regex] : print the log lines of <id> written since the last `mark <id>`
logs()  { local from=$(( $(cat run/.mark.$1 2>/dev/null || echo 0) + 1 ))
          echo "    [log $1]"; tail -n +$from logs/$1.log | grep -v JAVA_TOOL | grep -E "${2:-.}" | tail -n ${3:-12} | sed 's/^/    | /'; }
kill9() { scripts/kill-node.sh "$1" | sed 's/^/    /'; }
start() { scripts/restart-node.sh "$1" | sed 's/^/    /'; }
C -c STATUS | grep -q '^OK' || { echo "Cluster is not running. Run:  demo/00-setup.sh"; exit 1; }
