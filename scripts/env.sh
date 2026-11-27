# shared settings for the bash scripts
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
CONF="${CONF:-config/cluster.conf}"
JAVA_OPTS="${JAVA_OPTS:--Xms16m -Xmx96m -XX:+UseSerialGC -XX:TieredStopAtLevel=1}"
node_ids() { awk '$1=="node"{print $3}' "$CONF"; }
