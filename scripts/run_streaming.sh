set -euo pipefail

if [[ -f .env.pyspark ]]; then
  echo "  Loading .env.pyspark"
  source .env.pyspark
else
  echo "  WARNING: .env.pyspark not found."
  echo "    You must run:  bash scripts/setup_kafka_connectors.sh"
  echo "    Without this, Spark will NOT have the Kafka connector jars."
fi

CFG="${1:-configs/stream_config.yml}"

if [[ ! -f "$CFG" ]]; then
  echo " ERROR: Config file not found at: $CFG" >&2
  exit 1
fi

echo " Starting Spark Streaming Job"
echo " Using config: $CFG"
echo

python apps/streaming_kafka.py --config "$CFG"