#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${SPARK_VENV:-$HOME/spark-env}/bin/activate"
export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}"
export PYSPARK_DRIVER_PYTHON="$VIRTUAL_ENV/bin/python"
export PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python"
exec "$SPARK_HOME/bin/spark-submit" --master "${SPARK_MASTER:-local[2]}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" "$project_root/scripts/test_silver.py"
