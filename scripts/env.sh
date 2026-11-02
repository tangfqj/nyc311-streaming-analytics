#!/usr/bin/env bash
# Source from the repository root; never changes the user's global environment.
if [ -x "$PWD/data/runtime/java17/Contents/Home/bin/java" ]; then
  export JAVA_HOME="$PWD/data/runtime/java17/Contents/Home"
fi
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
export MPLCONFIGDIR="$PWD/data/runtime/matplotlib"
export XDG_CACHE_HOME="$PWD/data/runtime/cache"
export DATABASE_URL="${DATABASE_URL:-postgresql://nyc311:nyc311_local@localhost:55432/nyc311}"

export TZ=UTC
