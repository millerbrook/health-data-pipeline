#!/usr/bin/env bash
set -euo pipefail

# Get script directory
if [ "${BASH_SOURCE+x}" = x ]; then
  _script_path="${BASH_SOURCE[0]}"
elif [ -n "${0:-}" ] && [ "$0" != "bash" ]; then
  _script_path="$0"
else
  _script_path="."
fi

PROJECT_DIR="$(cd "$(dirname "$_script_path")" && pwd)"
cd "$PROJECT_DIR"

# Create venv if missing
if [ ! -d .venv ]; then
  echo "[INFO] Creating virtual environment..."
  uv venv .venv -p 3.11
fi

# Activate venv
. .venv/bin/activate

# Load environment variables
if [ -f .env ]; then
  set -a; . .env; set +a
fi

echo "[INFO] Project dir: $PROJECT_DIR"
echo "[INFO] Python: $(python -V 2>/dev/null || echo 'python missing')"
echo "[INFO] Airflow: $(command -v airflow || echo 'NOT INSTALLED')"

# Set Airflow environment variables
export AIRFLOW_HOME="$PROJECT_DIR/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_DIR/dags"
export AIRFLOW__CORE__PLUGINS_FOLDER="$PROJECT_DIR/plugins"

# Run command or start shell
exec "${@:-bash}"