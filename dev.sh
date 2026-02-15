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

# Create .env.local if it doesn't exist (for database credentials)
if [ ! -f .env.local ]; then
  echo "[INFO] Creating .env.local for local development..."
  cat > .env.local << 'ENVEOF'
export PGPASSWORD=airflow
export AIRFLOW_CONN_HEALTH_DB="postgresql://airflow:airflow@localhost:5433/airflow"
ENVEOF
  echo "[INFO] Created .env.local - customize if needed"
fi

# Load environment variables (local first, then .env)
if [ -f .env.local ]; then
  set -a; . .env.local; set +a
fi
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

# Start database if docker compose file exists
if [ -f docker-compose.db.yml ]; then
  echo "[INFO] 🐘 Starting PostgreSQL..."
  docker compose -f docker-compose.db.yml up -d >/dev/null 2>&1
  sleep 2
  if PGPASSWORD=airflow psql -h localhost -p 5433 -U airflow -d airflow -c "SELECT 1;" >/dev/null 2>&1; then
    echo "[INFO] ✅ Database connected"
  else
    echo "[WARN] ⚠️  Database connection failed - check if Docker is running"
  fi
fi

# Customize prompt to show (.venv)
export PS1="(.venv) \[\e[32m\]\u@\h\[\e[0m\]:\[\e[34m\]\w\[\e[0m\]\$ "

echo ""
echo "✨ Dev environment ready!"
echo "📚 Quick commands:"
echo "   airflow dags test health_data_pipeline 2026-02-15   # Run DAG"
echo "   streamlit run dashboard.py                          # Start dashboard"
echo "   airflow webserver --port 8080                       # Airflow UI"
echo ""

# Run command or start shell
# If no command provided, start bash with --rcfile to preserve our PS1
if [ $# -eq 0 ]; then
  # Create a temporary rc file that sources bashrc but preserves our settings
  TEMP_RC=$(mktemp)
  cat > "$TEMP_RC" << 'EOF'
# Source user's bashrc if it exists
if [ -f ~/.bashrc ]; then
  source ~/.bashrc
fi
# Restore our custom PS1
export PS1="(.venv) \[\e[32m\]\u@\h\[\e[0m\]:\[\e[34m\]\w\[\e[0m\]\$ "
EOF
  exec bash --rcfile "$TEMP_RC"
else
  exec "$@"
fi