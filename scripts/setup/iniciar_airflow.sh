#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ -z "${VIRTUAL_ENV:-}" ]]; then
    echo "Activa primero el entorno virtual del proyecto."
    echo "Ejemplo: source .venv/bin/activate"
    exit 1
fi

export AIRFLOW_HOME="${AIRFLOW_HOME:-$PROJECT_ROOT/airflow_home}"
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_ROOT/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

cd "$PROJECT_ROOT"
exec airflow standalone
