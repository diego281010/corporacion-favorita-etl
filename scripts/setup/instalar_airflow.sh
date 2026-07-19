#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AIRFLOW_VERSION="${AIRFLOW_VERSION:-3.3.0}"
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

if [[ -z "${VIRTUAL_ENV:-}" ]]; then
    echo "Activa primero el entorno virtual del proyecto."
    echo "Ejemplo: source .venv/bin/activate"
    exit 1
fi

cd "$PROJECT_ROOT"
python -m pip install --upgrade pip
python -m pip install \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r requirements.txt \
    --constraint "$CONSTRAINT_URL"

mkdir -p "$PROJECT_ROOT/airflow_home"

echo
echo "Airflow ${AIRFLOW_VERSION} instalado para Python ${PYTHON_VERSION}."
echo "Inicia el entorno con: scripts/setup/iniciar_airflow.sh"
