import logging
import sys
from datetime import timedelta
from pathlib import Path

import pendulum

# Carga de dags desde la carpeta
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    # Interfaz pública de Airflow 3.x.
    from airflow.sdk import DAG, TaskGroup
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    # Compatibilidad para Airflow 2.x
    from airflow import DAG
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator
    from airflow.utils.task_group import TaskGroup

from scripts.consolidate.consolidar import ejecutar_consolidacion
from scripts.eda.eda_inicial import ejecutar_eda_inicial
from scripts.eda.estacionalidad_feriados import ejecutar_feriados
from scripts.eda.petroleo_economia import ejecutar_petroleo
from scripts.eda.promociones import ejecutar_promociones
from scripts.eda.transacciones import ejecutar_transacciones
from scripts.eda.ventas_generales import ejecutar_ventas_generales
from scripts.extract.cargar_datos import ejecutar_carga
from scripts.transform.limpiar_datos import ejecutar_limpieza
from scripts.load.exportar_postgres import ejecutar_exportacion_postgres


def registrar_error(context):
    tarea = context.get("task_instance")
    excepcion = context.get("exception")
    logging.error(
        "Falló la tarea %s del DAG favorita_pipelina: %s",
        getattr(tarea, "task_id", "desconocida"),
        excepcion,
    )


CONFIGURACION_TAREAS = {
    "owner": "equipo_favorita",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": registrar_error,
}

with DAG(
    dag_id="favorita_pipeline",
    description="Pipeline ETL y EDA de ventas de Corporación Favorita",
    default_args=CONFIGURACION_TAREAS,
    start_date=pendulum.datetime(2026, 7, 5, tz="America/Guayaquil"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["favorita", "etl", "polars", "eda"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")

    cargar = PythonOperator(
        task_id="cargar_datos",
        python_callable=ejecutar_carga,
        do_xcom_push=False,
    )
    eda_inicial = PythonOperator(
        task_id="eda_inicial",
        python_callable=ejecutar_eda_inicial,
        do_xcom_push=False,
    )
    limpiar = PythonOperator(
        task_id="limpiar_datos",
        python_callable=ejecutar_limpieza,
        do_xcom_push=False,
    )
    consolidar = PythonOperator(
        task_id="consolidar",
        python_callable=ejecutar_consolidacion,
        do_xcom_push=False,
    )
    with TaskGroup(group_id="eda_profundo") as eda_profundo:
        ventas_generales = PythonOperator(
            task_id="ventas_generales",
            python_callable=ejecutar_ventas_generales,
            do_xcom_push=False,
        )
        estacionalidad = PythonOperator(
            task_id="estacionalidad_feriados",
            python_callable=ejecutar_feriados,
            do_xcom_push=False,
        )
        promociones = PythonOperator(
            task_id="promociones",
            python_callable=ejecutar_promociones,
            do_xcom_push=False,
        )
        petroleo_economia = PythonOperator(
            task_id="petroleo_economia",
            python_callable=ejecutar_petroleo,
            do_xcom_push=False,
        )
        transacciones = PythonOperator(
            task_id="transacciones",
            python_callable=ejecutar_transacciones,
            do_xcom_push=False,
        )
    fin_eda = EmptyOperator(task_id="fin_eda_profundo")
    exportar_postgres = PythonOperator(
            task_id="exportar_postgres",
            python_callable=ejecutar_exportacion_postgres,
            do_xcom_push=False,
            )

    inicio >> cargar >> eda_inicial >> limpiar >> consolidar >> eda_profundo >> fin_eda >> exportar_postgres
