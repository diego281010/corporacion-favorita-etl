import io
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from sqlalchemy import create_engine, text

from config.config import RUTAS

try:
    from config.config import construir_url_base_datos
except ImportError:
    construir_url_base_datos = None

try:
    from config.config import URL_BASE_DATOS
except ImportError:
    URL_BASE_DATOS = None


RUTA_EDA = RUTAS.get(
    "eda_profundo",
    RUTAS["processed"] / "eda_profundo",
)

RUTA_METRICAS = (
    RUTAS["processed"] / "exportacion_postgres_metricas.json"
)

ESQUEMA_POSTGRES = os.getenv("DB_SCHEMA", "public")
TAMANIO_LOTE = int(os.getenv("DB_CHUNK_SIZE", "50000"))

TABLAS_PARQUET = {
    "consolidado": RUTAS["processed"] / "consolidado.parquet",
    "ventas_por_familia": RUTA_EDA / "ventas_por_familia.parquet",
    "ventas_por_tienda": RUTA_EDA / "ventas_por_tienda.parquet",
    "ventas_por_ciudad": RUTA_EDA / "ventas_por_ciudad.parquet",
    "ventas_por_provincia": RUTA_EDA / "ventas_por_provincia.parquet",
    "ventas_por_anio": RUTA_EDA / "ventas_por_anio.parquet",
    "ventas_por_mes": RUTA_EDA / "ventas_por_mes.parquet",
    "feriados_vs_dias_normales": (
        RUTA_EDA / "feriados_vs_dias_normales.parquet"
    ),
    "feriados_ventana_tres_dias": (
        RUTA_EDA / "feriados_ventana_tres_dias.parquet"
    ),
    "feriados_sensibilidad_familias": (
        RUTA_EDA / "feriados_sensibilidad_familias.parquet"
    ),
    "promociones_general": RUTA_EDA / "promociones_general.parquet",
    "promociones_por_familia": (
        RUTA_EDA / "promociones_por_familia.parquet"
    ),
    "petroleo_ventas_mensuales": (
        RUTA_EDA / "petroleo_ventas_mensuales.parquet"
    ),
    "petroleo_variaciones_2015_2016": (
        RUTA_EDA / "petroleo_variaciones_2015_2016.parquet"
    ),
    "petroleo_lags": RUTA_EDA / "petroleo_lags.parquet",
    "petroleo_sensibilidad_ciudades": (
        RUTA_EDA / "petroleo_sensibilidad_ciudades.parquet"
    ),
    "transacciones_ventas_diarias": (
        RUTA_EDA / "transacciones_ventas_diarias.parquet"
    ),
    "transacciones_por_tienda": (
        RUTA_EDA / "transacciones_por_tienda.parquet"
    ),
    "transacciones_clasificacion_ticket": (
        RUTA_EDA / "transacciones_clasificacion_ticket.parquet"
    ),
}

INDICES = {
    "consolidado": [
        ("date",),
        ("store_nbr",),
        ("family",),
    ],
    "ventas_por_tienda": [("store_nbr",)],
    "ventas_por_mes": [("anio", "mes")],
    "feriados_ventana_tres_dias": [
        ("family",),
        ("dias_relativo_feriado",),
    ],
    "promociones_por_familia": [("family",)],
    "petroleo_ventas_mensuales": [("anio", "mes")],
    "petroleo_sensibilidad_ciudades": [("city",)],
    "transacciones_por_tienda": [("store_nbr",)],
}


def obtener_url_base_datos():
    if construir_url_base_datos is not None:
        return construir_url_base_datos()

    if URL_BASE_DATOS:
        return URL_BASE_DATOS

    raise RuntimeError(
        "No se encontró construir_url_base_datos() ni URL_BASE_DATOS "
        "en config/config.py"
    )


def validar_identificador(nombre):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", nombre):
        raise ValueError(f"Identificador PostgreSQL inválido: {nombre}")


def citar_identificador(nombre):
    validar_identificador(nombre)
    return f'"{nombre}"'


def nombre_tabla_completo(nombre_tabla):
    return (
        f"{citar_identificador(ESQUEMA_POSTGRES)}."
        f"{citar_identificador(nombre_tabla)}"
    )


def tipo_postgres(tipo_polars):
    nombre = str(tipo_polars)

    if nombre in {"Int8", "Int16", "Int32", "UInt8", "UInt16"}:
        return "INTEGER"

    if nombre in {"Int64", "UInt32"}:
        return "BIGINT"

    if nombre == "UInt64":
        return "NUMERIC(20, 0)"

    if nombre == "Float32":
        return "REAL"

    if nombre == "Float64":
        return "DOUBLE PRECISION"

    if nombre == "Boolean":
        return "BOOLEAN"

    if nombre == "Date":
        return "DATE"

    if nombre == "Time":
        return "TIME"

    if nombre.startswith("Datetime"):
        if "time_zone=" in nombre and "time_zone=None" not in nombre:
            return "TIMESTAMP WITH TIME ZONE"
        return "TIMESTAMP WITHOUT TIME ZONE"

    if nombre.startswith("Duration"):
        return "INTERVAL"

    if nombre.startswith("Decimal"):
        coincidencia = re.search(
            r"precision=(\d+).*scale=(\d+)",
            nombre,
        )
        if coincidencia:
            precision, escala = coincidencia.groups()
            return f"NUMERIC({precision}, {escala})"
        return "NUMERIC"

    if nombre in {"String", "Categorical", "Enum"}:
        return "TEXT"

    if nombre == "Binary":
        return "BYTEA"

    if nombre == "Null":
        return "TEXT"

    raise TypeError(
        f"No existe una conversión PostgreSQL para el tipo Polars {nombre}"
    )


def obtener_esquema_parquet(ruta):
    if hasattr(pl, "read_parquet_schema"):
        return pl.read_parquet_schema(ruta)

    return pl.scan_parquet(ruta).collect_schema()


def recolectar_lazy(lazy_frame):
    try:
        return lazy_frame.collect(engine="streaming")
    except TypeError:
        return lazy_frame.collect(streaming=True)


def contar_filas_parquet(ruta):
    resultado = recolectar_lazy(
        pl.scan_parquet(ruta).select(pl.len().alias("cantidad"))
    )
    return int(resultado.item())


def iterar_lotes_parquet(ruta, cantidad_filas):
    lazy_frame = pl.scan_parquet(ruta)

    if hasattr(lazy_frame, "collect_batches"):
        try:
            lotes = lazy_frame.collect_batches(
                chunk_size=TAMANIO_LOTE,
                engine="streaming",
            )
        except TypeError:
            lotes = lazy_frame.collect_batches(
                chunk_size=TAMANIO_LOTE,
            )

        for lote in lotes:
            if lote.height > 0:
                yield lote
        return

    for inicio in range(0, cantidad_filas, TAMANIO_LOTE):
        lote = recolectar_lazy(
            pl.scan_parquet(ruta).slice(inicio, TAMANIO_LOTE)
        )

        if lote.height > 0:
            yield lote


def construir_create_table(nombre_tabla, esquema):
    columnas = []

    for nombre_columna, tipo in esquema.items():
        columna = citar_identificador(nombre_columna)
        columnas.append(f"{columna} {tipo_postgres(tipo)}")

    columnas_sql = ",\n    ".join(columnas)
    tabla = nombre_tabla_completo(nombre_tabla)

    return f"CREATE TABLE {tabla} (\n    {columnas_sql}\n)"


def copiar_lote(cursor, nombre_tabla, lote):
    columnas = lote.columns
    columnas_sql = ", ".join(
        citar_identificador(columna) for columna in columnas
    )
    tabla = nombre_tabla_completo(nombre_tabla)

    buffer = io.StringIO()
    lote.write_csv(
        buffer,
        include_header=False,
        null_value="\\N",
    )
    buffer.seek(0)

    consulta_copy = (
        f"COPY {tabla} ({columnas_sql}) "
        "FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
    )
    cursor.copy_expert(consulta_copy, buffer)


def crear_indices(cursor, nombre_tabla, columnas_tabla):
    indices_tabla = INDICES.get(nombre_tabla, [])
    columnas_disponibles = set(columnas_tabla)

    for columnas_indice in indices_tabla:
        if not set(columnas_indice).issubset(columnas_disponibles):
            continue

        sufijo = "_".join(columnas_indice)
        nombre_indice = f"idx_{nombre_tabla}_{sufijo}"
        columnas_sql = ", ".join(
            citar_identificador(columna)
            for columna in columnas_indice
        )

        cursor.execute(
            f"CREATE INDEX {citar_identificador(nombre_indice)} "
            f"ON {nombre_tabla_completo(nombre_tabla)} "
            f"({columnas_sql})"
        )


def exportar_tabla(engine, nombre_tabla, ruta_parquet):
    inicio = time.perf_counter()
    esquema = obtener_esquema_parquet(ruta_parquet)
    cantidad_filas = contar_filas_parquet(ruta_parquet)

    conexion = engine.raw_connection()
    cursor = conexion.cursor()

    try:
        cursor.execute(
            f"CREATE SCHEMA IF NOT EXISTS "
            f"{citar_identificador(ESQUEMA_POSTGRES)}"
        )
        cursor.execute(
            f"DROP TABLE IF EXISTS "
            f"{nombre_tabla_completo(nombre_tabla)}"
        )
        cursor.execute(construir_create_table(nombre_tabla, esquema))

        filas_copiadas = 0
        for numero_lote, lote in enumerate(
            iterar_lotes_parquet(ruta_parquet, cantidad_filas),
            start=1,
        ):
            copiar_lote(cursor, nombre_tabla, lote)
            filas_copiadas += lote.height

            print(
                f"   {nombre_tabla}: lote {numero_lote}, "
                f"{filas_copiadas:,}/{cantidad_filas:,} filas"
            )

        crear_indices(cursor, nombre_tabla, esquema.keys())
        cursor.execute(f"ANALYZE {nombre_tabla_completo(nombre_tabla)}")
        conexion.commit()

    except Exception:
        conexion.rollback()
        raise

    finally:
        cursor.close()
        conexion.close()

    duracion = round(time.perf_counter() - inicio, 2)

    return {
        "tabla": nombre_tabla,
        "archivo": str(ruta_parquet),
        "filas": cantidad_filas,
        "columnas": len(esquema),
        "duracion_segundos": duracion,
    }


def validar_archivos_salida():
    faltantes = [
        str(ruta)
        for ruta in TABLAS_PARQUET.values()
        if not Path(ruta).exists()
    ]

    if faltantes:
        detalle = "\n- ".join(faltantes)
        raise FileNotFoundError(
            "Faltan archivos requeridos para exportar a PostgreSQL:\n"
            f"- {detalle}\n"
            "Ejecuta primero consolidar y todos los scripts del EDA profundo."
        )


def comprobar_conexion(engine):
    with engine.connect() as conexion:
        conexion.execute(text("SELECT 1"))


def guardar_metricas(metricas):
    RUTA_METRICAS.parent.mkdir(parents=True, exist_ok=True)

    with open(RUTA_METRICAS, "w", encoding="utf-8") as archivo:
        json.dump(
            metricas,
            archivo,
            indent=4,
            ensure_ascii=False,
        )


def exportar_postgres():
    if TAMANIO_LOTE <= 0:
        raise ValueError("DB_CHUNK_SIZE debe ser mayor que cero")

    validar_identificador(ESQUEMA_POSTGRES)
    validar_archivos_salida()

    print("EXPORTACIÓN A POSTGRESQL")
    print("-" * 60)
    print(f"Esquema: {ESQUEMA_POSTGRES}")
    print(f"Tamaño de lote: {TAMANIO_LOTE:,} filas")

    engine = create_engine(
        obtener_url_base_datos(),
        pool_pre_ping=True,
    )

    inicio_total = time.perf_counter()
    comprobar_conexion(engine)
    print("Conexión a PostgreSQL validada correctamente")

    tablas_exportadas = []

    try:
        for nombre_tabla, ruta_parquet in TABLAS_PARQUET.items():
            print(f"\nExportando {nombre_tabla}...")
            resultado = exportar_tabla(
                engine,
                nombre_tabla,
                ruta_parquet,
            )
            tablas_exportadas.append(resultado)
            print(
                f"Tabla {nombre_tabla} exportada: "
                f"{resultado['filas']:,} filas en "
                f"{resultado['duracion_segundos']:.2f} segundos"
            )

    finally:
        engine.dispose()

    metricas = {
        "estado": "ok",
        "fecha_exportacion_utc": datetime.now(timezone.utc).isoformat(),
        "esquema": ESQUEMA_POSTGRES,
        "cantidad_tablas": len(tablas_exportadas),
        "total_filas": sum(
            tabla["filas"] for tabla in tablas_exportadas
        ),
        "duracion_total_segundos": round(
            time.perf_counter() - inicio_total,
            2,
        ),
        "tablas": tablas_exportadas,
    }

    guardar_metricas(metricas)

    print("\nEXPORTACIÓN FINALIZADA")
    print(f"Tablas creadas: {metricas['cantidad_tablas']}")
    print(f"Filas exportadas: {metricas['total_filas']:,}")
    print(
        "Duración total: "
        f"{metricas['duracion_total_segundos']:.2f} segundos"
    )
    print(f"Métricas guardadas en: {RUTA_METRICAS}")

    return metricas


def ejecutar_exportacion_postgres():
    return exportar_postgres()


if __name__ == "__main__":
    ejecutar_exportacion_postgres()

