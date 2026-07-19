import math

import polars as pl
from config.config import RUTAS

INPUT_DIR = RUTAS["processed"]
OUTPUT_DIR = RUTAS["processed"] / "eda_profundo"


def cargar_consolidado():
    archivo = INPUT_DIR / "consolidado.parquet"

    if not archivo.exists():
        raise FileNotFoundError(
            f"No existe el archivo consolidado: {archivo}. "
            "Ejecuta primero la tarea consolidar."
        )

    return pl.read_parquet(archivo)


def guardar_resultado(df, nombre_archivo):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    archivo_salida = OUTPUT_DIR / nombre_archivo
    df.write_parquet(archivo_salida)
    return archivo_salida


def analisis_transacciones(df=None):
    if df is None:
        df = cargar_consolidado()

    columnas_necesarias = [
        "date",
        "store_nbr",
        "sales",
        "transactions",
    ]
    columnas_faltantes = [
        columna for columna in columnas_necesarias if columna not in df.columns
    ]

    if columnas_faltantes:
        raise ValueError(
            "No se puede realizar el análisis de transacciones. "
            f"Faltan las columnas: {columnas_faltantes}"
        )

    print("TRANSACCIONES")
    print("-" * 60)

    # transactions se repite por cada familia de una misma tienda y fecha.
    # Se valida que el valor repetido sea consistente antes de reducirlo.
    transacciones_inconsistentes = (
        df.group_by(["store_nbr", "date"])
        .agg(pl.col("transactions").n_unique().alias("cantidad_valores_transacciones"))
        .filter(pl.col("cantidad_valores_transacciones") > 1)
    )

    if transacciones_inconsistentes.height > 0:
        raise ValueError(
            "Existen tiendas y fechas con más de un valor diferente de "
            "transactions en el consolidado."
        )

    ventas_transacciones_diarias = (
        df.group_by(["store_nbr", "date"])
        .agg(
            [
                pl.col("sales").sum().alias("ventas_diarias"),
                pl.col("transactions").first().alias("transactions"),
            ]
        )
        # En el consolidado los faltantes fueron reemplazados por cero.
        # Se excluyen para no tratarlos como observaciones reales.
        .filter(pl.col("transactions") > 0)
        .sort(["store_nbr", "date"])
    )

    if ventas_transacciones_diarias.is_empty():
        raise ValueError(
            "No existen registros válidos de transacciones para el análisis."
        )

    print("\n5.1 RELACIÓN TRANSACCIONES VS VENTAS")

    correlacion_general = ventas_transacciones_diarias.select(
        pl.corr("transactions", "ventas_diarias").alias("correlacion")
    ).item()

    if correlacion_general is not None and math.isfinite(correlacion_general):
        print(
            "Correlación diaria general entre transacciones y ventas: "
            f"{correlacion_general:.3f}"
        )
    else:
        print("No fue posible calcular la correlación general")
        correlacion_general = None

    transacciones_por_tienda = (
        ventas_transacciones_diarias.group_by("store_nbr")
        .agg(
            [
                pl.col("transactions").sum().alias("total_transacciones"),
                pl.col("ventas_diarias").sum().alias("ventas_totales"),
                pl.col("transactions").mean().alias("transacciones_promedio_diarias"),
                pl.col("ventas_diarias").mean().alias("ventas_promedio_diarias"),
                pl.corr("transactions", "ventas_diarias").alias(
                    "correlacion_transacciones_ventas"
                ),
                pl.len().alias("dias_analizados"),
            ]
        )
        .with_columns(
            pl.when(pl.col("total_transacciones") > 0)
            .then(pl.col("ventas_totales") / pl.col("total_transacciones"))
            .otherwise(None)
            .alias("ticket_promedio")
        )
    )

    tiendas_mayor_correlacion = transacciones_por_tienda.filter(
        pl.col("correlacion_transacciones_ventas").is_not_null()
        & pl.col("correlacion_transacciones_ventas").is_finite()
    ).sort("correlacion_transacciones_ventas", descending=True)

    print("\nTiendas con mayor relación positiva:")
    for i, row in enumerate(
        tiendas_mayor_correlacion.head(5).iter_rows(named=True),
        start=1,
    ):
        print(
            f"{i}. Tienda {row['store_nbr']}: "
            f"r = {row['correlacion_transacciones_ventas']:.3f}"
        )

    print("\n5.2 CLASIFICACIÓN DE TICKET PROMEDIO")

    mediana_ventas_diarias = transacciones_por_tienda.select(
        pl.col("ventas_promedio_diarias").median()
    ).item()

    mediana_transacciones_diarias = transacciones_por_tienda.select(
        pl.col("transacciones_promedio_diarias").median()
    ).item()

    clasificacion_tiendas = transacciones_por_tienda.with_columns(
        pl.when(
            (pl.col("ventas_promedio_diarias") >= mediana_ventas_diarias)
            & (
                pl.col("transacciones_promedio_diarias")
                <= mediana_transacciones_diarias
            )
        )
        .then(pl.lit("Ticket alto"))
        .when(
            (pl.col("ventas_promedio_diarias") <= mediana_ventas_diarias)
            & (
                pl.col("transacciones_promedio_diarias")
                >= mediana_transacciones_diarias
            )
        )
        .then(pl.lit("Ticket bajo"))
        .otherwise(pl.lit("Intermedio"))
        .alias("clasificacion_ticket")
    )

    tiendas_ticket_alto = clasificacion_tiendas.filter(
        pl.col("clasificacion_ticket") == "Ticket alto"
    ).sort("ticket_promedio", descending=True)

    tiendas_ticket_bajo = clasificacion_tiendas.filter(
        pl.col("clasificacion_ticket") == "Ticket bajo"
    ).sort("ticket_promedio")

    print(
        "\nTicket alto: relativamente pocas transacciones diarias "
        "y ventas diarias altas"
    )
    if tiendas_ticket_alto.is_empty():
        print("No se encontraron tiendas en este cuadrante")
    else:
        for i, row in enumerate(
            tiendas_ticket_alto.head(10).iter_rows(named=True),
            start=1,
        ):
            print(
                f"{i}. Tienda {row['store_nbr']}: "
                f"ticket aproximado {row['ticket_promedio']:,.2f} "
                "sales/transacción, "
                f"ventas promedio diarias "
                f"{row['ventas_promedio_diarias']:,.2f}, "
                f"transacciones promedio diarias "
                f"{row['transacciones_promedio_diarias']:,.2f}"
            )

    print(
        "\nTicket bajo: relativamente muchas transacciones diarias "
        "y ventas diarias bajas"
    )
    if tiendas_ticket_bajo.is_empty():
        print("No se encontraron tiendas en este cuadrante")
    else:
        for i, row in enumerate(
            tiendas_ticket_bajo.head(10).iter_rows(named=True),
            start=1,
        ):
            print(
                f"{i}. Tienda {row['store_nbr']}: "
                f"ticket aproximado {row['ticket_promedio']:,.2f} "
                "sales/transacción, "
                f"ventas promedio diarias "
                f"{row['ventas_promedio_diarias']:,.2f}, "
                f"transacciones promedio diarias "
                f"{row['transacciones_promedio_diarias']:,.2f}"
            )

    archivo_diario = guardar_resultado(
        ventas_transacciones_diarias,
        "transacciones_ventas_diarias.parquet",
    )
    archivo_tiendas = guardar_resultado(
        transacciones_por_tienda,
        "transacciones_por_tienda.parquet",
    )
    archivo_clasificacion = guardar_resultado(
        clasificacion_tiendas,
        "transacciones_clasificacion_ticket.parquet",
    )

    print("\nResultados guardados:")
    print(f"- {archivo_diario}")
    print(f"- {archivo_tiendas}")
    print(f"- {archivo_clasificacion}")

    return {
        "estado": "ok",
        "archivos": {
            "datos_diarios": str(archivo_diario),
            "resumen_tiendas": str(archivo_tiendas),
            "clasificacion_ticket": str(archivo_clasificacion),
        },
        "correlacion_general": correlacion_general,
        "tienda_mayor_correlacion": (
            int(tiendas_mayor_correlacion.get_column("store_nbr")[0])
            if tiendas_mayor_correlacion.height > 0
            else None
        ),
        "cantidad_tiendas_ticket_alto": tiendas_ticket_alto.height,
        "cantidad_tiendas_ticket_bajo": tiendas_ticket_bajo.height,
    }


def ejecutar_transacciones():
    return analisis_transacciones()


if __name__ == "__main__":
    ejecutar_transacciones()
