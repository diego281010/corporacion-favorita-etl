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


def imprimir_familia_promocion(indice, row):
    print(
        f"{indice}. {row['family']}:"
        f"sin promoción {row['venta_promedio_sin_promocion']:,.2f}, "
        f"con promoción {row['venta_promedio_con_promocion']:,.2f}, "
        f"cambio {row['incremento_porcentual']:+.2f}%"
    )
    print(
        f"Registros: {row['registros_sin_promocion']:,} sin promoción y "
        f"{row['registros_con_promocion']:,} con promoción"
    )


def analisis_promociones(df=None):
    if df is None:
        df = cargar_consolidado()

    resultados = {}

    columnas_necesarias = ["family", "sales", "onpromotion"]
    columnas_faltantes = [
        columna for columna in columnas_necesarias if columna not in df.columns
    ]

    if columnas_faltantes:
        raise ValueError(
            f"No se puede realizar el análisis de promociones. "
            f"Faltan las columnas: {columnas_faltantes}"
        )

    resultados = {}

    print("PROMOCIONES")
    print("-" * 60)

    df_promociones = df.with_columns(
        (pl.col("onpromotion").fill_null(0) > 0).alias("en_promocion")
    )

    # 3.1 Comparación general con y sin promoción
    print("\n3.1 COMPARACIÓN GENERAL CON/SIN PROMOCIÓN")

    ventas_promo_general = (
        df_promociones.group_by("en_promocion")
        .agg(
            [
                pl.col("sales").sum().alias("ventas_totales"),
                pl.col("sales").mean().alias("venta_promedio"),
                pl.col("sales").median().alias("venta_mediana"),
                pl.len().alias("cantidad_registros"),
            ]
        )
        .sort("en_promocion", descending=True)
    )

    for row in ventas_promo_general.iter_rows(named=True):
        tipo = "Con promoción" if row["en_promocion"] else "Sin promoción"
        print(f"{tipo}:")
        print(f"   Venta promedio: ${row['venta_promedio']:,.2f}")
        print(f"   Venta mediana: ${row['venta_mediana']:,.2f}")
        print(f"   Registros: {row['cantidad_registros']:,}")

    resultados["comparacion_general_promociones"] = ventas_promo_general.to_dicts()

    # 3.2 Comparación y efecto por familia
    print("\n3.2 EFECTO DE PROMOCIONES POR FAMILIA")

    efecto_promo = (
        df_promociones.group_by("family")
        .agg(
            [
                pl.col("sales")
                .filter(pl.col("en_promocion"))
                .mean()
                .alias("venta_promedio_con_promocion"),
                pl.col("sales")
                .filter(~pl.col("en_promocion"))
                .mean()
                .alias("venta_promedio_sin_promocion"),
                pl.col("sales")
                .filter(pl.col("en_promocion"))
                .count()
                .alias("registros_con_promocion"),
                pl.col("sales")
                .filter(~pl.col("en_promocion"))
                .count()
                .alias("registros_sin_promocion"),
            ]
        )
        .with_columns(
            (
                pl.col("venta_promedio_con_promocion")
                - pl.col("venta_promedio_sin_promocion")
            ).alias("diferencia_promedio")
        )
        .with_columns(
            pl.when(
                pl.col("venta_promedio_con_promocion").is_not_null()
                & pl.col("venta_promedio_sin_promocion").is_not_null()
                & (pl.col("venta_promedio_sin_promocion") != 0)
            )
            .then(
                pl.col("diferencia_promedio")
                / pl.col("venta_promedio_sin_promocion")
                * 100
            )
            .otherwise(None)
            .alias("incremento_porcentual")
        )
        .filter(
            pl.col("incremento_porcentual").is_not_null()
            & pl.col("incremento_porcentual").is_finite()
        )
        .with_columns(pl.col("incremento_porcentual").abs().alias("efecto_absoluto"))
        .sort("incremento_porcentual", descending=True)
    )

    familias_mayor_incremento = efecto_promo.head(10)

    familias_efecto_negativo = efecto_promo.filter(
        pl.col("incremento_porcentual") < 0
    ).sort("incremento_porcentual")

    familias_menor_incremento = (
        efecto_promo.filter(pl.col("incremento_porcentual") >= 0)
        .sort("incremento_porcentual")
        .head(10)
    )

    print("\nFamilias con mayor incremento asociado a promociones:")
    for i, row in enumerate(
        familias_mayor_incremento.head(5).iter_rows(named=True), start=1
    ):
        imprimir_familia_promocion(i, row)

    print("\nFamilias con efecto negativo asociado a promociones:")
    if familias_efecto_negativo.is_empty():
        print("No se encontraron familias comparables con efecto negativo")
    else:
        for i, row in enumerate(
            familias_efecto_negativo.head(5).iter_rows(named=True), start=1
        ):
            imprimir_familia_promocion(i, row)

    print("\nFamilias con menor incremento positivo asociado a promociones:")
    if familias_menor_incremento.is_empty():
        print("No se encontraron familias con incremento positivo")
    else:
        for i, row in enumerate(
            familias_menor_incremento.head(5).iter_rows(named=True), start=1
        ):
            imprimir_familia_promocion(i, row)

    archivo_general = guardar_resultado(
        ventas_promo_general,
        "promociones_general.parquet",
    )
    archivos_familias = guardar_resultado(
        efecto_promo,
        "promociones_por_familia.parquet",
    )

    print("\nResultados guardados:")
    print(f"- {archivo_general}")
    print(f"- {archivos_familias}")

    return {
        "estado": "ok",
        "archivos": {
            "comparacion_general": str(archivo_general),
            "efecto_por_familia": str(archivos_familias),
        },
        "filas": {
            "comparacion_general": ventas_promo_general.height,
            "efecto_por_familia": efecto_promo.height,
        },
        "familias_mayor_incremento": (
            familias_mayor_incremento.get_column("family")[0]
            if familias_mayor_incremento.height > 0
            else None
        ),
    }


def ejecutar_promociones():
    return analisis_promociones()


if __name__ == "__main__":
    ejecutar_promociones()
