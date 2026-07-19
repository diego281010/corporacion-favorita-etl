import polars as pl

from config.config import RUTAS

INPUT_DIR = RUTAS["processed"]
OUTPUT_DIR = RUTAS["eda_profundo"]


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


def analisis_ventas_generales(df=None):
    if df is None:
        df = cargar_consolidado()

    columnas_necesarias = ["date", "store_nbr", "family", "sales", "city", "state"]
    columnas_faltantes = [
        columna for columna in columnas_necesarias if columna not in df.columns
    ]
    if columnas_faltantes:
        raise ValueError(
            "No se puede realizar el análisis de ventas generales. "
            f"Faltan las columnas: {columnas_faltantes}"
        )

    print("VENTAS GENERALES")
    print("-" * 60)

    ventas_por_familia = (
        df.group_by("family")
        .agg(
            [
                pl.col("sales").sum().alias("ventas_totales"),
                pl.col("sales").mean().alias("venta_promedio"),
                pl.len().alias("numero_registros"),
            ]
        )
        .with_columns(
            (pl.col("ventas_totales") / pl.col("ventas_totales").sum() * 100).alias(
                "participacion_porcentual"
            )
        )
        .sort("ventas_totales", descending=True)
    )

    print("\n1.1 FAMILIAS CON MAYOR VOLUMEN")
    for i, row in enumerate(ventas_por_familia.head(10).iter_rows(named=True), start=1):
        print(
            f"{i}. {row['family']}: {row['ventas_totales']:,.2f} "
            f"({row['participacion_porcentual']:.2f}%)"
        )

    ventas_por_tienda = (
        df.group_by("store_nbr")
        .agg(
            [
                pl.col("sales").sum().alias("ventas_totales"),
                pl.col("sales").mean().alias("venta_promedio"),
            ]
        )
        .with_columns(
            [
                pl.col("ventas_totales")
                .rank(method="ordinal", descending=True)
                .alias("ranking_mayor_venta"),
                pl.col("ventas_totales")
                .rank(method="ordinal")
                .alias("ranking_menor_venta"),
            ]
        )
        .sort("ventas_totales", descending=True)
    )

    top_10_tiendas = ventas_por_tienda.head(10)
    bottom_10_tiendas = ventas_por_tienda.sort("ventas_totales").head(10)

    print("\n1.2 TOP 10 TIENDAS")
    for i, row in enumerate(top_10_tiendas.iter_rows(named=True), start=1):
        print(f"{i}. Tienda {row['store_nbr']}: {row['ventas_totales']:,.2f}")

    print("\nBOTTOM 10 TIENDAS")
    for i, row in enumerate(bottom_10_tiendas.iter_rows(named=True), start=1):
        print(f"{i}. Tienda {row['store_nbr']}: {row['ventas_totales']:,.2f}")

    ventas_por_ciudad = (
        df.group_by("city")
        .agg(
            [
                pl.col("sales").sum().alias("ventas_totales"),
                pl.col("sales").mean().alias("venta_promedio"),
            ]
        )
        .sort("ventas_totales", descending=True)
    )

    ventas_por_provincia = (
        df.group_by("state")
        .agg(
            [
                pl.col("sales").sum().alias("ventas_totales"),
                pl.col("sales").mean().alias("venta_promedio"),
            ]
        )
        .sort("ventas_totales", descending=True)
    )

    ventas_por_anio = (
        df.with_columns(pl.col("date").dt.year().alias("anio"))
        .group_by("anio")
        .agg(
            [
                pl.col("sales").sum().alias("ventas_totales"),
                pl.col("sales").mean().alias("venta_promedio"),
            ]
        )
        .sort("anio")
    )

    ventas_por_mes = (
        df.with_columns(pl.col("date").dt.truncate("1mo").alias("fecha_mes"))
        .group_by("fecha_mes")
        .agg(pl.col("sales").sum().alias("ventas_totales"))
        .with_columns(
            [
                pl.col("fecha_mes").dt.year().alias("anio"),
                pl.col("fecha_mes").dt.month().alias("mes"),
            ]
        )
        .sort("fecha_mes")
    )

    print("\n1.3 CIUDADES CON MAYOR VENTA PROMEDIO")
    for i, row in enumerate(
        ventas_por_ciudad.sort("venta_promedio", descending=True)
        .head(5)
        .iter_rows(named=True),
        start=1,
    ):
        print(f"{i}. {row['city']}: {row['venta_promedio']:,.2f}")

    print("\n1.4 EVOLUCIÓN ANUAL")
    for row in ventas_por_anio.iter_rows(named=True):
        print(f"{row['anio']}: {row['ventas_totales']:,.2f}")

    archivos = {
        "ventas_por_familia": guardar_resultado(
            ventas_por_familia, "ventas_por_familia.parquet"
        ),
        "ventas_por_tienda": guardar_resultado(
            ventas_por_tienda, "ventas_por_tienda.parquet"
        ),
        "ventas_por_ciudad": guardar_resultado(
            ventas_por_ciudad, "ventas_por_ciudad.parquet"
        ),
        "ventas_por_provincia": guardar_resultado(
            ventas_por_provincia, "ventas_por_provincia.parquet"
        ),
        "ventas_por_anio": guardar_resultado(
            ventas_por_anio, "ventas_por_anio.parquet"
        ),
        "ventas_por_mes": guardar_resultado(ventas_por_mes, "ventas_por_mes.parquet"),
    }

    print("\nResultados guardados en data/processed/eda_profundo")

    return {
        "estado": "ok",
        "archivos": {nombre: str(ruta) for nombre, ruta in archivos.items()},
        "familia_mayor_venta": ventas_por_familia.get_column("family")[0],
        "tienda_mayor_venta": int(top_10_tiendas.get_column("store_nbr")[0]),
        "tienda_menor_venta": int(bottom_10_tiendas.get_column("store_nbr")[0]),
    }


def ejecutar_ventas_generales():
    return analisis_ventas_generales()


if __name__ == "__main__":
    ejecutar_ventas_generales()
