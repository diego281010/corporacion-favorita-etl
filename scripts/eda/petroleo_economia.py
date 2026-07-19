import math
from datetime import date

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


def analisis_petroleo(df=None):
    if df is None:
        df = cargar_consolidado()

    columnas_necesarias = ["date", "sales", "dcoilwtico", "city"]
    columnas_faltantes = [
        columna for columna in columnas_necesarias if columna not in df.columns
    ]

    if columnas_faltantes:
        raise ValueError(
            "No se puede realizar el análisis de petróleo. "
            f"Faltan las columnas: {columnas_faltantes}"
        )

    print("PETRÓLEO Y ECONOMÍA")
    print("-" * 60)

    ventas_mensuales = (
        df.with_columns(pl.col("date").dt.truncate("1mo").alias("fecha_mes"))
        .group_by("fecha_mes")
        .agg(pl.col("sales").sum().alias("ventas_totales"))
    )

    petroleo_mensual = (
        df.select(["date", "dcoilwtico"])
        .unique(subset=["date"], keep="first")
        .drop_nulls("dcoilwtico")
        .with_columns(pl.col("date").dt.truncate("1mo").alias("fecha_mes"))
        .group_by("fecha_mes")
        .agg(pl.col("dcoilwtico").mean().alias("precio_petroleo_promedio"))
    )

    petroleo_ventas_mensual = (
        ventas_mensuales.join(petroleo_mensual, on="fecha_mes", how="inner")
        .with_columns(
            [
                pl.col("fecha_mes").dt.year().alias("anio"),
                pl.col("fecha_mes").dt.month().alias("mes"),
            ]
        )
        .sort("fecha_mes")
    )

    if petroleo_ventas_mensual.height < 3:
        raise ValueError(
            "No existen suficientes meses para calcular el análisis de petróleo."
        )

    print("\n4.1 CORRELACIÓN PETRÓLEO VS VENTAS MENSUALES")

    correlacion_niveles = petroleo_ventas_mensual.select(
        pl.corr("precio_petroleo_promedio", "ventas_totales").alias("correlacion")
    ).item()

    if correlacion_niveles is not None and math.isfinite(correlacion_niveles):
        print(f"Correlación mensual: {correlacion_niveles:.3f}")
    else:
        print("No fue posible calcular la correlación mensual")
        correlacion_niveles = None

    variaciones_mensuales = petroleo_ventas_mensual.with_columns(
        [
            (pl.col("precio_petroleo_promedio").pct_change() * 100).alias(
                "variacion_petroleo_pct"
            ),
            (pl.col("ventas_totales").pct_change() * 100).alias("variacion_ventas_pct"),
        ]
    )

    periodo_caida = variaciones_mensuales.filter(
        pl.col("fecha_mes").is_between(
            date(2015, 1, 1),
            date(2016, 12, 1),
            closed="both",
        )
    ).sort("fecha_mes")

    if periodo_caida.height < 4:
        raise ValueError(
            "No existen suficientes meses entre 2015 y 2016 para calcular el lag."
        )

    print("\n4.2 LAG TEMPORAL ENTRE CAÍDAS, 2015-2016")

    correlaciones_lag = []

    for lag in range(0, 7):
        datos_lag = periodo_caida.select(
            [
                "fecha_mes",
                "variacion_petroleo_pct",
                pl.col("variacion_ventas_pct")
                .shift(-lag)
                .alias("variacion_ventas_desfasada_pct"),
            ]
        ).filter(
            pl.col("variacion_petroleo_pct").is_not_null()
            & pl.col("variacion_ventas_desfasada_pct").is_not_null()
            & pl.col("variacion_petroleo_pct").is_finite()
            & pl.col("variacion_ventas_desfasada_pct").is_finite()
        )

        correlacion_lag = None

        if datos_lag.height > 2:
            valor = datos_lag.select(
                pl.corr(
                    "variacion_petroleo_pct",
                    "variacion_ventas_desfasada_pct",
                ).alias("correlacion")
            ).item()

            if valor is not None and math.isfinite(valor):
                correlacion_lag = valor

        correlaciones_lag.append(
            {
                "lag_meses": lag,
                "correlacion": correlacion_lag,
                "meses_comparados": datos_lag.height,
            }
        )

        texto_correlacion = (
            f"{correlacion_lag:.3f}" if correlacion_lag is not None else "N/A"
        )
        print(f"Lag {lag} mes(es): correlación = {texto_correlacion}")

    lags_misma_direccion = [
        resultado
        for resultado in correlaciones_lag
        if resultado["correlacion"] is not None and resultado["correlacion"] > 0
    ]

    mejor_lag = (
        max(
            lags_misma_direccion,
            key=lambda resultado: resultado["correlacion"],
        )
        if lags_misma_direccion
        else None
    )

    if mejor_lag:
        print(
            "Lag con mayor relación en la misma dirección: "
            f"{mejor_lag['lag_meses']} mes(es), "
            f"r = {mejor_lag['correlacion']:.3f}"
        )
    else:
        print(
            "No se encontró una correlación positiva que evidencie "
            "caídas en la misma dirección."
        )

    print("\n4.3 CIUDADES MÁS SENSIBLES A LA CAÍDA DEL PETRÓLEO")

    lag_ciudades = mejor_lag["lag_meses"] if mejor_lag else 0

    ventas_ciudad_mensual = (
        df.with_columns(pl.col("date").dt.truncate("1mo").alias("fecha_mes"))
        .group_by(["city", "fecha_mes"])
        .agg(pl.col("sales").sum().alias("ventas_ciudad"))
        .sort(["city", "fecha_mes"])
        .with_columns(
            (pl.col("ventas_ciudad").pct_change().over("city") * 100).alias(
                "variacion_ventas_ciudad_pct"
            )
        )
    )

    datos_ciudades = (
        ventas_ciudad_mensual.join(
            variaciones_mensuales.select(
                [
                    "fecha_mes",
                    "variacion_petroleo_pct",
                ]
            ),
            on="fecha_mes",
            how="inner",
        )
        .filter(
            pl.col("fecha_mes").is_between(
                date(2015, 1, 1),
                date(2016, 12, 1),
                closed="both",
            )
        )
        .sort(["city", "fecha_mes"])
        .with_columns(
            pl.col("variacion_ventas_ciudad_pct")
            .shift(-lag_ciudades)
            .over("city")
            .alias("variacion_ventas_desfasada_pct")
        )
        .filter(
            pl.col("variacion_petroleo_pct").is_not_null()
            & pl.col("variacion_ventas_desfasada_pct").is_not_null()
            & pl.col("variacion_petroleo_pct").is_finite()
            & pl.col("variacion_ventas_desfasada_pct").is_finite()
        )
    )

    sensibilidad_ciudades = (
        datos_ciudades.group_by("city")
        .agg(
            [
                pl.corr(
                    "variacion_petroleo_pct",
                    "variacion_ventas_desfasada_pct",
                ).alias("correlacion_petroleo_ventas"),
                pl.len().alias("meses_comparados"),
            ]
        )
        .filter(
            pl.col("correlacion_petroleo_ventas").is_not_null()
            & pl.col("correlacion_petroleo_ventas").is_finite()
        )
        .with_columns(
            [
                pl.col("correlacion_petroleo_ventas")
                .abs()
                .alias("sensibilidad_absoluta"),
                pl.lit(lag_ciudades).alias("lag_meses_utilizado"),
            ]
        )
        .sort("sensibilidad_absoluta", descending=True)
    )

    print(f"Sensibilidad calculada usando un lag de {lag_ciudades} mes(es):")

    for i, row in enumerate(
        sensibilidad_ciudades.head(10).iter_rows(named=True),
        start=1,
    ):
        print(f"{i}. {row['city']}: r = {row['correlacion_petroleo_ventas']:+.3f}")

    correlaciones_lag_df = pl.DataFrame(
        correlaciones_lag,
        schema={
            "lag_meses": pl.Int64,
            "correlacion": pl.Float64,
            "meses_comparados": pl.Int64,
        },
    )

    archivo_mensual = guardar_resultado(
        petroleo_ventas_mensual,
        "petroleo_ventas_mensuales.parquet",
    )
    archivo_variaciones = guardar_resultado(
        periodo_caida,
        "petroleo_variaciones_2015_2016.parquet",
    )
    archivo_lags = guardar_resultado(
        correlaciones_lag_df,
        "petroleo_lags.parquet",
    )
    archivo_ciudades = guardar_resultado(
        sensibilidad_ciudades,
        "petroleo_sensibilidad_ciudades.parquet",
    )

    print("\nResultados guardados:")
    print(f"- {archivo_mensual}")
    print(f"- {archivo_variaciones}")
    print(f"- {archivo_lags}")
    print(f"- {archivo_ciudades}")

    return {
        "estado": "ok",
        "archivos": {
            "datos_mensuales": str(archivo_mensual),
            "variaciones_2015_2016": str(archivo_variaciones),
            "lags": str(archivo_lags),
            "sensibilidad_ciudades": str(archivo_ciudades),
        },
        "correlacion_mensual": correlacion_niveles,
        "mejor_lag_meses": (mejor_lag["lag_meses"] if mejor_lag else None),
        "mejor_correlacion_lag": (mejor_lag["correlacion"] if mejor_lag else None),
        "ciudad_mayor_sensibilidad": (
            sensibilidad_ciudades.get_column("city")[0]
            if sensibilidad_ciudades.height > 0
            else None
        ),
    }


def ejecutar_petroleo():
    return analisis_petroleo()


if __name__ == "__main__":
    ejecutar_petroleo()
