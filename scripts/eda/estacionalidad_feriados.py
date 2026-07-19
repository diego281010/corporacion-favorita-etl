from datetime import timedelta

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


def analisis_feriados(df=None):
    if df is None:
        df = cargar_consolidado()

    columnas_necesarias = [
        "date",
        "family",
        "sales",
        "es_feriado_nacional",
    ]
    columnas_faltantes = [
        columna for columna in columnas_necesarias if columna not in df.columns
    ]
    if columnas_faltantes:
        raise ValueError(
            "No se puede realizar el análisis de feriados. "
            f"Faltan las columnas: {columnas_faltantes}"
        )

    df_feriados = df.with_columns(pl.col("es_feriado_nacional").fill_null(False))

    fechas_feriado = (
        df_feriados.filter(pl.col("es_feriado_nacional"))
        .select("date")
        .unique()
        .sort("date")
        .to_series()
        .to_list()
    )
    if not fechas_feriado:
        raise ValueError("No se encontraron feriados nacionales en el consolidado.")

    print("ESTACIONALIDAD Y FERIADOS")
    print("-" * 60)

    # 2.1 Impacto general: primero se calcula el volumen total de cada fecha.
    ventas_diarias = (
        df_feriados.group_by(["date", "es_feriado_nacional"])
        .agg(pl.col("sales").sum().alias("ventas_diarias"))
        .sort("date")
    )

    ventas_feriado_normal = (
        ventas_diarias.group_by("es_feriado_nacional")
        .agg(
            [
                pl.col("ventas_diarias").sum().alias("ventas_totales"),
                pl.col("ventas_diarias").mean().alias("venta_promedio_diaria"),
                pl.col("ventas_diarias").median().alias("venta_mediana_diaria"),
                pl.len().alias("cantidad_dias"),
            ]
        )
        .sort("es_feriado_nacional", descending=True)
    )

    print("\n2.1 IMPACTO DE FERIADOS NACIONALES")
    for row in ventas_feriado_normal.iter_rows(named=True):
        tipo = "Feriado nacional" if row["es_feriado_nacional"] else "Día normal"
        print(
            f"{tipo}: promedio diario {row['venta_promedio_diaria']:,.2f}; "
            f"mediana diaria {row['venta_mediana_diaria']:,.2f}; "
            f"días {row['cantidad_dias']:,}"
        )

    promedio_feriado = ventas_feriado_normal.filter(
        pl.col("es_feriado_nacional")
    ).get_column("venta_promedio_diaria")
    promedio_normal = ventas_feriado_normal.filter(
        ~pl.col("es_feriado_nacional")
    ).get_column("venta_promedio_diaria")

    cambio_porcentual_general = None
    if (
        len(promedio_feriado) > 0
        and len(promedio_normal) > 0
        and promedio_normal[0] != 0
    ):
        cambio_porcentual_general = (
            (promedio_feriado[0] - promedio_normal[0]) / promedio_normal[0] * 100
        )
        print(
            "Cambio del volumen promedio diario en feriados: "
            f"{cambio_porcentual_general:+.2f}%"
        )

    # 2.2 Ventana alrededor de cada feriado.
    filas_ventana = []
    for fecha_feriado in fechas_feriado:
        for dias_relativos in [-3, -2, -1, 1, 2, 3]:
            filas_ventana.append(
                {
                    "date": fecha_feriado + timedelta(days=dias_relativos),
                    "fecha_feriado": fecha_feriado,
                    "dias_relativo_feriado": dias_relativos,
                }
            )

    ventana_feriados = pl.DataFrame(filas_ventana)

    # Primero se suma cada familia para cada feriado y día relativo.
    ventas_por_evento = (
        df_feriados.join(ventana_feriados, on="date", how="inner")
        .group_by(["fecha_feriado", "family", "dias_relativo_feriado"])
        .agg(pl.col("sales").sum().alias("ventas_familia_evento"))
    )

    # Después se resume el comportamiento promedio entre todos los feriados.
    ventas_dias_cercanos = (
        ventas_por_evento.group_by(["family", "dias_relativo_feriado"])
        .agg(
            [
                pl.col("ventas_familia_evento").sum().alias("ventas_totales"),
                pl.col("ventas_familia_evento")
                .mean()
                .alias("venta_promedio_por_feriado"),
                pl.col("ventas_familia_evento")
                .median()
                .alias("venta_mediana_por_feriado"),
                pl.col("fecha_feriado").n_unique().alias("cantidad_feriados"),
            ]
        )
        .sort(["family", "dias_relativo_feriado"])
    )

    print("\n2.2 VENTAS TRES DÍAS ANTES Y DESPUÉS")
    for row in ventas_dias_cercanos.head(10).iter_rows(named=True):
        print(
            f"{row['family']} | día {row['dias_relativo_feriado']:+d}: "
            f"promedio {row['venta_promedio_por_feriado']:,.2f}"
        )

    # 2.3 Sensibilidad por familia usando volúmenes diarios por familia.
    ventas_diarias_familia = df_feriados.group_by(
        ["date", "family", "es_feriado_nacional"]
    ).agg(pl.col("sales").sum().alias("ventas_diarias_familia"))

    sensibilidad_familias = (
        ventas_diarias_familia.group_by("family")
        .agg(
            [
                pl.col("ventas_diarias_familia")
                .filter(pl.col("es_feriado_nacional"))
                .mean()
                .alias("venta_promedio_diaria_feriado"),
                pl.col("ventas_diarias_familia")
                .filter(~pl.col("es_feriado_nacional"))
                .mean()
                .alias("venta_promedio_diaria_normal"),
                pl.col("date")
                .filter(pl.col("es_feriado_nacional"))
                .n_unique()
                .alias("dias_feriado"),
                pl.col("date")
                .filter(~pl.col("es_feriado_nacional"))
                .n_unique()
                .alias("dias_normales"),
            ]
        )
        .with_columns(
            pl.when(
                pl.col("venta_promedio_diaria_feriado").is_not_null()
                & pl.col("venta_promedio_diaria_normal").is_not_null()
                & (pl.col("venta_promedio_diaria_normal") != 0)
            )
            .then(
                (
                    pl.col("venta_promedio_diaria_feriado")
                    - pl.col("venta_promedio_diaria_normal")
                )
                / pl.col("venta_promedio_diaria_normal")
                * 100
            )
            .otherwise(None)
            .alias("cambio_porcentual")
        )
        .filter(
            pl.col("cambio_porcentual").is_not_null()
            & pl.col("cambio_porcentual").is_finite()
        )
        .with_columns(pl.col("cambio_porcentual").abs().alias("sensibilidad_absoluta"))
        .sort("sensibilidad_absoluta", descending=True)
    )

    familias_impacto_positivo = sensibilidad_familias.filter(
        pl.col("cambio_porcentual") > 0
    ).sort("cambio_porcentual", descending=True)
    familias_impacto_negativo = sensibilidad_familias.filter(
        pl.col("cambio_porcentual") < 0
    ).sort("cambio_porcentual")

    print("\n2.3 FAMILIAS MÁS SENSIBLES")
    for i, row in enumerate(
        sensibilidad_familias.head(10).iter_rows(named=True), start=1
    ):
        print(f"{i}. {row['family']}: {row['cambio_porcentual']:+.2f}%")

    archivos = {
        "feriado_vs_normal": guardar_resultado(
            ventas_feriado_normal, "feriados_vs_dias_normales.parquet"
        ),
        "ventana_feriados": guardar_resultado(
            ventas_dias_cercanos, "feriados_ventana_tres_dias.parquet"
        ),
        "sensibilidad_familias": guardar_resultado(
            sensibilidad_familias, "feriados_sensibilidad_familias.parquet"
        ),
    }

    return {
        "estado": "ok",
        "archivos": {nombre: str(ruta) for nombre, ruta in archivos.items()},
        "cantidad_feriados_nacionales": len(fechas_feriado),
        "cambio_porcentual_general": cambio_porcentual_general,
        "familia_mayor_sensibilidad": (
            sensibilidad_familias.get_column("family")[0]
            if sensibilidad_familias.height > 0
            else None
        ),
        "familias_impacto_positivo": familias_impacto_positivo.height,
        "familias_impacto_negativo": familias_impacto_negativo.height,
    }


def ejecutar_feriados():
    return analisis_feriados()


if __name__ == "__main__":
    ejecutar_feriados()
