import polars as pl
from datetime import timedelta
from config.config import RUTAS


def analisis_feriados():

    df = pl.read_parquet(RUTAS["processed"] / "consolidado.parquet")

    resultados = {}

    print("ESTACIONALIDAD Y FERIADOS")
    print("-" * 60)

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
        print("No se puede realizar el análisis. ")
        print(f"Faltan las columnas: {columnas_faltantes}")
        return resultados

    df_feriados = df.with_columns(pl.col("es_feriado_nacional").fill_null(False))

    # 2.1 Impacto de feriados

    print("\nImpacto de feriados nacionales")
    print("-" * 60)

    ventas_diarias = df_feriados.group_by(["date", "es_feriado_nacional"]).agg(
        pl.col("sales").sum().alias("ventas_diarias")
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

    for row in ventas_feriado_normal.iter_rows(named=True):
        tipo_dia = "Feriado nacional" if row["es_feriado_nacional"] else "Día normal"
        print(f"{tipo_dia}:")
        print("Ventas totales: ")
        print(f"${row['ventas_totales']:,.2f}")

        print("Venta promedio por registro: ")
        print(f"${row['venta_promedio_diaria']:,.2f}")

        print("Cantidad de días: ")
        print(f"{row['cantidad_dias']:,}")

    comparacion_feriados = ventas_feriado_normal.select(
        ["es_feriado_nacional", "venta_promedio_diaria"]
    )
    venta_promedio_feriado = (
        comparacion_feriados.filter(pl.col("es_feriado_nacional"))
        .select("venta_promedio_diaria")
        .to_series()
    )

    venta_promedio_normal = (
        comparacion_feriados.filter(~pl.col("es_feriado_nacional"))
        .select("venta_promedio_diaria")
        .to_series()
    )

    if (
        len(venta_promedio_feriado) > 0
        and len(venta_promedio_normal) > 0
        and venta_promedio_normal[0] != 0
    ):
        cambio_porcentual_general = (
            (venta_promedio_feriado[0] - venta_promedio_normal[0])
            / venta_promedio_normal[0]
            * 100
        )

        print("\nCambio de ventas promedio en feriados: ")
        print(f"{cambio_porcentual_general:+.2f}%")

        resultados["cambio_porcentual_general"] = cambio_porcentual_general

    resultados["ventas_feriado_vs_normal"] = ventas_feriado_normal.to_dicts()

    # 2.2 Ventanas de feriados
    print("\n2.2 VENTAS 3 DIAS ANTES/DESPUES")
    print("-" * 60)
    fechas_feriado = (
        df_feriados.filter(pl.col("es_feriado_nacional"))
        .select("date")
        .unique()
        .sort("date")
        .to_series()
        .to_list()
    )

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

    if filas_ventana:
        ventana_feriados = pl.DataFrame(filas_ventana)

        ventas_dias_cercanos = (
            df_feriados.join(ventana_feriados, on="date", how="inner")
            .group_by(["family", "dias_relativo_feriado"])
            .agg(
                [
                    pl.col("sales").sum().alias("ventas_totales"),
                    pl.col("sales").mean().alias("venta_promedio_diaria"),
                    pl.col("fecha_feriado").n_unique().alias("cantidad_feriados"),
                ]
            )
            .sort(["family", "dias_relativo_feriado"])
        )
        ventas_previas = ventas_dias_cercanos.filter(
            pl.col("dias_relativo_feriado") < 0
        )

        ventas_posteriores = ventas_dias_cercanos.filter(
            pl.col("dias_relativo_feriado") > 0
        )
        print("\nEjemplo de ventas previas a feriado")

        for row in ventas_previas.head(10).iter_rows(named=True):
            print(
                f"{row['family']} | Día {row['dias_relativo_feriado']:+d}: ${row['venta_promedio_diaria']:,.2f} promedio"
            )

        print("\nEjemplo de ventas posteriores a feriado")

        for row in ventas_posteriores.head(10).iter_rows(named=True):
            print(
                f"{row['family']} | Día {row['dias_relativo_feriado']:+d}: ${row['venta_promedio_diaria']:,.2f} promedio"
            )

        resultados["ventas_ventana_feriados"] = ventas_dias_cercanos.to_dicts()

        resultados["ventas_previas_feriados"] = ventas_previas.to_dicts()

        resultados["ventas_posteriores_feriados"] = ventas_posteriores.to_dicts()

    else:
        print("No se encontraron feriados nacionales")

    # 2.3 Sensibilidad a feriados
    print("\n2.3 FAMILIAS MAS SENSIBLES")
    print("-" * 60)

    sensibilidad_familias = (
        df_feriados.group_by("family")
        .agg(
            [
                pl.col("sales")
                .filter(pl.col("es_feriado_nacional"))
                .mean()
                .alias("venta_promedio_feriado"),
                pl.col("sales")
                .filter(~pl.col("es_feriado_nacional"))
                .mean()
                .alias("venta_promedio_normal"),
                pl.col("sales")
                .filter(pl.col("es_feriado_nacional"))
                .sum()
                .alias("ventas_totales_feriado"),
            ]
        )
        .with_columns(
            pl.when(
                pl.col("venta_promedio_normal").is_not_null()
                & (pl.col("venta_promedio_normal") != 0)
                & pl.col("venta_promedio_feriado").is_not_null()
            )
            .then(
                (pl.col("venta_promedio_feriado") - pl.col("venta_promedio_normal"))
                / pl.col("venta_promedio_normal")
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
    print("Familias con mayor sensibilidad general:")

    for i, row in enumerate(sensibilidad_familias.head(10).iter_rows(named=True)):
        print(f"{i + 1}. {row['family']}: {row['cambio_porcentual']:+.2f}%")

    familias_impacto_positivo = sensibilidad_familias.filter(
        pl.col("cambio_porcentual") > 0
    ).sort("cambio_porcentual", descending=True)

    familias_impacto_negativo = sensibilidad_familias.filter(
        pl.col("cambio_porcentual") < 0
    ).sort("cambio_porcentual")

    print("\nFamilias más beneficiadas por los feriados:")

    for i, row in enumerate(familias_impacto_positivo.head(5).iter_rows(named=True)):
        print(f"{i + 1}. {row['family']}: {row['cambio_porcentual']:+.2f}%")

    print("\nFamilias más afectadas negativamente:")

    for i, row in enumerate(familias_impacto_negativo.head(5).iter_rows(named=True)):
        print(f"{i + 1}. {row['family']}: {row['cambio_porcentual']:+.2f}%")

    print(sensibilidad_familias.sort("cambio_porcentual"))

    resultados["sensibilidad_familias_feriados"] = sensibilidad_familias.to_dicts()

    resultados["familias_impacto_positivo"] = familias_impacto_positivo.head(
        10
    ).to_dicts()

    resultados["familias_impacto_negativo"] = familias_impacto_negativo.head(
        10
    ).to_dicts()

    return resultados


if __name__ == "__main__":
    analisis_feriados()
