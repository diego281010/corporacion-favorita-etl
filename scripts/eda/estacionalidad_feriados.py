import polars as pl
from datetime import timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

def analisis_feriados(df):
    resultados = {}

    print("ESTACIONALIDAD Y FERIADOS")
    print("-" * 40)

    if "locale" in df.columns and "type_right" in df.columns:
        df = df.with_columns(
            (
                (pl.col("locale") == "National")
                & (pl.col("type_right") != "Work Day")
            ).fill_null(False).alias("es_feriado_nacional")
        )

        # 2.1 Impacto de feriados
        print("2.1 IMPACTO DE FERIADOS NACIONALES")
        ventas_feriado = (
            df.group_by("es_feriado_nacional")
            .agg(pl.col("sales").mean().alias("venta_promedio"))
        )
        for row in ventas_feriado.rows():
            tipo = "Feriado" if row[0] else "Normal"
            print(f"   {tipo}: ${row[1]:,.2f} promedio")
        resultados["ventas_feriado_vs_normal"] = ventas_feriado.to_dicts()

        # 2.2 Ventanas de feriados
        print("\n2.2 VENTAS 3 DIAS ANTES/DESPUES")
        fechas_feriado = (
            df.filter(pl.col("es_feriado_nacional"))
            .select("date")
            .unique()
            .to_series()
            .to_list()
        )

        filas_ventana = []
        for fecha in fechas_feriado:
            for offset in [-3, -2, -1, 1, 2, 3]:
                filas_ventana.append({
                    "date": fecha + timedelta(days=offset),
                    "dias_relativo_feriado": offset,
                })

        if filas_ventana:
            ventana_df = pl.DataFrame(filas_ventana)
            df_ventana = df.join(ventana_df, on="date", how="inner")
            ventas_ventana = (
                df_ventana.group_by(["family", "dias_relativo_feriado"])
                .agg(pl.col("sales").mean().alias("venta_promedio"))
                .sort(["family", "dias_relativo_feriado"])
            )
            print("Pre-feriados:")
            for row in ventas_ventana.filter(pl.col("dias_relativo_feriado") < 0).head(5).rows():
                print(f"   {row[0]}: dia {row[1]:+d}: ${row[2]:.2f}")
            print("Post-feriados:")
            for row in ventas_ventana.filter(pl.col("dias_relativo_feriado") > 0).head(5).rows():
                print(f"   {row[0]}: dia {row[1]:+d}: ${row[2]:.2f}")
            resultados["ventas_ventana_feriados"] = ventas_ventana.to_dicts()

        # 2.3 Sensibilidad a feriados
        print("\n2.3 FAMILIAS MAS SENSIBLES")
        ventas_familia_feriado = (
            df.group_by(["family", "es_feriado_nacional"])
            .agg(pl.col("sales").mean().alias("venta_promedio"))
        )
        pivot = ventas_familia_feriado.pivot(
            on="es_feriado_nacional",
            index="family",
            values="venta_promedio"
        )
        pivot = pivot.rename({"true": "venta_feriado", "false": "venta_normal"})
        pivot = pivot.with_columns(
            (
                (pl.col("venta_feriado") - pl.col("venta_normal")) / pl.col("venta_normal") * 100
            ).alias("cambio_porcentual")
        )
        sensibilidad = pivot.sort("cambio_porcentual", descending=True)
        print("Mas afectadas positivamente:")
        for row in sensibilidad.head(5).rows():
            print(f"   {row[0]}: +{row[3]:.1f}%")
        print("Mas afectadas negativamente:")
        for row in sensibilidad.tail(5).rows():
            print(f"   {row[0]}: {row[3]:.1f}%")
        resultados["sensibilidad_familias_feriados"] = sensibilidad.to_dicts()

    else:
        print("   No se encontraron columnas de feriados")

    return resultados