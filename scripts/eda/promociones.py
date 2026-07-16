import polars as pl
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

def analisis_promociones(df):
    resultados = {}

    print("PROMOCIONES")
    print("-" * 40)

    if "onpromotion" in df.columns:
        df = df.with_columns(
            (pl.col("onpromotion") > 0).alias("en_promocion")
        )

        # 3.1 Comparacion con/sin promocion
        print("3.1 COMPARACION CON/SIN PROMOCION")
        ventas_promo = (
            df.group_by("en_promocion")
            .agg([
                pl.col("sales").mean().alias("venta_promedio"),
                pl.col("sales").count().alias("numero_ventas")
            ])
        )
        for row in ventas_promo.rows():
            tipo = "Con promocion" if row[0] else "Sin promocion"
            print(f"   {tipo}: ${row[1]:,.2f} promedio ({row[2]:,} ventas)")
        resultados["ventas_con_promocion"] = ventas_promo.to_dicts()

        # 3.2 Efecto por familia
        print("\n3.2 EFECTO POR FAMILIA")
        promocion_por_familia = (
            df.group_by(["family", "en_promocion"])
            .agg(pl.col("sales").mean().alias("venta_promedio"))
            .sort(["family", "en_promocion"])
        )
        efecto_promo = (
            promocion_por_familia
            .pivot(on="en_promocion", index="family", values="venta_promedio")
            .rename({"true": "venta_con_promo", "false": "venta_sin_promo"})
            .with_columns(
                ((pl.col("venta_con_promo") - pl.col("venta_sin_promo")) / pl.col("venta_sin_promo") * 100)
                .alias("incremento_porcentual")
            )
            .sort("incremento_porcentual", descending=True)
            .filter(pl.col("incremento_porcentual").is_not_null())
        )
        print("Mayor efecto:")
        for row in efecto_promo.head(5).rows():
            print(f"   {row[0]}: +{row[3]:.1f}%")
        print("Menor efecto:")
        for row in efecto_promo.tail(5).rows():
            print(f"   {row[0]}: {row[3]:.1f}%")
        resultados["efecto_promociones_por_familia"] = efecto_promo.to_dicts()

    else:
        print("   No se encontro la columna 'onpromotion'")

    return resultados