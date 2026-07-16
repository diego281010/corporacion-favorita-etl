import polars as pl
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
INPUT_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"

def analisis_ventas_generales(df):
    resultados = {}

    print("VENTAS GENERALES")
    print("-" * 40)

    # 1.1 Ventas por familia
    print("1.1 VENTAS POR FAMILIA")
    ventas_por_familia = (
        df.group_by("family")
        .agg([
            pl.col("sales").sum().alias("ventas_totales"),
            pl.col("sales").mean().alias("venta_promedio"),
            pl.col("sales").count().alias("numero_ventas")
        ])
        .sort("ventas_totales", descending=True)
    )
    for i, row in enumerate(ventas_por_familia.head(10).rows()):
        print(f"   {i+1}. {row[0]}: ${row[1]:,.2f}")
    resultados["ventas_por_familia"] = ventas_por_familia.to_dicts()

    # 1.2 Ranking de tiendas
    print("\n1.2 RANKING DE TIENDAS")
    ventas_por_tienda = (
        df.group_by("store_nbr")
        .agg([
            pl.col("sales").sum().alias("ventas_totales"),
            pl.col("sales").mean().alias("venta_promedio")
        ])
        .sort("ventas_totales", descending=True)
    )
    print("Top 10 tiendas:")
    for i, row in enumerate(ventas_por_tienda.head(10).rows()):
        print(f"   {i+1}. Tienda {row[0]}: ${row[1]:,.2f}")
    print("Bottom 10 tiendas:")
    for i, row in enumerate(ventas_por_tienda.tail(10).rows()):
        print(f"   {i+1}. Tienda {row[0]}: ${row[1]:,.2f}")
    resultados["top_10_tiendas"] = ventas_por_tienda.head(10).to_dicts()
    resultados["bottom_10_tiendas"] = ventas_por_tienda.tail(10).to_dicts()

    # 1.3 Ventas por ciudad y provincia
    print("\n1.3 VENTAS POR CIUDAD Y PROVINCIA")
    ventas_por_ciudad = (
        df.group_by("city")
        .agg([
            pl.col("sales").sum().alias("ventas_totales"),
            pl.col("sales").mean().alias("venta_promedio")
        ])
        .sort("ventas_totales", descending=True)
    )
    for row in ventas_por_ciudad.rows():
        print(f"   {row[0]}: ${row[1]:,.2f} total, ${row[2]:,.2f} promedio")
    resultados["ventas_por_ciudad"] = ventas_por_ciudad.to_dicts()

    ventas_por_provincia = (
        df.group_by("state")
        .agg([
            pl.col("sales").sum().alias("ventas_totales"),
            pl.col("sales").mean().alias("venta_promedio")
        ])
        .sort("ventas_totales", descending=True)
    )
    for row in ventas_por_provincia.rows():
        print(f"   {row[0]}: ${row[1]:,.2f} total, ${row[2]:,.2f} promedio")
    resultados["ventas_por_provincia"] = ventas_por_provincia.to_dicts()

    # 1.4 Evolucion temporal
    print("\n1.4 EVOLUCION TEMPORAL")
    ventas_por_anio = (
        df.with_columns(pl.col("date").dt.year().alias("anio"))
        .group_by("anio")
        .agg([
            pl.col("sales").sum().alias("ventas_totales"),
            pl.col("sales").mean().alias("venta_promedio")
        ])
        .sort("anio")
    )
    for row in ventas_por_anio.rows():
        print(f"   {row[0]}: ${row[1]:,.2f}")
    resultados["ventas_por_anio"] = ventas_por_anio.to_dicts()

    ventas_por_mes = (
        df.with_columns([
            pl.col("date").dt.year().alias("anio"),
            pl.col("date").dt.month().alias("mes")
        ])
        .group_by(["anio", "mes"])
        .agg(pl.col("sales").sum().alias("ventas_totales"))
        .sort(["anio", "mes"])
    )
    resultados["ventas_por_mes"] = ventas_por_mes.to_dicts()

    return resultados