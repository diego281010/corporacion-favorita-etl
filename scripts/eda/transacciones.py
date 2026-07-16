import polars as pl
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

def analisis_transacciones(df):
    resultados = {}

    print("TRANSACCIONES")
    print("-" * 40)

    if "transactions" in df.columns:
        print("5.1 RELACION TRANSACCIONES VS VENTAS")
        transacciones_por_tienda = (
            df.group_by("store_nbr")
            .agg([
                pl.col("transactions").sum().alias("total_transacciones"),
                pl.col("sales").sum().alias("ventas_totales"),
                (pl.col("sales").sum() / pl.col("transactions").sum()).alias("ticket_promedio")
            ])
            .sort("ticket_promedio", descending=True)
        )
        print("Ticket promedio mas alto:")
        for row in transacciones_por_tienda.head(5).rows():
            print(f"   Tienda {row[0]}: ${row[3]:.2f}")
        print("Ticket promedio mas bajo:")
        for row in transacciones_por_tienda.tail(5).rows():
            print(f"   Tienda {row[0]}: ${row[3]:.2f}")
        resultados["transacciones_por_tienda"] = transacciones_por_tienda.to_dicts()

    else:
        print("   No se encontro la columna 'transactions'")

    return resultados