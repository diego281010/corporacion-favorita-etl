import polars as pl
from config.config import RUTAS

def consolidar():
    try:
        train = pl.read_parquet(RUTAS['processed'] / "train_limpio.parquet")
        stores = pl.read_parquet(RUTAS['processed'] / "stores_limpio.parquet")
        oil = pl.read_parquet(RUTAS['processed'] / "oil_limpio.parquet")
        holidays = pl.read_parquet(RUTAS['processed'] / "holidays_events_limpio.parquet")
        transactions = pl.read_parquet(RUTAS['processed'] / "transactions_limpio.parquet")
    except Exception as e:
        print(f"Error al cargar archivos: {e}")
        print("Se necesita ejecutar primero limpiar_datos.py")
        return None

    print(f"train: {train.height:,} filas ")
    print(f"stores: {stores.height:,} filas ")
    print(f"oil: {oil.height:,} filas ")
    print(f"holidays_events: {holidays.height:,} filas ")
    print(f"transactions: {transactions.height:,} filas ")

    # REVISIÓN DE DUPLCIADOS

    fechas_duplicadas = (
            holidays.group_by("date").len().filter(pl.col("len") > 1).sort("len", descending=True)
            )

    print("\nFechas con más de un registro en holidays_events:")
    print(fechas_duplicadas)

    cantidad_fechas_duplicadas = fechas_duplicadas.height

    print("Cantidad de fechas con múltiples eventos: ")
    print(f"{cantidad_fechas_duplicadas}")

    impacto_fechas = (
            fechas_duplicadas.join(train.group_by("date").len().rename({"len" : "filas_train"}),
                                   on="date",
                                   how="left"
                                   )
            .with_columns(
                (pl.col("filas_train") * (pl.col("len") -1)
                 ).alias("filas_adicionales")
            ).sort("filas_adicionales", descending=True)
    )

    print("\nImpacto de las fechas duplicadas:")
    print(impacto_fechas)

    print("\nTotal de filas adicionales esperadas:")
    print(impacto_fechas["filas_adicionales"].sum())

    return None

if __name__ == "__main__":
    consolidar()


