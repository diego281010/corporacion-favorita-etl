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

    print("\n UNION DE TABLAS")

    df = (train
          .join(stores, on="store_nbr", how="left")
          .join(transactions, on=["store_nbr", "date"], how="left")
          .join(oil, on="date", how="left")
          .join(holidays, on="date", how="left"))
    print(f"Consolidado: {df.height:,} filas, {df.width} columnas")

    print(f"GUARDANDO DATOS CONSOLIDADOS")

    RUTAS['processed'].mkdir(parents=True, exist_ok=True)
    archivo_salida = RUTAS['processed'] / "consolidado.parquet"
    df.write_parquet(archivo_salida)

    tamanio_mb = archivo_salida.stat().st_size / (1024 * 1024)

    print(f"Tamaño del archivo : {tamanio_mb:.2f} MB")
    print(f"Archivo guardado en : {archivo_salida}")

    return df


if __name__ == "__main__":
    consolidar()


