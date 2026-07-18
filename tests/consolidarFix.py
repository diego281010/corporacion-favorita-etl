import polars as pl
from config.config import RUTAS


def consolidar():
    try:
        train = pl.read_parquet(RUTAS["processed"] / "train_limpio.parquet")
        stores = pl.read_parquet(RUTAS["processed"] / "stores_limpio.parquet")
        oil = pl.read_parquet(RUTAS["processed"] / "oil_limpio.parquet")
        holidays = pl.read_parquet(
            RUTAS["processed"] / "holidays_events_limpio.parquet"
        )
        transactions = pl.read_parquet(
            RUTAS["processed"] / "transactions_limpio.parquet"
        )
    except Exception as e:
        print(f"Error al cargar archivos: {e}")
        print("Se necesita ejecutar primero limpiar_datos.py")
        return None

    print(f"train: {train.height:,} filas ")
    print(f"stores: {stores.height:,} filas ")
    print(f"oil: {oil.height:,} filas ")
    print(f"holidays_events: {holidays.height:,} filas ")
    print(f"transactions: {transactions.height:,} filas ")

    print("\nHOLIDAY_EVENTS_AGRUPADO")

    holidays = holidays.with_columns(
        (
            (pl.col("locale") == "National")
            & pl.col("type").is_in(["Holiday", "Additional", "Bridge", "Transfer"])
            & (~pl.col("transferred"))
        ).alias("es_feriado_nacional")
    )

    holidays_agrupado = (
        holidays.group_by("date")
        .agg(
            [
                pl.len().alias("cantidad_eventos"),
                pl.col("type").unique().sort().str.join(", ").alias("tipos_evento"),
                pl.col("locale").unique().sort().str.join(", ").alias("locales_evento"),
                pl.col("description")
                .unique()
                .sort()
                .str.join(", ")
                .alias("descripciones_evento"),
                pl.col("transferred").any().alias("evento_transferido"),
                pl.col("es_feriado_nacional").any().alias("es_feriado_nacional"),
            ]
        )
        .with_columns(pl.lit(True).alias("es_feriado_evento"))
    )
    print("holidays agrupado: ")
    print(f"{holidays_agrupado.height:,} fechas únicas")

    print("\nJOIN DE TABLAS")

    df = (
        train.join(stores, on="store_nbr", how="left")
        .join(transactions, on=["store_nbr", "date"], how="left")
        .join(oil, on="date", how="left")
        .join(holidays_agrupado, on="date", how="left", validate="m:1")
        .with_columns(
            pl.col("transactions").fill_null(0),
            pl.col("cantidad_eventos").fill_null(0),
            pl.col("es_feriado_evento").fill_null(False),
            pl.col("es_feriado_nacional").fill_null(False),
            pl.col("evento_transferido").fill_null(False),
        )
    )

    print(f"Consolidado: {df.height:,} filas, ")
    print(f"{df.width} columnas")

    if df.height != train.height:
        print("\nERROR EN LA CONSOLIDACIÓN")
        print(f"Train: {train.height:,} filas")
        print(f"Consolidado: {df.height:,} filas")
        print("La cantidad de filas cambió durante los joins.")
        return None

    print("\nCantidad de filas validada correctamente")

    print(f"Consolidado: {df.height:,} filas, {df.width} columnas")

    print("\nGUARDANDO DATOS CONSOLIDADOS")

    RUTAS["processed"].mkdir(parents=True, exist_ok=True)
    archivo_salida = RUTAS["processed"] / "consolidado.parquet"
    df.write_parquet(archivo_salida)

    tamanio_mb = archivo_salida.stat().st_size / (1024 * 1024)

    print(f"Tamaño del archivo : {tamanio_mb:.2f} MB")
    print(f"Archivo guardado en : {archivo_salida}")

    return df


if __name__ == "__main__":
    consolidar()
