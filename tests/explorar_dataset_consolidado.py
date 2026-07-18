import polars as pl
from config.config import RUTAS


def mostrar_info_dataset(df):
    print("="*60)
    print("CONSOLIDADO")
    print("="*60)

    print(f"Filas: {df.height}")
    print(f"Columnas: {df.width}")

    print("\nColumnas:")
    print(df.columns)

    print("\nTipos de datos:")
    print(df.schema)

    print("\nPrimeras 10 filas:")
    print(df.head(10))

    print("\nUltimas 10 filas:")
    print(df.tail(10))

    print("\nValores locales_evento")
    print(df.select("locales_evento")
          .drop_nulls()
          .unique()
          .head(20)
          )

    print("\n")

def explorar_dataset():
    datos = pl.read_parquet(RUTAS["processed"] / "consolidado.parquet")


    mostrar_info_dataset(datos)

if __name__ == "__main__":
    explorar_dataset()
