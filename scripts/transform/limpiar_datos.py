import polars as pl

from scripts.extract.cargar_datos import cargar_datos
from config.config import RUTAS

def guardar_datos_limpios(datos_limpios):
    for nombre, df in datos_limpios.items():
        ruta_salida = RUTAS['processed'] / f"{nombre}_limpio.parquet"
        df.write_parquet(ruta_salida)

def convertir_fechas(df):
    if "date" not in df.columns:
        return df
    
    return df.with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").alias("date")
    )

def eliminar_duplicados(df):
    return df.unique()

def limpiar_oil(df):
    df = convertir_fechas(df)
    df = eliminar_duplicados(df)

    return df.with_columns(
        pl.col("dcoilwtico").interpolate()
    )

def limpiar_dataframe(df):
    df = convertir_fechas(df)
    df = eliminar_duplicados(df)
    return df

def limpiar_datos():
    datos = cargar_datos()

    datos_limpios = {
        "train" : limpiar_dataframe(datos['train']),
        "stores" : limpiar_dataframe(datos['stores']),
        "transactions" : limpiar_dataframe(datos['transactions']),
        "holidays_events" : limpiar_dataframe(datos['holidays_events']),
        "oil" : limpiar_oil(datos['oil']),
    }

    return datos_limpios

if __name__ == "__main__":
    datos_limpios = limpiar_datos()
    guardar_datos_limpios(datos_limpios)

    for nombre, df in datos_limpios.items():
        print(f"{nombre}: {df.shape}")
        print(df.head(10))