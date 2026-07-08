import polars as pl
from config.config import RUTAS

ARCHIVOS_CSV = {
    "train" : "train.csv",
    "stores" : "stores.csv",
    "transactions" : "transactions.csv",
    "holidays_events" : "holidays_events.csv",
    "oil" : "oil.csv"
}

def cargar_datos():
    datos = {}

    for nombre, archivo in ARCHIVOS_CSV.items():
        try:
            ruta_archivo = RUTAS['raw'] / archivo
            datos[nombre] = pl.read_csv(ruta_archivo)
        except Exception as error:
            raise RuntimeError(f"No fue posible cargar el archivo {archivo}") from error

    return datos

if __name__ == "__main__":
    datos = cargar_datos()

    for nombre, df in datos.items():
        print(f"{nombre}: {df.shape}")