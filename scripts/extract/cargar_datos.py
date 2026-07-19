import polars as pl

from config.config import RUTAS

ARCHIVOS_CSV = {
    "train": "train.csv",
    "stores": "stores.csv",
    "transactions": "transactions.csv",
    "holidays_events": "holidays_events.csv",
    "oil": "oil.csv",
}

COLUMNAS_ESPERADAS = {
    "train": {"id", "date", "store_nbr", "family", "sales", "onpromotion"},
    "stores": {"store_nbr", "city", "state", "type", "cluster"},
    "transactions": {"date", "store_nbr", "transactions"},
    "holidays_events": {
        "date",
        "type",
        "locale",
        "locale_name",
        "description",
        "transferred",
    },
    "oil": {"date", "dcoilwtico"},
}


def cargar_datos():
    datos = {}

    for nombre, archivo in ARCHIVOS_CSV.items():
        ruta_archivo = RUTAS["raw"] / archivo

        if not ruta_archivo.exists():
            raise FileNotFoundError(f"No existe el archivo requerido: {ruta_archivo}")

        try:
            df = pl.read_csv(ruta_archivo)
        except Exception as error:
            raise RuntimeError(f"No fue posible cargar el archivo {archivo}") from error

        columnas_faltantes = COLUMNAS_ESPERADAS[nombre] - set(df.columns)
        if columnas_faltantes:
            raise ValueError(
                f"El archivo {archivo} no contiene las columnas esperadas: "
                f"{sorted(columnas_faltantes)}"
            )

        datos[nombre] = df

    return datos


def ejecutar_carga():
    datos = cargar_datos()
    resumen = {}

    print("CARGA DE DATOS")
    print("-" * 60)

    for nombre, df in datos.items():
        resumen[nombre] = {
            "filas": df.height,
            "columnas": df.width,
            "archivo": str(RUTAS["raw"] / ARCHIVOS_CSV[nombre]),
        }
        print(f"{nombre}: {df.height:,} filas, {df.width} columnas")

    return resumen


if __name__ == "__main__":
    ejecutar_carga()
