import json

from scripts.extract.cargar_datos import cargar_datos
from config.config import RUTAS

def obtener_rango_fechas(df):
    if "date" not in df.columns:
        return None
    
    fecha_min = df.select("date").min().item()
    fecha_max = df.select("date").max().item()

    return {
        "inicio" : str(fecha_min),
        "fin" : str(fecha_max),
    }

def generar_diagnostico(df):
    return {
        "filas" : df.height,
        "columnas" : df.width,
        "tipos_datos" : {columna : str(tipo) for columna, tipo in df.schema.items()},
        "nulos" : df.null_count().row(0, named=True),
        "duplicados" : df.height - df.unique().height,
        "rango_fechas" : obtener_rango_fechas(df),
    }

def eda_inicial():
    datos = cargar_datos()
    reporte = {}

    for nombre, df in datos.items():
        reporte[nombre] = generar_diagnostico(df)

    ruta_reporte = RUTAS["processed"] / "eda_inicial.json"

    with open(ruta_reporte, "w", encoding="utf-8") as archivo:
        json.dump(reporte, archivo, indent=4, ensure_ascii=False)

    return reporte

if __name__ == "__main__":
    reporte = eda_inicial()

    for nombre, info in reporte.items():
        print(f"{nombre} : {info['filas']} filas, {info['columnas']} columnas")