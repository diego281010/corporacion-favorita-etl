import json

from config.config import RUTAS
from scripts.extract.cargar_datos import cargar_datos


def obtener_rango_fechas(df):
    if "date" not in df.columns:
        return None

    fecha_min = df.select("date").min().item()
    fecha_max = df.select("date").max().item()

    return {
        "inicio": str(fecha_min),
        "fin": str(fecha_max),
    }


def obtener_nulos(df):
    conteos = df.null_count().row(0, named=True)

    return {
        columna: {
            "cantidad": int(cantidad),
            "porcentaje": round((cantidad / df.height * 100), 4) if df.height else 0,
        }
        for columna, cantidad in conteos.items()
    }


def generar_diagnostico(df):
    return {
        "filas": df.height,
        "columnas": df.width,
        "tipos_datos": {columna: str(tipo) for columna, tipo in df.schema.items()},
        "nulos": obtener_nulos(df),
        "duplicados": df.height - df.unique().height,
        "rango_fechas": obtener_rango_fechas(df),
    }


def eda_inicial():
    datos = cargar_datos()
    reporte = {}

    print("EDA INICIAL")
    print("=" * 60)

    for nombre, df in datos.items():
        reporte[nombre] = generar_diagnostico(df)
        print(
            f"{nombre}: {df.height:,} filas, {df.width} columnas, "
            f"{reporte[nombre]['duplicados']:,} duplicados"
        )

    RUTAS["processed"].mkdir(parents=True, exist_ok=True)
    ruta_reporte = RUTAS["processed"] / "eda_inicial.json"

    with open(ruta_reporte, "w", encoding="utf-8") as archivo:
        json.dump(reporte, archivo, indent=4, ensure_ascii=False)

    print(f"Reporte guardado en: {ruta_reporte}")
    return reporte


def ejecutar_eda_inicial():
    reporte = eda_inicial()

    return {
        "estado": "ok",
        "archivo": str(RUTAS["processed"] / "eda_inicial.json"),
        "datasets_analizados": len(reporte),
    }


if __name__ == "__main__":
    ejecutar_eda_inicial()
