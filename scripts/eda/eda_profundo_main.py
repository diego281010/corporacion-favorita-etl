import polars as pl
import json
from pathlib import Path

# Importaciones desde la misma carpeta eda
from ventas_generales import analisis_ventas_generales
from estacionalidad_feriados import analisis_feriados
from promociones import analisis_promociones
from petroleo_economia import analisis_petroleo
from transacciones import analisis_transacciones

# Configuración de rutas
PROJECT_ROOT = Path(__file__).parent.parent.parent 
INPUT_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"

def eda_profundo():
    print("=" * 60)
    print("EDA PROFUNDO - Analisis Detallado")
    print("=" * 60)

    archivo_consolidado = INPUT_DIR / "consolidado.parquet"
    if not archivo_consolidado.exists():
        print(f"ERROR: No encuentro {archivo_consolidado}")
        print("   Ejecuta primero consolidar.py")
        return

    print(f"\nCargando datos consolidados...")
    df = pl.read_parquet(archivo_consolidado)
    print(f"   {df.height:,} filas, {df.width} columnas")

    resultados = {}

    print("\n" + "=" * 60)
    print("1. VENTAS GENERALES")
    print("=" * 60)
    resultados.update(analisis_ventas_generales(df))

    print("\n" + "=" * 60)
    print("2. ESTACIONALIDAD Y FERIADOS")
    print("=" * 60)
    resultados.update(analisis_feriados(df))

    print("\n" + "=" * 60)
    print("3. PROMOCIONES")
    print("=" * 60)
    resultados.update(analisis_promociones(df))

    print("\n" + "=" * 60)
    print("4. PETROLEO Y ECONOMIA")
    print("=" * 60)
    resultados.update(analisis_petroleo(df))

    print("\n" + "=" * 60)
    print("5. TRANSACCIONES")
    print("=" * 60)
    resultados.update(analisis_transacciones(df))

    print("\nGuardando resultados...")
    archivo_salida = OUTPUT_DIR / "resultados_eda_profundo.json"
    with open(archivo_salida, "w") as f:
        json.dump(resultados, f, indent=2, default=str)
    print(f"   Guardado en: {archivo_salida}")

    print("\n" + "=" * 60)
    print("EDA Profundo completado exitosamente")
    print("=" * 60)

    return resultados

if __name__ == "__main__":
    eda_profundo()