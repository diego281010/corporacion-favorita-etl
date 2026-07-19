from scripts.eda.estacionalidad_feriados import ejecutar_feriados
from scripts.eda.petroleo_economia import ejecutar_petroleo
from scripts.eda.promociones import ejecutar_promociones
from scripts.eda.transacciones import ejecutar_transacciones
from scripts.eda.ventas_generales import ejecutar_ventas_generales

def ejecutar_eda_profundo_local():
    "Ejecutor local opcional, solo para testing"
    print("EDA PROFUNDO COMPLETO")
    print("="*60)

    resultados = {
            "ventas_generales" : ejecutar_ventas_generales(),
            "estacionalidad_feriados" : ejecutar_feriados(),
            "promociones" : ejecutar_promociones(),
            "petroleo_economia" : ejecutar_petroleo(),
            "transacciones" : ejecutar_transacciones(),
            }
    print("=" * 60)
    print("EDA profundo local completado")
    return resultados


if __name__ = "__main__":
    ejecutar_eda_profundo_local()
