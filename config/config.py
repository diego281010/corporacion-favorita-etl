import os
from pathlib import Path

from dotenv import load_dotenv

RUTA_RAIZ = Path(__file__).resolve().parent.parent
RUTA_ENV = RUTA_RAIZ / ".env"

load_dotenv(RUTA_ENV)


def obtener_variable_entorno(env):
    variable = os.getenv(env)
    if variable:
        return variable

    raise RuntimeError(f"La variable {env} no está definida en el archivo .env")


BASE_DATOS = {
    "host": obtener_variable_entorno("DB_HOST"),
    "puerto": obtener_variable_entorno("DB_PORT"),
    "nombre": obtener_variable_entorno("DB_NAME"),
    "usuario": obtener_variable_entorno("DB_USER"),
    "password": obtener_variable_entorno("DB_PASSWORD"),
}

RUTAS = {
    "raiz": RUTA_RAIZ,
    "raw": RUTA_RAIZ / "data" / "raw",
    "processed": RUTA_RAIZ / "data" / "processed",
    "eda_profundo": RUTA_RAIZ / "data" / "processed" / "eda_profundo",
    "reports": RUTA_RAIZ / "data" / "reports",
}

URL_BASE_DATOS = (
    f"postgresql+psycopg2://{BASE_DATOS['usuario']}:{BASE_DATOS['password']}"
    f"@{BASE_DATOS['host']}:{BASE_DATOS['puerto']}/{BASE_DATOS['nombre']}"
)
