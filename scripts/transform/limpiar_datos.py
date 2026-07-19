import json

import polars as pl

from config.config import RUTAS
from scripts.extract.cargar_datos import cargar_datos


def convertir_fechas(df):
    if "date" not in df.columns or df.schema["date"] == pl.Date:
        return df

    return df.with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d", strict=True)
    )


def eliminar_duplicados(df):
    filas_antes = df.height
    resultado = df.unique(maintain_order=True)
    return resultado, filas_antes - resultado.height


def imputar_nulos_genericos(df):
    imputaciones = {}

    for columna, tipo in df.schema.items():
        cantidad_nulos = df.get_column(columna).null_count()
        if cantidad_nulos == 0:
            continue

        valor = None
        criterio = None

        if tipo.is_numeric():
            valor = df.select(pl.col(columna).median()).item()
            criterio = "mediana"
        elif tipo in (pl.String, pl.Boolean):
            moda = df.get_column(columna).drop_nulls().mode()
            valor = moda[0] if len(moda) else None
            criterio = "moda"

        if valor is not None:
            df = df.with_columns(pl.col(columna).fill_null(valor))
            imputaciones[columna] = {
                "cantidad": cantidad_nulos,
                "criterio": criterio,
                "valor": str(valor),
            }

    return df, imputaciones


def limpiar_dataframe(df):
    nulos_antes = int(sum(df.null_count().row(0)))
    df = convertir_fechas(df)
    df, duplicados_eliminados = eliminar_duplicados(df)
    df, imputaciones = imputar_nulos_genericos(df)
    nulos_despues = int(sum(df.null_count().row(0)))

    return df, {
        "filas_finales": df.height,
        "duplicados_eliminados": duplicados_eliminados,
        "nulos_antes": nulos_antes,
        "nulos_despues": nulos_despues,
        "imputaciones": imputaciones,
    }


def limpiar_oil(df, fecha_inicio, fecha_fin):
    nulos_antes = int(df.get_column("dcoilwtico").null_count())
    filas_antes = df.height

    df = convertir_fechas(df)
    df, duplicados_eliminados = eliminar_duplicados(df)
    df = df.sort("date")

    calendario = pl.DataFrame(
        {
            "date": pl.date_range(
                fecha_inicio,
                fecha_fin,
                interval="1d",
                eager=True,
            )
        }
    )

    df = (
        calendario.join(df, on="date", how="left", validate="1:1")
        .sort("date")
        .with_columns(
            pl.col("dcoilwtico")
            .interpolate()
            .backward_fill()
            .forward_fill()
        )
    )

    nulos_despues = int(df.get_column("dcoilwtico").null_count())
    if nulos_despues > 0:
        raise ValueError(
            "No fue posible imputar todos los valores del precio del petróleo."
        )

    return df, {
        "filas_originales": filas_antes,
        "filas_finales": df.height,
        "fechas_diarias_agregadas": df.height - (filas_antes - duplicados_eliminados),
        "duplicados_eliminados": duplicados_eliminados,
        "nulos_antes": nulos_antes,
        "nulos_despues": nulos_despues,
        "criterio": "interpolación lineal; relleno de borde con valor más cercano",
    }


def guardar_datos_limpios(datos_limpios):
    RUTAS["processed"].mkdir(parents=True, exist_ok=True)
    archivos = {}

    for nombre, df in datos_limpios.items():
        ruta_salida = RUTAS["processed"] / f"{nombre}_limpio.parquet"
        df.write_parquet(ruta_salida)
        archivos[nombre] = str(ruta_salida)

    return archivos


def limpiar_datos():
    datos = cargar_datos()
    metricas = {}

    print("LIMPIEZA DE DATOS")
    print("-" * 60)

    train_limpio, metricas_train = limpiar_dataframe(datos["train"])
    datos["train"] = train_limpio
    metricas["train"] = {
        "filas_originales": metricas_train["filas_finales"]
        + metricas_train["duplicados_eliminados"],
        **metricas_train,
    }

    fecha_inicio = train_limpio.get_column("date").min()
    fecha_fin = train_limpio.get_column("date").max()

    for nombre in ["stores", "transactions", "holidays_events"]:
        filas_originales = datos[nombre].height
        datos[nombre], resumen = limpiar_dataframe(datos[nombre])
        metricas[nombre] = {
            "filas_originales": filas_originales,
            **resumen,
        }

    datos["oil"], metricas["oil"] = limpiar_oil(
        datos["oil"],
        fecha_inicio,
        fecha_fin,
    )

    archivos = guardar_datos_limpios(datos)

    ruta_metricas = RUTAS["processed"] / "limpieza_metricas.json"
    with open(ruta_metricas, "w", encoding="utf-8") as archivo:
        json.dump(metricas, archivo, indent=4, ensure_ascii=False)

    for nombre, df in datos.items():
        print(
            f"{nombre}: {df.height:,} filas; "
            f"duplicados eliminados: {metricas[nombre]['duplicados_eliminados']:,}; "
            f"nulos finales: {metricas[nombre]['nulos_despues']:,}"
        )

    print(f"Métricas guardadas en: {ruta_metricas}")
    return archivos, metricas


def ejecutar_limpieza():
    archivos, metricas = limpiar_datos()

    return {
        "estado": "ok",
        "archivos": archivos,
        "archivo_metricas": str(RUTAS["processed"] / "limpieza_metricas.json"),
        "filas_finales": {
            nombre: resumen["filas_finales"]
            for nombre, resumen in metricas.items()
        },
    }


if __name__ == "__main__":
    ejecutar_limpieza()
