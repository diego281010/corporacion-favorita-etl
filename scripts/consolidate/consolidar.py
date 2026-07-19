import json

import polars as pl

from config.config import RUTAS

ARCHIVOS_LIMPIOS = {
    "train": "train_limpio.parquet",
    "stores": "stores_limpio.parquet",
    "oil": "oil_limpio.parquet",
    "holidays": "holidays_events_limpio.parquet",
    "transactions": "transactions_limpio.parquet",
}


def cargar_archivos_limpios():
    datos = {}

    for nombre, archivo in ARCHIVOS_LIMPIOS.items():
        ruta = RUTAS["processed"] / archivo
        if not ruta.exists():
            raise FileNotFoundError(
                f"No existe {ruta}. Ejecuta primero la tarea limpiar_datos."
            )

        try:
            datos[nombre] = pl.read_parquet(ruta)
        except Exception as error:
            raise RuntimeError(f"No fue posible leer {ruta}") from error

    return datos


def preparar_feriados(holidays):
    columnas_necesarias = {
        "date",
        "type",
        "locale",
        "description",
        "transferred",
    }
    faltantes = columnas_necesarias - set(holidays.columns)
    if faltantes:
        raise ValueError(
            f"Faltan columnas en holidays_events: {sorted(faltantes)}"
        )

    holidays = holidays.with_columns(
        (
            (pl.col("locale") == "National")
            & pl.col("type").is_in(
                ["Holiday", "Additional", "Bridge", "Transfer"]
            )
            & (~pl.col("transferred"))
        ).alias("es_feriado_nacional")
    )

    return (
        holidays.group_by("date")
        .agg(
            [
                pl.len().alias("cantidad_eventos"),
                pl.col("type")
                .unique()
                .sort()
                .str.join(", ")
                .alias("tipos_evento"),
                pl.col("locale")
                .unique()
                .sort()
                .str.join(", ")
                .alias("locales_evento"),
                pl.col("description")
                .unique()
                .sort()
                .str.join(", ")
                .alias("descripciones_evento"),
                pl.col("transferred").any().alias("evento_transferido"),
                pl.col("es_feriado_nacional")
                .any()
                .alias("es_feriado_nacional"),
            ]
        )
        .with_columns(pl.lit(True).alias("es_feriado_evento"))
        .sort("date")
    )


def consolidar():
    datos = cargar_archivos_limpios()
    train = datos["train"]
    stores = datos["stores"]
    oil = datos["oil"]
    holidays = datos["holidays"]
    transactions = datos["transactions"]

    print("CONSOLIDACIÓN")
    print("-" * 60)
    for nombre, df in datos.items():
        print(f"{nombre}: {df.height:,} filas")

    holidays_agrupado = preparar_feriados(holidays)
    print(f"holidays agrupado: {holidays_agrupado.height:,} fechas únicas")

    try:
        df = (
            train.join(
                stores,
                on="store_nbr",
                how="left",
                validate="m:1",
            )
            .join(
                transactions,
                on=["store_nbr", "date"],
                how="left",
                validate="m:1",
            )
            .join(
                oil,
                on="date",
                how="left",
                validate="m:1",
            )
            .join(
                holidays_agrupado,
                on="date",
                how="left",
                validate="m:1",
            )
            .with_columns(
                [
                    pl.col("transactions").fill_null(0),
                    pl.col("cantidad_eventos").fill_null(0),
                    pl.col("es_feriado_evento").fill_null(False),
                    pl.col("es_feriado_nacional").fill_null(False),
                    pl.col("evento_transferido").fill_null(False),
                ]
            )
        )
    except Exception as error:
        raise RuntimeError(
            "Falló uno de los joins de consolidación. "
            "Revisa duplicados en las claves de las tablas limpias."
        ) from error

    if df.height != train.height:
        raise RuntimeError(
            "La consolidación modificó la cantidad de filas: "
            f"train={train.height:,}, consolidado={df.height:,}."
        )

    tiendas_sin_metadata = df.filter(pl.col("city").is_null()).height
    precios_petroleo_nulos = df.get_column("dcoilwtico").null_count()

    if tiendas_sin_metadata > 0:
        raise RuntimeError(
            f"Existen {tiendas_sin_metadata:,} filas sin metadata de tienda."
        )

    if precios_petroleo_nulos > 0:
        raise RuntimeError(
            f"Existen {precios_petroleo_nulos:,} filas sin precio de petróleo. "
            "Revisa la generación diaria de oil_limpio.parquet."
        )

    RUTAS["processed"].mkdir(parents=True, exist_ok=True)
    archivo_salida = RUTAS["processed"] / "consolidado.parquet"
    df.write_parquet(archivo_salida)

    metricas = {
        "filas_train": train.height,
        "filas_consolidado": df.height,
        "columnas_consolidado": df.width,
        "fechas_eventos_unicas": holidays_agrupado.height,
        "filas_sin_transacciones": int(
            df.filter(pl.col("transactions") == 0).height
        ),
        "filas_sin_precio_petroleo": int(precios_petroleo_nulos),
        "tamanio_mb": round(archivo_salida.stat().st_size / (1024 * 1024), 2),
    }

    ruta_metricas = RUTAS["processed"] / "consolidacion_metricas.json"
    with open(ruta_metricas, "w", encoding="utf-8") as archivo:
        json.dump(metricas, archivo, indent=4, ensure_ascii=False)

    print(
        f"Consolidado: {df.height:,} filas, {df.width} columnas; "
        f"guardado en {archivo_salida}"
    )

    return {
        "estado": "ok",
        "archivo": str(archivo_salida),
        "archivo_metricas": str(ruta_metricas),
        **metricas,
    }


def ejecutar_consolidacion():
    return consolidar()


if __name__ == "__main__":
    ejecutar_consolidacion()
