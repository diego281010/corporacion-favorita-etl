from scripts.extract.cargar_datos import cargar_datos

def mostrar_info_dataset(nombre, df):
    print("="*60)
    print(nombre.upper())
    print("="*60)

    print(f"Filas: {df.height}")
    print(f"Columnas: {df.width}")

    print("\nColumnas:")
    print(df.columns)

    print("\nTipos de datos:")
    print(df.schema)

    print("\nPrimeras 10 filas:")
    print(df.head(10))

    print("\nUltimas 10 filas:")
    print(df.tail(10))

    print("\n")

def explorar_dataset():
    datos = cargar_datos()

    for nombre, df in datos.items():
        mostrar_info_dataset(nombre, df)

if __name__ == "__main__":
    explorar_dataset()