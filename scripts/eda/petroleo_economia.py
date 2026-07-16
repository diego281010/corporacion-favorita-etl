import polars as pl
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

def analisis_petroleo(df):
    resultados = {}

    print("PETROLEO Y ECONOMIA")
    print("-" * 40)

    if "dcoilwtico" in df.columns:
        print("4.1 CORRELACION PETROLEO VS VENTAS")
        petroleo_ventas_mensual = (
            df.group_by(
                pl.col("date").dt.year().alias("anio"),
                pl.col("date").dt.month().alias("mes")
            )
            .agg([
                pl.col("dcoilwtico").mean().alias("precio_petroleo_promedio"),
                pl.col("sales").sum().alias("ventas_totales")
            ])
            .sort(["anio", "mes"])
        )
        correlacion = petroleo_ventas_mensual.select(
            pl.corr("precio_petroleo_promedio", "ventas_totales")
        ).item()
        print(f"   Correlacion: {correlacion:.3f}")
        resultados["petroleo_ventas"] = {
            "correlacion": correlacion,
            "datos_mensuales": petroleo_ventas_mensual.to_dicts()
        }

        # 4.2 Caida 2015-2016
        print("\n4.2 CAIDA DEL PETROLEO 2015-2016")
        caida_2015 = petroleo_ventas_mensual.filter(
            (pl.col("anio") >= 2015) & (pl.col("anio") <= 2016)
        )
        if caida_2015.height > 0:
            print("   Mes    | Petroleo | Ventas")
            for row in caida_2015.rows():
                print(f"   {row[0]}-{row[1]:02d} | ${row[2]:.2f} | ${row[3]:,.2f}")

            print("\n   Lag temporal (petroleo vs ventas desfasadas, 2015-2016):")
            serie_petroleo = caida_2015.get_column("precio_petroleo_promedio")
            serie_ventas = caida_2015.get_column("ventas_totales")

            correlaciones_lag = []
            for lag in range(0, 7):
                df_lag = pl.DataFrame({
                    "precio_petroleo_promedio": serie_petroleo,
                    "ventas_totales_desfasadas": serie_ventas.shift(-lag)
                }).drop_nulls()

                corr_lag = (
                    df_lag.select(
                        pl.corr("precio_petroleo_promedio", "ventas_totales_desfasadas")
                    ).item()
                    if df_lag.height > 2 else None
                )
                correlaciones_lag.append({"lag_meses": lag, "correlacion": corr_lag})
                corr_str = f"{corr_lag:.3f}" if corr_lag is not None else "N/A"
                print(f"      Lag {lag} mes(es): correlacion = {corr_str}")

            lags_validos = [r for r in correlaciones_lag if r["correlacion"] is not None]
            mejor_lag = max(lags_validos, key=lambda r: abs(r["correlacion"])) if lags_validos else None
            if mejor_lag:
                print(
                    f"   -> Lag con correlacion de mayor magnitud: "
                    f"{mejor_lag['lag_meses']} mes(es) (r = {mejor_lag['correlacion']:.3f})"
                )

            resultados["lag_temporal_petroleo_ventas"] = {
                "correlaciones_por_lag": correlaciones_lag,
                "mejor_lag_meses": mejor_lag["lag_meses"] if mejor_lag else None,
            }

        # 4.3 Ciudades mas sensibles
        print("\n4.3 CIUDADES MAS SENSIBLES")
        df_con_anio = df.with_columns(pl.col("date").dt.year().alias("anio"))
        caida_ciudades = (
            df_con_anio
            .filter(pl.col("anio").is_in([2014, 2016]))
            .group_by(["city", "anio"])
            .agg(pl.col("sales").sum().alias("ventas_totales"))
            .sort(["city", "anio"])
        )
        cambio_ciudades = (
            caida_ciudades
            .pivot(on="anio", index="city", values="ventas_totales")
            .with_columns(
                ((pl.col("2016") - pl.col("2014")) / pl.col("2014") * 100)
                .alias("cambio_porcentual")
            )
            .sort("cambio_porcentual")
        )
        print("Mayor caida:")
        for row in cambio_ciudades.head(5).rows():
            print(f"   {row[0]}: {row[3]:.1f}%")
        print("Mayor crecimiento:")
        for row in cambio_ciudades.tail(5).rows():
            print(f"   {row[0]}: +{row[3]:.1f}%")
        resultados["sensibilidad_ciudades_petroleo"] = cambio_ciudades.to_dicts()

    else:
        print("   No se encontro la columna 'dcoilwtico'")

    return resultados