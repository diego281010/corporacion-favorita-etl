# PROYECTO ETL - Corporación Favorita

**Integrantes:**
- Alejandro Aguirre
- Diego Almeida

---

## 1. Descripción del proyecto

> Este proyecto implementa un pipeline ETL para analizar el histórico de ventas de Corporación Favorita, la principal cadena de retail de Ecuador, utilizando el dataset público Store Sales – Time Series Forecasting de Kaggle. El pipeline transforma cinco archivos CSV en un conjunto de datos limpio y consolidado, y a partir de ahí responde preguntas de análisis descriptivo sobre estacionalidad, promociones, comportamiento por tienda y ciudad, y la relación entre el precio del petróleo y las ventas, sin construir ningún modelo predictivo.

> El pipeline desarrollado se ejecuta sobre una máquina virtual Ubuntu desplegada en Azure (portal.azure.com), donde corren Apache Airflow como el orquestador y PostgreSQL como la base de datos usada como destino. Python con la librería Polars se usa para la carga, limpieza, consolidación y análisis exploratorio de los datos; los resultados limpios y consolidados se persisten en PostgreSQL, y desde ahí Power BI se conecta en tiempo real para permitir la visualizacion de los resultados finales.

---

## 2. Descripción de los archivos del dataset y su rol en el pipeline

**Fuente:** Store Sales – Time Series Forecasting (Kaggle)
https://www.kaggle.com/competitions/store-sales-time-series-forecasting

| Archivo | Descripción | Rol en el pipeline |
|---|---|---|
| `train.csv` | Ventas diarias por tienda, familia de producto y promoción. Más de 3 millones de registros | Archivo principal del análisis |
| `stores.csv` | Metadata de las 54 tiendas: ciudad, provincia, tipo y clúster | Dimensión para consolidar por tienda |
| `transactions.csv` | Número de transacciones por tienda y fecha | Relación entre transacciones y volumen de ventas |
| `oil.csv` | Precio diario del petróleo. Incluye nulos en fines de semana/feriados | Análisis de correlación con la economía |
| `holidays_events.csv` | Feriados nacionales, regionales y locales de Ecuador, con tipo y bandera de transferencia | Análisis de estacionalidad |

> Nota: los archivos CSV **no** se suben al repositorio de GitHub. Solo existen en la carpeta local de datos de la VM.

---

## 3. Diagrama de arquitectura de la solución

![Arquitectura del pipeline](docs/img/arquitectura.PNG)

---

## 4. Descripción del DAG

> Pendiente de implementación

---

## 5. Proceso del pipeline

> Pendiente de implementación

---

## 6. Métricas del pipeline

> Pendiente de ejecución del pipeline

---

## 7. Captura del dashboard de Power BI

> Pendiente de implementación

---

## 8. Despliegue

>

---

## 9. Conclusiones y recomendaciones

> Se completará al finalizar el proyecto
