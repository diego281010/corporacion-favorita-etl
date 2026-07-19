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

La VM se desplegó en el [portal de Azure](https://portal.azure.com/#home) usando la cuenta institucional EPN, acogiéndose al programa **Microsoft Azure for Students** ($100 en créditos, sin tarjeta de crédito).

**Paso 1: Ingresar a Azure y buscar "Máquinas virtuales"**
Se inicia sesión con la cuenta institucional y se busca la opción de Máquinas virtuales en el portal.

![Ingresar a máquinas virtuales](docs/img/Paso1.PNG)

**Paso 2: Activar la cuenta de estudiante**
Se selecciona la opción **Microsoft Azure for Students**, detectada automáticamente por el dominio institucional del correo.

![Microsoft Azure for Students](docs/img/Paso2.PNG)

**Paso 3: Iniciar gratis con la cuenta institucional**
Se confirma el inicio del proceso de activación con la cuenta EPN.

![Iniciar gratis - parte 1](docs/img/Paso3_a.png)
![Iniciar gratis - parte 2](docs/img/Paso3_b.png)

**Paso 4: Completar y verificar la cuenta**
Se llenan los campos requeridos (datos personales, verificación telefónica) para activar el crédito de estudiante.

![Verificar cuenta](docs/img/Paso4.png)

**Paso 5: Volver a Máquinas virtuales**
Ya con la suscripción activa, se regresa a la sección de Máquinas virtuales para crear el recurso.

![Volver a máquinas virtuales](docs/img/Paso5.PNG)

**Paso 6: Crear la máquina virtual**
Se selecciona **Crear → Máquina virtual**.

![Crear máquina virtual](docs/img/Paso6.png)

**Paso 7: Datos básicos**
Se configura el grupo de recursos, el nombre de la VM, la región, la imagen (**Ubuntu**) y el tamaño de la instancia, junto con la cuenta de administrador y las reglas de puerto de entrada (SSH).

![Datos básicos 1](docs/img/Paso7_a.png)
![Datos básicos 2](docs/img/Paso7_b.png)
![Datos básicos 3](docs/img/Paso7_c.png)
![Datos básicos 4](docs/img/Paso7_d.png)

**Paso 8: Discos**
No se modificó la configuración por defecto de discos.

![Discos 1](docs/img/Paso8_a.png)
![Discos 2](docs/img/Paso8_b.png)

**Paso 9: Redes y administración**
No se modificó la configuración por defecto de redes. En Administración se habilitó el **apagado automático** de la VM, para evitar consumir el crédito de Azure fuera del horario de uso del equipo.

![Redes 1](docs/img/Paso9_a.png)
![Redes 2](docs/img/Paso9_b.png)
![Administración 1](docs/img/Paso9_c.png)
![Administración 2](docs/img/Paso9_d.png)

**Paso 10: Revisar y crear**
Se revisan todas las especificaciones configuradas antes de confirmar la creación de la máquina virtual.
![Revisión final](docs/img/Paso10.png)
---

## 9. Conclusiones y recomendaciones

> Se completará al finalizar el proyecto
