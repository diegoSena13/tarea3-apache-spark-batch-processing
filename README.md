Procesamiento Batch de Transacciones Financieras con Apache Spark
Descripción del proyecto

Este proyecto implementa un proceso de análisis batch utilizando Apache Spark sobre un conjunto de datos de transacciones financieras almacenado en HDFS.

El objetivo es limpiar, transformar y analizar más de un millón de registros de transacciones para obtener indicadores relevantes sobre el comportamiento financiero de las cuentas, los tipos de transacción y la evolución temporal de las operaciones.

Además de los análisis estadísticos, el sistema genera archivos CSV con los resultados y diferentes gráficas en formato PNG para facilitar la interpretación de la información.

Dataset utilizado

El conjunto de datos contiene información de transacciones financieras y las siguientes variables:

Transaction_ID
Account_Number
Account_Name
Transaction_Amount
Transaction_Mode
Transaction_Type
Transaction_Date
Transaction_Source
Transaction_Destination
Tecnologías utilizadas
Apache Spark
PySpark
Hadoop HDFS
Python 3
Matplotlib
Funcionalidades implementadas
Limpieza y transformación de datos
Eliminación de registros nulos.
Conversión de tipos de datos.
Eliminación de transacciones duplicadas.
Creación de la variable Transaction_Month para análisis temporal.
Procesamiento batch

Se calculan los siguientes indicadores:

Total de transacciones.
Monto total procesado.
Monto promedio.
Valor mínimo y máximo de transacciones.
Transacciones por tipo.
Transacciones por modalidad.
Evolución mensual.
Top 10 cuentas por monto acumulado.
Análisis por origen y destino de transacciones.
Resultados generados
Datos limpios en formato Parquet.
Tablas de resultados en formato CSV.
Gráficas PNG con indicadores de análisis.


Estructura de salida
HDFS
├── datos_limpios_parquet
├── resultados_batch
│   ├── resumen_general
│   ├── transacciones_por_tipo
│   ├── transacciones_por_modalidad
│   ├── evolucion_mensual
│   ├── top_10_cuentas
│   └── transacciones_por_origen_destino
└── tarea3_graficas

Requisitos
Hadoop HDFS
Apache Spark
Python 3
Matplotlib
Ejecución

Ubicarse en el directorio donde se encuentra el archivo:
cd ~

Ejecutar el procesamiento batch:

spark-submit --master "local[1]" --driver-memory 512m \
--conf spark.sql.shuffle.partitions=2 \
--conf spark.default.parallelism=1 \
tarea3.py

Resultados esperados

Al finalizar el proceso se generan:

Datos limpios en formato Parquet.
Archivos CSV con los resultados del análisis.
Gráficas PNG almacenadas localmente y en HDFS.
Indicadores estadísticos mostrados en consola.
Autor

Diego
