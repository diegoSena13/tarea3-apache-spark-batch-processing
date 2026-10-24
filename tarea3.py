# -*- coding: utf-8 -*-
"""
Tarea 3 - Procesamiento Batch con Apache Spark
Lee SFinDSet.csv desde HDFS, limpia los datos, genera resúmenes CSV y
gráficas PNG, y guarda los resultados en HDFS.

Ejecutar:
spark-submit --master 'local[1]' --driver-memory 512m \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.default.parallelism=1 ~/tarea3.py
"""

import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, to_timestamp, date_format, count, sum as spark_sum,
    avg, min as spark_min, max as spark_max, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# ------------------------------------------------------------
# 1. RUTAS
# ------------------------------------------------------------
HDFS_BASE = "hdfs://localhost:9000"
ARCHIVO_ENTRADA = HDFS_BASE + "/Tarea3/SFinDSet.csv"
RUTA_PARQUET = HDFS_BASE + "/user/vboxuser/datos_limpios_parquet"
RUTA_RESULTADOS = HDFS_BASE + "/user/vboxuser/resultados_batch"
RUTA_GRAFICAS_HDFS = HDFS_BASE + "/user/vboxuser/tarea3_graficas"
RUTA_GRAFICAS_LOCAL = os.path.expanduser("~/tarea3_graficas")

# ------------------------------------------------------------
# 2. INICIAR SPARK CON CONFIGURACIÓN LIGERA
# ------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("Tarea3_Procesamiento_Batch")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.default.parallelism", "1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

try:
    # --------------------------------------------------------
    # 3. LEER CSV DESDE HDFS
    # --------------------------------------------------------
    esquema = StructType([
        StructField("Transaction_ID", StringType(), True),
        StructField("Account_Number", StringType(), True),
        StructField("Account_Name", StringType(), True),
        StructField("Transaction_Amount", StringType(), True),
        StructField("Transaction_Mode", StringType(), True),
        StructField("Transaction_Type", StringType(), True),
        StructField("Transaction_Date", StringType(), True),
        StructField("Transaction_Source", StringType(), True),
        StructField("Transaction_Destination", StringType(), True),
    ])

    print("\n[1/7] Leyendo el archivo desde HDFS...")
    df_original = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(esquema)
        .csv(ARCHIVO_ENTRADA)
    )

    print("Columnas:", df_original.columns)

    # --------------------------------------------------------
    # 4. LIMPIEZA Y TRANSFORMACIÓN
    # --------------------------------------------------------
    print("\n[2/7] Limpiando y transformando los datos...")
    df = df_original

    for nombre in df.columns:
        df = df.withColumn(
            nombre,
            when(col(nombre).isNull(), None).otherwise(trim(col(nombre)))
        )

    df = (
        df.withColumn(
            "Transaction_Amount",
            col("Transaction_Amount").cast(DoubleType())
        )
        .withColumn(
            "Transaction_Date",
            to_timestamp(col("Transaction_Date"), "M/d/yyyy H:mm")
        )
    )

    # Eliminar registros sin identificador, cuenta, monto o fecha válidos.
    df_limpio = (
        df.dropna(subset=[
            "Transaction_ID", "Account_Number",
            "Transaction_Amount", "Transaction_Date"
        ])
        .filter(col("Transaction_Amount") >= 0)
        .dropDuplicates(["Transaction_ID"])
        .withColumn("Transaction_Month", date_format(col("Transaction_Date"), "yyyy-MM"))
    )

    total_original = df_original.count()
    total_limpio = df_limpio.count()
    eliminados = total_original - total_limpio

    print(f"Registros leídos: {total_original:,}")
    print(f"Registros después de limpieza: {total_limpio:,}")
    print(f"Registros excluidos/duplicados: {eliminados:,}")

    # Guardar los datos limpios en Parquet en HDFS.
    print("\n[3/7] Guardando datos limpios en Parquet...")
    df_limpio.write.mode("overwrite").parquet(RUTA_PARQUET)
    print("Datos limpios guardados en:", RUTA_PARQUET)

    # --------------------------------------------------------
    # 5. ANÁLISIS BATCH
    # --------------------------------------------------------
    print("\n[4/7] Calculando indicadores y agrupaciones...")

    resumen_general = df_limpio.agg(
        count("*").alias("total_transacciones"),
        spark_sum("Transaction_Amount").alias("monto_total"),
        avg("Transaction_Amount").alias("monto_promedio"),
        spark_min("Transaction_Amount").alias("monto_minimo"),
        spark_max("Transaction_Amount").alias("monto_maximo")
    )

    por_tipo = (
        df_limpio.groupBy("Transaction_Type")
        .agg(count("*").alias("cantidad_transacciones"),
             spark_sum("Transaction_Amount").alias("monto_total"))
        .orderBy(col("cantidad_transacciones").desc())
    )

    por_modalidad = (
        df_limpio.groupBy("Transaction_Mode")
        .agg(count("*").alias("cantidad_transacciones"),
             spark_sum("Transaction_Amount").alias("monto_total"))
        .orderBy(col("cantidad_transacciones").desc())
    )

    por_mes = (
        df_limpio.groupBy("Transaction_Month")
        .agg(count("*").alias("cantidad_transacciones"),
             spark_sum("Transaction_Amount").alias("monto_total"))
        .orderBy("Transaction_Month")
    )

    top_cuentas = (
        df_limpio.groupBy("Account_Number")
        .agg(count("*").alias("cantidad_transacciones"),
             spark_sum("Transaction_Amount").alias("monto_total"))
        .orderBy(col("monto_total").desc())
        .limit(10)
    )

    por_origen_destino = (
        df_limpio.groupBy("Transaction_Source", "Transaction_Destination")
        .agg(count("*").alias("cantidad_transacciones"),
             spark_sum("Transaction_Amount").alias("monto_total"))
        .orderBy(col("cantidad_transacciones").desc())
    )

    # Mostrar los indicadores principales en pantalla.
    print("\n--- RESUMEN GENERAL ---")
    resumen_general.show(truncate=False)
    print("\n--- TRANSACCIONES POR TIPO ---")
    por_tipo.show(30, truncate=False)
    print("\n--- TRANSACCIONES POR MODALIDAD ---")
    por_modalidad.show(30, truncate=False)
    print("\n--- EVOLUCIÓN MENSUAL ---")
    por_mes.show(60, truncate=False)
    print("\n--- TOP 10 CUENTAS POR MONTO ---")
    top_cuentas.show(10, truncate=False)

    # --------------------------------------------------------
    # 6. GUARDAR RESULTADOS COMO CSV EN HDFS
    # --------------------------------------------------------
    print("\n[5/7] Guardando tablas de resultados en HDFS...")
    # Cada ruta será una carpeta Spark con part-*.csv y _SUCCESS.
    resultados = [
        ("resumen_general", resumen_general),
        ("transacciones_por_tipo", por_tipo),
        ("transacciones_por_modalidad", por_modalidad),
        ("evolucion_mensual", por_mes),
        ("top_10_cuentas", top_cuentas),
        ("transacciones_por_origen_destino", por_origen_destino),
    ]

    for nombre, tabla in resultados:
        ruta = RUTA_RESULTADOS + "/" + nombre
        (
            tabla.coalesce(1).write
            .mode("overwrite")
            .option("header", True)
            .option("encoding", "UTF-8")
            .csv(ruta)
        )
        print(f"Guardado: {ruta}")

    # --------------------------------------------------------
    # 7. CREAR GRÁFICAS Y COPIARLAS A HDFS SIN USAR 'hdfs' CLI
    # --------------------------------------------------------
    print("\n[6/7] Generando gráficas PNG...")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    os.makedirs(RUTA_GRAFICAS_LOCAL, exist_ok=True)

    def guardar_grafica(nombre_archivo):
        ruta_local = os.path.join(RUTA_GRAFICAS_LOCAL, nombre_archivo)
        plt.tight_layout()
        plt.savefig(ruta_local, dpi=150, bbox_inches="tight")
        plt.close()
        return ruta_local

    # Gráfica 1: cantidad por tipo de transacción
    filas = por_tipo.collect()
    if filas:
        etiquetas = [str(f["Transaction_Type"]) for f in filas]
        valores = [int(f["cantidad_transacciones"]) for f in filas]
        plt.figure(figsize=(9, 5))
        plt.bar(etiquetas, valores)
        plt.title("Transacciones por tipo")
        plt.xlabel("Tipo de transacción")
        plt.ylabel("Cantidad")
        plt.xticks(rotation=25, ha="right")
        guardar_grafica("01_transacciones_por_tipo.png")

    # Gráfica 2: cantidad por modalidad
    filas = por_modalidad.collect()
    if filas:
        etiquetas = [str(f["Transaction_Mode"]) for f in filas]
        valores = [int(f["cantidad_transacciones"]) for f in filas]
        plt.figure(figsize=(9, 5))
        plt.bar(etiquetas, valores)
        plt.title("Transacciones por modalidad")
        plt.xlabel("Modalidad")
        plt.ylabel("Cantidad")
        plt.xticks(rotation=25, ha="right")
        guardar_grafica("02_transacciones_por_modalidad.png")

    # Gráfica 3: cantidad mensual
    filas = por_mes.collect()
    if filas:
        etiquetas = [str(f["Transaction_Month"]) for f in filas]
        valores = [int(f["cantidad_transacciones"]) for f in filas]
        plt.figure(figsize=(12, 5))
        plt.plot(etiquetas, valores, marker="o", linewidth=1.5)
        plt.title("Evolución mensual de las transacciones")
        plt.xlabel("Mes")
        plt.ylabel("Cantidad de transacciones")
        plt.xticks(rotation=70, ha="right", fontsize=8)
        guardar_grafica("03_evolucion_mensual_cantidad.png")

    # Gráfica 4: monto mensual
    filas = por_mes.collect()
    if filas:
        etiquetas = [str(f["Transaction_Month"]) for f in filas]
        valores = [float(f["monto_total"] or 0) for f in filas]
        plt.figure(figsize=(12, 5))
        plt.plot(etiquetas, valores, marker="o", linewidth=1.5)
        plt.title("Monto total mensual")
        plt.xlabel("Mes")
        plt.ylabel("Monto total")
        plt.xticks(rotation=70, ha="right", fontsize=8)
        guardar_grafica("04_monto_mensual.png")

    # Gráfica 5: diez cuentas con mayor monto total
    filas = top_cuentas.collect()
    if filas:
        etiquetas = [str(f["Account_Number"]) for f in filas]
        valores = [float(f["monto_total"] or 0) for f in filas]
        plt.figure(figsize=(10, 5))
        plt.bar(etiquetas, valores)
        plt.title("Top 10 cuentas por monto acumulado")
        plt.xlabel("Cuenta")
        plt.ylabel("Monto acumulado")
        plt.xticks(rotation=45, ha="right")
        guardar_grafica("05_top_10_cuentas.png")

    # Copiar PNG locales a HDFS usando Hadoop FileSystem a través de Spark.
    print("\n[7/7] Copiando gráficas a HDFS...")
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hdfs_uri = jvm.java.net.URI(HDFS_BASE)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hdfs_uri, hadoop_conf)
    ruta_remota_graficas = jvm.org.apache.hadoop.fs.Path(RUTA_GRAFICAS_HDFS)
    fs.mkdirs(ruta_remota_graficas)

    cantidad_graficas = 0
    for nombre in sorted(os.listdir(RUTA_GRAFICAS_LOCAL)):
        if nombre.lower().endswith(".png"):
            local_path = jvm.org.apache.hadoop.fs.Path(
                "file://" + os.path.abspath(os.path.join(RUTA_GRAFICAS_LOCAL, nombre))
            )
            remote_path = jvm.org.apache.hadoop.fs.Path(
                RUTA_GRAFICAS_HDFS + "/" + nombre
            )
            fs.copyFromLocalFile(False, True, local_path, remote_path)
            print("Gráfica guardada en HDFS:", RUTA_GRAFICAS_HDFS + "/" + nombre)
            cantidad_graficas += 1

    print("\nPROCESO FINALIZADO.")
    print("Parquet limpio:", RUTA_PARQUET)
    print("Resultados CSV:", RUTA_RESULTADOS)
    print("Gráficas PNG en HDFS:", RUTA_GRAFICAS_HDFS)
    print("Cantidad de gráficas copiadas:", cantidad_graficas)
    print("También se conservaron las gráficas localmente en:", RUTA_GRAFICAS_LOCAL)

finally:
    spark.stop()