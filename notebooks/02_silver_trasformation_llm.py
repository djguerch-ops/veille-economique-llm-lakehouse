# Databricks notebook source
# ============================================================
# COUCHE SILVER — Nettoyage et standardisation des articles
# Catalog : veille_eco | Schema : silver
# ============================================================

from pyspark.sql.functions import (
    col, to_timestamp, length, trim, 
    when, regexp_replace, current_timestamp
)

# -------------------------------------------------------
# Configuration
# -------------------------------------------------------
CATALOG        = "veille_eco"
BRONZE_TABLE   = f"{CATALOG}.bronze.articles"
SILVER_TABLE   = f"{CATALOG}.silver.articles"

spark.sql(f"USE CATALOG {CATALOG}")

# -------------------------------------------------------
# Lecture Bronze
# -------------------------------------------------------
print(f"Lecture de {BRONZE_TABLE}...")
df_bronze = spark.table(BRONZE_TABLE)
print(f"Articles Bronze : {df_bronze.count():,}")

# -------------------------------------------------------
# Nettoyage et standardisation
# -------------------------------------------------------
df_silver = df_bronze \
    .filter(col("titre").isNotNull()) \
    .filter(col("description").isNotNull()) \
    .filter(length(trim(col("titre"))) > 10) \
    .filter(length(trim(col("description"))) > 20) \
    .withColumn("titre", trim(regexp_replace(col("titre"), r"\s+", " "))) \
    .withColumn("description", trim(regexp_replace(col("description"), r"\s+", " "))) \
    .withColumn("date_publication", to_timestamp(col("date_publication"))) \
    .withColumn("source", trim(col("source"))) \
    .dropDuplicates(["url"]) \
    .withColumn("silver_timestamp", current_timestamp())

# -------------------------------------------------------
# Écriture Silver
# -------------------------------------------------------
print(f"Écriture dans {SILVER_TABLE}...")

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(SILVER_TABLE)

# -------------------------------------------------------
# Validation
# -------------------------------------------------------
silver_count = spark.table(SILVER_TABLE).count()
bronze_count = df_bronze.count()
rejetes      = bronze_count - silver_count

print(f"\n✅ Silver transformation terminée !")
print(f"Bronze articles  : {bronze_count:,}")
print(f"Silver articles  : {silver_count:,}")
print(f"Articles rejetés : {rejetes:,}")
spark.table(SILVER_TABLE).show(5, truncate=True)