# Databricks notebook source
# ============================================================
# COUCHE GOLD 2 — KPIs agrégés pour Power BI
# Catalog : veille_eco | Schema : gold
# ============================================================

from pyspark.sql.functions import (
    col, count, avg, round, when, 
    current_timestamp, lit, sum as spark_sum
)

# -------------------------------------------------------
# Configuration
# -------------------------------------------------------
CATALOG              = "veille_eco"
GOLD_ARTICLES        = f"{CATALOG}.gold.articles_enrichis"
GOLD_KPI_SENTIMENT   = f"{CATALOG}.gold.kpi_sentiment"
GOLD_KPI_SECTEURS    = f"{CATALOG}.gold.kpi_secteurs"
GOLD_KPI_ALERTES     = f"{CATALOG}.gold.kpi_alertes"
GOLD_KPI_TEMPORAL    = f"{CATALOG}.gold.kpi_temporal"

spark.sql(f"USE CATALOG {CATALOG}")

df = spark.table(GOLD_ARTICLES)
print(f"Articles enrichis chargés : {df.count():,}")

# -------------------------------------------------------
# KPI 1 — Baromètre du sentiment global
# -------------------------------------------------------
print("\n📊 KPI 1 — Baromètre sentiment...")

df_sentiment = df.groupBy("sentiment").agg(
    count("*").alias("nombre_articles")
).withColumn(
    "pourcentage",
    round(col("nombre_articles") / df.count() * 100, 1)
).withColumn("calcul_timestamp", current_timestamp())

df_sentiment.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_KPI_SENTIMENT)

print("Baromètre sentiment :")
df_sentiment.show()

# -------------------------------------------------------
# KPI 2 — Analyse par secteur
# -------------------------------------------------------
print("\n📊 KPI 2 — Analyse par secteur...")

df_secteurs = df.groupBy("secteur").agg(
    count("*").alias("nombre_articles"),
    round(avg("score_impact"), 2).alias("score_impact_moyen"),
    count(when(col("sentiment") == "positif", True)).alias("articles_positifs"),
    count(when(col("sentiment") == "négatif", True)).alias("articles_negatifs"),
    count(when(col("sentiment") == "neutre",  True)).alias("articles_neutres")
).orderBy("nombre_articles", ascending=False) \
 .withColumn("calcul_timestamp", current_timestamp())

df_secteurs.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_KPI_SECTEURS)

print("Analyse par secteur :")
df_secteurs.show()

# -------------------------------------------------------
# KPI 3 — Alertes (articles à fort impact)
# -------------------------------------------------------
print("\n📊 KPI 3 — Alertes articles à fort impact...")

df_alertes = df.filter(col("score_impact") >= 7) \
    .select(
        "titre", "source", "secteur", 
        "sentiment", "score_impact", 
        "resume", "date_publication", "url"
    ).orderBy("score_impact", ascending=False) \
     .withColumn("calcul_timestamp", current_timestamp())

df_alertes.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_KPI_ALERTES)

print(f"Alertes générées : {df_alertes.count()}")
df_alertes.show(5, truncate=True)

# -------------------------------------------------------
# KPI 4 — Evolution temporelle
# -------------------------------------------------------
print("\n📊 KPI 4 — Evolution temporelle...")

from pyspark.sql.functions import to_date

df_temporal = df.withColumn("date", to_date(col("date_publication"))) \
    .groupBy("date", "sentiment").agg(
        count("*").alias("nombre_articles"),
        round(avg("score_impact"), 2).alias("score_impact_moyen")
    ).orderBy("date", ascending=False) \
     .withColumn("calcul_timestamp", current_timestamp())

df_temporal.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_KPI_TEMPORAL)

print("Evolution temporelle :")
df_temporal.show(10)

# -------------------------------------------------------
# Résumé final
# -------------------------------------------------------
print("\n" + "="*50)
print("✅ TOUTES LES TABLES GOLD CRÉÉES !")
print("="*50)
print(f"📊 {GOLD_ARTICLES}     → {spark.table(GOLD_ARTICLES).count()} articles enrichis")
print(f"📊 {GOLD_KPI_SENTIMENT} → {spark.table(GOLD_KPI_SENTIMENT).count()} lignes")
print(f"📊 {GOLD_KPI_SECTEURS}  → {spark.table(GOLD_KPI_SECTEURS).count()} secteurs")
print(f"📊 {GOLD_KPI_ALERTES}   → {spark.table(GOLD_KPI_ALERTES).count()} alertes")
print(f"📊 {GOLD_KPI_TEMPORAL}  → {spark.table(GOLD_KPI_TEMPORAL).count()} lignes")
print("\n➡️  Connectez Power BI à ces tables pour le dashboard !")