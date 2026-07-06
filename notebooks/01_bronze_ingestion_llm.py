# Databricks notebook source
import requests
import pandas as pd
from pyspark.sql.functions import current_timestamp

# -------------------------------------------------------
# Configuration
# -------------------------------------------------------
CATALOG        = "veille_eco"
SCHEMA         = "bronze"
TABLE_ARTICLES = f"{CATALOG}.{SCHEMA}.articles"
NEWSAPI_KEY    = "5bf7df8e2c764598913d0e9aed3bf618"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# -------------------------------------------------------
# Récupération des articles via NewsAPI
# -------------------------------------------------------
print("Récupération des articles...")

articles = []
mots_cles = ["economie france", "bourse paris", "CAC40", "entreprise france", "finance france"]

for mot in mots_cles:
    url = (
        f"https://newsapi.org/v2/everything"
        f"?q={mot}"
        f"&language=fr"
        f"&pageSize=20"
        f"&sortBy=publishedAt"
        f"&apiKey={NEWSAPI_KEY}"
    )
    response = requests.get(url)
    data = response.json()
    
    for article in data.get('articles', []):
        articles.append({
            "source"           : article.get('source', {}).get('name', ''),
            "auteur"           : article.get('author', ''),
            "titre"            : article.get('title', ''),
            "description"      : article.get('description', ''),
            "contenu"          : article.get('content', ''),
            "url"              : article.get('url', ''),
            "date_publication" : article.get('publishedAt', ''),
            "langue"           : "fr",
            "mot_cle"          : mot
        })
    print(f"  ✅ '{mot}' → {len(data.get('articles', []))} articles")

print(f"\nTotal articles récupérés : {len(articles)}")

# -------------------------------------------------------
# Suppression des doublons
# -------------------------------------------------------
df_pandas = pd.DataFrame(articles).drop_duplicates(subset=['url'])
print(f"Articles après déduplication : {len(df_pandas)}")

# -------------------------------------------------------
# Écriture en table Delta Bronze
# -------------------------------------------------------
df_bronze = spark.createDataFrame(df_pandas) \
    .withColumn("ingestion_timestamp", current_timestamp())

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TABLE_ARTICLES)

# -------------------------------------------------------
# Validation
# -------------------------------------------------------
count = spark.table(TABLE_ARTICLES).count()
print(f"\n✅ Bronze ingestion terminée !")
print(f"Total articles dans la table : {count:,}")
spark.table(TABLE_ARTICLES).show(5, truncate=True)