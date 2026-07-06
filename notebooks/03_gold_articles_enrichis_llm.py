# Databricks notebook source
import requests
import json
import time
import re
import pandas as pd
from pyspark.sql.functions import current_timestamp

# -------------------------------------------------------
# Configuration
# -------------------------------------------------------
CATALOG      = "veille_eco"
SILVER_TABLE = f"{CATALOG}.silver.articles"
GOLD_TABLE   = f"{CATALOG}.gold.articles_enrichis"
MISTRAL_KEY  = "QNYH5Ww67uQqSFsATKewa2bKnJGjb0WO"

spark.sql(f"USE CATALOG {CATALOG}")

# -------------------------------------------------------
# Lecture Silver
# -------------------------------------------------------
df_silver = spark.table(SILVER_TABLE)
print(f"Articles Silver : {df_silver.count():,}")

# -------------------------------------------------------
# Fonction nettoyage JSON
# -------------------------------------------------------
def nettoyer_json(texte):
    texte = re.sub(r"```json\s*", "", texte)
    texte = re.sub(r"```\s*", "", texte)
    texte = texte.strip()
    return texte

# -------------------------------------------------------
# Fonction d'analyse Mistral avec retry
# -------------------------------------------------------
def analyser_article(titre, description, max_retries=3):
    headers = {
        "Authorization": f"Bearer {MISTRAL_KEY}",
        "Content-Type": "application/json"
    }
    
    prompt = f"""Analyse cet article et réponds UNIQUEMENT en JSON sans markdown :
{{
  "sentiment": "positif" ou "négatif" ou "neutre",
  "secteur": "Tech" ou "Finance" ou "Energie" ou "Santé" ou "Distribution" ou "Industrie" ou "Autre",
  "resume": "résumé en 1 phrase",
  "score_impact": nombre entier de 1 à 10,
  "mots_cles": "3 mots clés"
}}

Titre : {titre}
Description : {description}"""

    payload = {
        "model"      : "mistral-small-latest",
        "messages"   : [{"role": "user", "content": prompt}],
        "max_tokens" : 200,
        "temperature": 0.1
    }
    
    for tentative in range(max_retries):
        try:
            response = requests.post(
                "https://api.mistral.ai/v1/chat/completions",
                headers=headers,
                json=payload,
                timeout=30
            )
            
            data = response.json()
            
            # Vérifier le rate limit
            if response.status_code == 429:
                print(f"  ⏳ Rate limit atteint — attente 10 secondes...")
                time.sleep(10)
                continue
            
            if 'choices' not in data:
                print(f"  ⚠️ Pas de choices : {data}")
                time.sleep(5)
                continue
                
            content = data['choices'][0]['message']['content']
            content = nettoyer_json(content)
            result  = json.loads(content)
            return json.dumps(result, ensure_ascii=False)
            
        except Exception as e:
            print(f"  ⚠️ Tentative {tentative+1}/{max_retries} — Erreur : {e}")
            time.sleep(5)
    
    return json.dumps({
        "sentiment"   : "inconnu",
        "secteur"     : "Autre",
        "resume"      : "Analyse non disponible",
        "score_impact": 0,
        "mots_cles"   : ""
    })

# -------------------------------------------------------
# Test sur 1 article
# -------------------------------------------------------
print("\nTest sur 1 article...")
test = analyser_article(
    "KNDS fabricant d'armes franco-allemand",
    "Le groupe franco-allemand annonce une expansion"
)
print(f"Résultat : {test}")

# -------------------------------------------------------
# Boucle sur tous les articles
# -------------------------------------------------------
print("\nAnalyse des 70 articles avec Mistral AI...")
print("(délai de 2 secondes entre chaque appel pour éviter le rate limiting)")

articles  = df_silver.select(
    "url", "titre", "description",
    "source", "date_publication", "mot_cle"
).collect()

resultats = []
for i, row in enumerate(articles):
    analyse      = analyser_article(row['titre'], row['description'])
    analyse_dict = json.loads(analyse)
    
    resultats.append({
        "url"             : row['url'],
        "titre"           : row['titre'],
        "source"          : row['source'],
        "date_publication": str(row['date_publication']),
        "mot_cle"         : row['mot_cle'],
        "sentiment"       : analyse_dict.get('sentiment',    'inconnu'),
        "secteur"         : analyse_dict.get('secteur',      'Autre'),
        "resume"          : analyse_dict.get('resume',       ''),
        "score_impact"    : int(analyse_dict.get('score_impact', 0)),
        "mots_cles"       : analyse_dict.get('mots_cles',    '')
    })
    
    if (i + 1) % 10 == 0:
        print(f"  {i + 1}/{len(articles)} articles analysés...")
    
    time.sleep(2)  # 2 secondes entre chaque appel

print(f"✅ {len(resultats)} articles analysés !")

# -------------------------------------------------------
# Écriture Gold
# -------------------------------------------------------
df_gold = spark.createDataFrame(pd.DataFrame(resultats)) \
    .withColumn("gold_timestamp", current_timestamp())

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_TABLE)

count = spark.table(GOLD_TABLE).count()
print(f"\n✅ Gold articles_enrichis terminé !")
print(f"Total articles enrichis : {count:,}")
spark.table(GOLD_TABLE).show(5, truncate=True)

print("\nDistribution des sentiments :")
spark.table(GOLD_TABLE).groupBy("sentiment").count().orderBy("count", ascending=False).show()

print("\nDistribution des secteurs :")
spark.table(GOLD_TABLE).groupBy("secteur").count().orderBy("count", ascending=False).show()