# test_cluster_complet_corrige.py
from pyspark.sql import SparkSession
import time

print("="*50)
print("🚀 TEST DU CLUSTER SPARK MULTI-NODE")
print("="*50)

# Connexion au cluster
spark = SparkSession.builder \
    .appName("Test_Cluster_PFE") \
    .master("spark://localhost:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

print("\n✅ CONNEXION ÉTABLIE")
print("="*50)

# Infos de base
print(f"\n📊 Version Spark: {spark.version}")
print(f"📊 Master URL: {spark.sparkContext.master}")
print(f"📊 Application ID: {spark.sparkContext.applicationId}")

# Test 1: Compter les exécuteurs (méthode simple)
print("\n🔍 RECHERCHE DES EXÉCUTEURS...")
try:
    # Méthode 1: via l'interface web
    print("   • Vérifie sur http://localhost:8080")
    print("   • Tu dois voir ton worker connecté !")
    
    # Méthode 2: via SparkContext
    executors = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().keys()
    executor_list = list(executors)
    print(f"   • Nombre d'exécuteurs trouvés: {len(executor_list)}")
    
    for i, executor in enumerate(executor_list):
        print(f"      - Exécuteur {i}: {executor}")
        
except Exception as e:
    print(f"   ⚠️ Méthode directe: {e}")
    print("   ✅ Utilise l'interface web pour vérifier")

# Test 2: Calcul distribué
print("\n⚡ TEST DE CALCUL DISTRIBUÉ")
print("-"*30)

# Créer un gros DataFrame
print("   Création d'un DataFrame de 10M lignes...")
debut = time.time()
df = spark.range(0, 10000000)
fin_creation = time.time()
print(f"   ✅ Créé en {fin_creation-debut:.2f} secondes")

# Compter
print("   Comptage en cours...")
debut_count = time.time()
count = df.count()
fin_count = time.time()
print(f"   ✅ {count:,} lignes comptées en {fin_count-debut_count:.2f} secondes")

# Test 3: Opération de groupBy
print("\n📊 TEST DE GROUPBY DISTRIBUÉ")
print("-"*30)

# Créer des données avec clés
print("   Création de données avec clés...")
df2 = spark.range(0, 1000000).selectExpr("id", "id % 5 as key")
print("   Calcul du groupBy...")
debut_group = time.time()
resultat = df2.groupBy("key").count().collect()
fin_group = time.time()
print(f"   ✅ GroupBy terminé en {fin_group-debut_group:.2f} secondes")

for row in resultat:
    print(f"      Clé {row['key']}: {row['count']} lignes")

print("\n" + "="*50)
print("🎉 TEST TERMINÉ AVEC SUCCÈS !")
print("="*50)
print("\n📌 VÉRIFICATION FINALE:")
print("   1. Ouvre http://localhost:8080 dans ton navigateur")
print("   2. Tu dois voir le worker connecté")
print("   3. Vérifie que l'application 'Test_Cluster_PFE' apparaît")

spark.stop()
print("\n✅ Spark arrêté")