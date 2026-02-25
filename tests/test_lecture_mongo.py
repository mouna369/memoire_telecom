from pyspark.sql import SparkSession

# 1. On crée la session avec le connecteur MongoDB
spark = SparkSession.builder \
    .appName("TestLectureMongo") \
    .master("spark://10.255.255.254:7077") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
    .config("spark.mongodb.read.connection.uri", "mongodb://mongodb_pfe:27017/telecom_algerie.commentaires_bruts") \
    .getOrCreate()

try:
    # 2. On essaie de lire la collection
    print("⏳ Lecture de MongoDB en cours...")
    df = spark.read.format("mongodb").load()
    
    # 3. On affiche un aperçu
    df.show(5)
    print("✅ Succès ! Spark a lu les données de MongoDB.")
    
except Exception as e:
    print(f"❌ Erreur de lecture : {e}")

spark.stop()