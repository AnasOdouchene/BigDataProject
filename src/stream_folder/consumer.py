import sys
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from kafka import KafkaConsumer

# Patch kafka-python pour Python 3.12 et versions supérieures
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

# 1. Configurer SparkSession
spark = SparkSession.builder \
    .appName("KafkaConsumerWithModel") \
    .getOrCreate()

# 2. Définir le schéma des données reçues depuis Kafka
schema = StructType([
    StructField("severity", StringType(), True),
    StructField("severity_index", DoubleType(), True),
    StructField("prediction", DoubleType(), True),
    StructField("probability", StringType(), True)  # Reçu sous forme de chaîne JSON
])

# 3. Configurer le Consumer Kafka
consumer = KafkaConsumer(
    'severity-predictions',  # Topic Kafka
    bootstrap_servers='localhost:9092',  # Serveur Kafka
    auto_offset_reset='earliest',  # Lire depuis le début
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Désérialiser JSON
)

print("Lecture des messages depuis le topic 'severity-predictions'...")

# 4. Traiter les messages reçus depuis Kafka
for message in consumer:
    # Lire les données JSON depuis Kafka
    input_data = message.value
    print(f"Données reçues : {input_data}")

    # Convertir en DataFrame Spark
    input_df = spark.createDataFrame([input_data], schema=schema)

    # Afficher les prédictions reçues
    print("===== Prédictions reçues =====")
    input_df.show()
