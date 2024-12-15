import sys
import json
from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassificationModel

# Patch kafka-python pour Python 3.12 et versions supérieures
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaProducer

# 1. Configurer SparkSession
spark = SparkSession.builder \
    .appName("KafkaProducerWithModel") \
    .getOrCreate()

# 2. Charger le modèle ML sauvegardé
model_save_dir = 'modele_sauvegarder'
rf_model_path = f"{model_save_dir}/rf_model_classification"
rf_model = RandomForestClassificationModel.load(rf_model_path)
print(f"Modèle chargé depuis {rf_model_path}")

# 3. Charger les données de test (simulées ici)
test_data = spark.read.csv("data/test_data.csv", header=True, inferSchema=True)

# 4. Faire des prédictions sur les données de test
predictions_classifier = rf_model.transform(test_data)
predictions_classifier.show()

# 5. Configurer le Producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# 6. Envoyer les prédictions au topic Kafka
for row in predictions_classifier.collect():
    message = {
        'severity': row['severity'],
        'severity_index': row['severity_index'],
        'prediction': row['prediction'],
        'probability': row['probability'].toArray().tolist()  # Convertir le vecteur en liste
    }
    producer.send('severity-predictions', value=message)

# 7. Fermer le Producteur
producer.flush()
producer.close()
print("Les prédictions ont été envoyées au topic Kafka.")
