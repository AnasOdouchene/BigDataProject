import csv
from kafka import KafkaProducer
import time

# Configuration du producteur Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Temps limite en secondes (par exemple, 30 secondes)
temps_limite = 100
debut = time.time()  # Enregistrer l'heure de début

# Lire le fichier CSV
with open('../../data/Transformed_GlobalFireBurnedArea_pandas.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Vérifier si le temps limite est dépassé
        if time.time() - debut > temps_limite:
            print("Temps limite atteint, arrêt de l'envoi.")
            break  # Sortir de la boucle

        # Convertir la ligne en format string
        message = str(row)
        
        # Envoyer le message au topic Kafka
        producer.send('global-fire-data', value=message.encode('utf-8'))
        print(f"Message envoyé : {message}")
        
        # Simuler un flux en temps réel
        time.sleep(6)
