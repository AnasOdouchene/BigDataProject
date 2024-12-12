from kafka import KafkaConsumer

# Configuration du consommateur Kafka
consumer = KafkaConsumer(
    'global-fire-data',  # Nom du topic
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Lire les messages depuis le début si aucun offset n'est trouvé
    group_id='consumer-group-1',  # Identifiant du groupe de consommateurs
    enable_auto_commit=True  # Valider automatiquement l'offset
)

print("Consommateur en attente des messages...")

# Lire les messages depuis le topic
try:
    for message in consumer:
        # Décoder et afficher le message
        print(f"Message reçu : {message.value.decode('utf-8')}")
except KeyboardInterrupt:
    print("Consommateur arrêté.")
finally:
    # Fermer le consommateur proprement
    consumer.close()
