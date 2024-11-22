import pandas as pd
from kafka import KafkaConsumer
import json
import time
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("TP_DATA_INTEGRATION") \
    .getOrCreate()

# Fonction de consommation en streaming de Kafka et de sauvegarde dans un DataFrame
def consume_and_save_to_csv(topic, output_file, bootstrap_servers='localhost:9092', batch_size=10000):
    # Créer un consommateur Kafka
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',  # Commencer à lire depuis le début si le consommateur est nouveau
        enable_auto_commit=True,  # Commit automatique des offsets
        group_id=None,  # Sans groupe de consommateurs
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),  # Désérialisation des messages en JSON
        fetch_max_wait_ms=500,  # Augmenter le délai d'attente pour collecter plus de messages
        max_poll_records=batch_size  # Lire jusqu'à `batch_size` messages à la fois
    )

    print(f"Consommateur connecté à Kafka sur {bootstrap_servers} pour le topic {topic}...")

    # Liste pour accumuler les messages reçus
    accumulated_data = []
    compteur = 1
    idle_time = 0  # Compteur de secondes d'inactivité

    try:
        while True:
            # Lire les messages de Kafka
            messages = consumer.poll(timeout_ms=1000)  # Timeout de 1 seconde

            # Vérifier s'il y a des messages
            if messages:
                idle_time = 0  # Réinitialiser le compteur d'inactivité

                for _, records in messages.items():
                    for message in records:
                        # Extraire le message
                        data = message.value
                        print(f"Message reçu : {data}")

                        # Ajouter le message aux données accumulées
                        accumulated_data.append(data)

                # Enregistrer les données en bloc toutes les `batch_size` lignes
                if len(accumulated_data) >= batch_size:
                    new_data = pd.DataFrame(accumulated_data)
                    nom_fichier = f'{output_file}_{compteur}.csv'
                    new_data.to_csv(nom_fichier, index=False)
                    compteur += 1
                    # Sauvegarder en mode append dans le fichier principal
                    new_data.to_csv(f'{output_file}.csv', mode='a', index=False,sep=',')
                    accumulated_data = []

            else:
                # Incrémenter le compteur d'inactivité
                idle_time += 1
                print(f"Inactivité pendant {idle_time} secondes...")

                # Vérifier si le consommateur est inactif depuis 15 secondes
                if idle_time >= 15:
                    new_data = pd.DataFrame(accumulated_data)
                    nom_fichier = f'{output_file}_{compteur}.csv'
                    new_data.to_csv(nom_fichier, index=False)
                    new_data.to_csv(f'{output_file}.csv', mode='a', index=False,sep=',')
                    consumer.close()  # Fermer le consommateur
                    print("Consommateur Kafka fermé.")

    except KeyboardInterrupt:
        print("Arrêt manuel du consommateur.")

# Utilisation de la fonction
consume_and_save_to_csv('csv_topic', 'data/output_data')
