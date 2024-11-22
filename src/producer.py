import pandas as pd
from kafka import KafkaProducer
import time
import json

# Fonction pour lire un fichier CSV et envoyer les données à Kafka
def produce_csv_to_kafka(file_path, topic, batch_size=10000, interval=10):
    # Initialisation du producteur Kafka avec des paramètres asynchrones et optimisés
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',               # Attente de la confirmation d'envoi pour chaque message
        linger_ms=5,              # Petit délai pour regrouper les messages
        batch_size=10000,         # Taille du lot pour regrouper les messages
    )

    # Lire le fichier CSV
    producer.flush() # Vide la queu

    data = pd.read_csv(file_path)
    results = []  # Liste pour collecter les données à stocker dans un DataFrame
    print(len(data))
    # Envoi par paquets de 'batch_size' lignes
    for i in range(0, len(data), batch_size):
        batch = data.iloc[i:i + batch_size].to_dict(orient='records')
        print(i)
        for record in batch:
            # Envoi asynchrone du message à Kafka
            producer.send(topic, value=record)
            results.append(record)  # Ajouter le record au tableau de résultats
            print(f"Ligne envoyée : {record}")
        # Pause entre les lots
        time.sleep(interval)

    # Convertir les résultats en DataFrame
    df = pd.DataFrame(results)
    
    # Une fois l'envoi terminé, assurez-vous de vider tous les messages dans le producteur
    producer.flush()
    producer.close()


    print(f"Envoi terminé. {len(results)} messages envoyés.")
    return df  # Retourne le DataFrame avec les données envoyées

# Utilisation de la fonction avec le fichier CSV et le topic Kafka
df = produce_csv_to_kafka("/home/lou/Data_Integration/TP_SPARK/alco-restuarant-violations.csv", 'csv_topic')
print(df.head())  # Affiche les premières lignes du DataFrame

