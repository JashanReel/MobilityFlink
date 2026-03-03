from json import dumps
from kafka import KafkaProducer
import pandas as pd



kafka_nodes = "kafka:29092"
myTopic = "aisdata"

def gen_data():
  df = pd.read_csv('ais_instants.csv')

  prod = KafkaProducer(bootstrap_servers=kafka_nodes, value_serializer=lambda x:dumps(x).encode('utf-8'))

  for index, row in df.iterrows():
    my_data = {
    't': row['t'],
    'mmsi': row['mmsi'],
    'lon': row['longitude'],   # était row['lon']
    'lat': row['latitude'],    # était row['lat']
    'speed': row['sog'],       # était row['speed']
    'course': 0.0              # n'existe pas dans le CSV, valeur par défaut
}
    print(my_data)
    prod.send(topic=myTopic, value=my_data)

  prod.flush()

if __name__ == "__main__":
  gen_data()