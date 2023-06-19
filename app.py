import requests
import os
from datetime import datetime
import time
from confluent_kafka import Producer, KafkaException, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka import Consumer
import json

if __name__ == '__main__':

    # Read arguments and configurations
    config_file = "your-config-file.conf"
    topic = "mini_project"

    # Load Confluent Cloud configuration
    conf = {
        'bootstrap.servers': 'YOUR SERVERS DETAILS',
        # 'ssl.ca.location': '<ssl-ca-location>',
        'security.protocol' : 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': 'YOUR USER NAME' ,
        'sasl.password': 'YOUR PASSWORD',

        }

    # Create Producer instance
    producer = Producer(conf)

    delivered_records = 0

    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))


    # basic info for openmapweather.org
    user_api = "YOUR OPEN_WEATHER_MAP API KEY "
    location = "SELECT A LOCATION"

    while True:
        complete_api_link = f"https://api.openweathermap.org/data/2.5/weather?appid={user_api}&q={location}&units=metric"
        api_link = requests.get(complete_api_link)
        api_data = api_link.json()
        # print(api_data)
        delivered_records = delivered_records + 1
        humidity = api_data["main"]["humidity"]
        pressure = api_data["main"]["pressure"]
        temperature = api_data["main"]["temp"]
        main_description = api_data['weather'][0]["main"]
        wind_speed = api_data["wind"]["speed"]
        temp_min = api_data["main"]["temp_min"]
        temp_max = api_data["main"]["temp_max"]
        ground_lvl = api_data["main"]["grnd_level"]
        time_stamp = time.time()
        # info = {humidity, pressure, temperature, time}
        Schema = {'humidity': humidity,
                  'pressure': pressure,
                  'temperature': temperature,
                  'description': main_description,
                  'wind_speed': wind_speed,
                  'temp_min': temp_min,
                  'temp_max': temp_max,
                  'ground_lvl': ground_lvl,
                  'date_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                  'day': datetime.now().strftime("%d"),
                  'hour': datetime.now().strftime("%H"),
                  'minute': datetime.now().strftime("%M")}
        record_value = json.dumps(Schema)
        producer.produce(topic, key=str(delivered_records), value=record_value, on_delivery=acked)

        print(record_value)

        print("{} messages were produced to topic {}!".format(delivered_records, topic))
        time.sleep(6*60)
        # break

 
