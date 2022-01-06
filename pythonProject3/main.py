import random
import json
import pika
from paho.mqtt import client as mqtt_client
from gpiozero import PWMLED
from time import sleep
from types import SimpleNamespace


class Temp:
    def __init__(self, temp, hum, id):
        self.temp = temp
        self.hum = hum
        self.id = id


broker = 'mqtt.flespi.io'
port = 1883
weather = "easv/weather"
gas = "easv/gas"
light = "easv/light"

led = PWMLED(25)

routing_key = "WeatherEvent"
exchange = "weather_exchange"
queue = "easv/WeatherEvent"

routing_key_gas = "GasEvent"
exchange_gas = "gas_exchange"
queue_gas = "easv/GasEvent"


# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'
client_id_gas = f'python-mqtt-{random.randint(0, 100)}'
username = '30Zeg20AopbivFfYG0I7klFHoZDm8GS1uF3AvR1H60PruSf0IMVTVqq9fTlbn10F'
password = ''

params = pika.URLParameters('amqps://mhomznpp:jdDTdT3rqFFEsNtgL6XefLF8mlaTYIo8@goose.rmq2.cloudamqp.com/mhomznpp')
connection = pika.BlockingConnection(params)

channel = connection.channel()  # start a channel
channel.exchange_declare(exchange)
channel.queue_bind(queue, exchange, routing_key)
channel.queue_declare(queue=queue)

channelgas = connection.channel()  # start a gas channel
channelgas.exchange_declare(exchange_gas)
channelgas.queue_declare(queue=queue_gas)
channelgas.queue_bind(queue_gas, exchange_gas, routing_key_gas)



def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        datajson = msg.payload.decode()
        print(datajson)
        if "temp" in datajson:
            channel.basic_publish(
                body=datajson,
                exchange=exchange,
                routing_key=routing_key
                )
        if "gasN" in datajson:
            channel.basic_publish(
                body=datajson,
                exchange=exchange_gas,
                routing_key=routing_key_gas
            )
        if "light_level" in datajson:
            x = json.loads(datajson, object_hook=lambda d: SimpleNamespace(**d))
            print(x.light_level)
            if x.light_level > 5:
                led.value = 0
            if x.light_level < 5:
                led.value = 1



    client.subscribe(weather)
    client.subscribe(light)
    client.subscribe(gas)
    client.on_message = on_message



def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()
