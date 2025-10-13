import json, logging
from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.write.point import Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException
from paho.mqtt.client import Client, MQTTMessage
from paho.mqtt.enums import CallbackAPIVersion


# MARK: Constants
DEBUG_MODE = True
CLIENT_ID = 'MQTT-to-InfluxDB ETL Pipeline'
BROKER_HOST = 'broker' # name of Mosquitto service in compose.yml
BROKER_PORT = 1883
USERNAME = 'swinuser'
PASSWORD = 'swinpass'
TOPIC = 'swinburne/hcmc_a35'


def extract(message: MQTTMessage) -> dict:
	"""Extract data from MQTT broker
	"""
	payload = message.payload.decode('utf-8')
	return json.loads(payload)


def transform(data: dict) -> Point:
	"""Transform data to InfluxDB point
	"""
	return (
		Point('weather').tag('city', 'HCMC').tag('campus', 'A35')
		.field('humidity', data['humidity'])
		.field('pressure', data['pressure'])
		.field('temperature', data['temperature'])
		.field('rainfall_day', data['rainfall_day'])
		.field('rainfall_hour', data['rainfall_hour'])
		.field('wind_speed_avg', data['wind_speed_avg'])
		.field('wind_speed_max', data['wind_speed_max'])
		.field('wind_direction', data['wind_direction'])
	)


def load(data: Point, database: InfluxDBClient):
	"""Load data to InfluxDB
	"""
	try:
		write_api = database.write_api(SYNCHRONOUS)
		write_api.write('weather_data', record = data)
	except ApiException:
		logging.error('3. Error: Connection to InfluxDB is unauthorised')
	except:
		logging.error('3. Error: Something wrong happened')


# MARK: Connection callback
def on_connect(client: Client, userdata, flags, reason_code, properties):
	logging.info('Connected to broker')

	client.subscribe(TOPIC)
	logging.info('Subscribed to topic: %s', TOPIC)


# MARK: Message callback
def on_message(client: Client, userdata: InfluxDBClient, message: MQTTMessage):
	if DEBUG_MODE: logging.info('1. Extracting message')
	extracted = extract(message)

	if DEBUG_MODE: logging.info('2. Transforming to InfluxDB point')
	transformed = transform(extracted)

	if DEBUG_MODE: logging.info('3. Loading to InfluxDB')
	load(transformed, userdata)


# MARK: Main procedure
def main():
	"""Main procedure
	"""
	logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
	database_client = InfluxDBClient.from_env_properties()

	logging.info("Entering credentials")
	mqtt_client = Client(CallbackAPIVersion.VERSION2, CLIENT_ID)
	mqtt_client.user_data_set(database_client)
	# mqtt_client.username_pw_set(USERNAME, PASSWORD)

	logging.info("Setting up callback functionality")
	mqtt_client.on_connect = on_connect
	mqtt_client.on_message = on_message

	logging.info("Connecting to broker")
	mqtt_client.connect(BROKER_HOST, BROKER_PORT)

	logging.info("Connected! Start processing incoming data")
	mqtt_client.loop_forever()

	return 0


# MARK: Main execution
if __name__ == '__main__': main()
