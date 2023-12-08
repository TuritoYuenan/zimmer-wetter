// Include settings
#include "config.h"

// Include libraries
#include <string>
#include <DHT.h>
#include <WiFi.h>
#include <WiFiClient.h>
#include <BlynkSimpleEsp32.h>

// Initialise Blynk timer
BlynkTimer timer;

// Initialise DHT22 sensor
DHT dht(PIN_DHT, DHTTYPE);

// Initialise sensor humidity and temperature
float temperature, humidity;
bool useFahrenheit;

// Send temperature and humidity data to Blynk
void sendData() {
	int codeRedStatus = codeRed(temperature, useFahrenheit);
	String description = describeWeather(temperature, humidity, useFahrenheit);

	Blynk.beginGroup();
	Blynk.virtualWrite(PIN_TEMP, temperature);
	Blynk.virtualWrite(PIN_HMDT, humidity);
	Blynk.virtualWrite(PIN_CRED, codeRedStatus);
	Blynk.virtualWrite(PIN_DESC, description);
	Blynk.endGroup();
}

void setup() {
	// Debug console
	Serial.begin(9600);

	// Setup LED pins
	pinMode(PIN_LEDC, OUTPUT);
	pinMode(PIN_LEDF, OUTPUT);
	pinMode(PIN_UNIT, INPUT_PULLUP);

	// Authenticate Blynk
	Blynk.begin(BLYNK_AUTH_TOKEN, WIFI_SSID, WIFI_PASS);

	// Send data every 0.9 seconds
	timer.setInterval(900L, sendData);
}

void loop() {
	// Determine temperature unit
	useFahrenheit = digitalRead(PIN_UNIT) == 1 ? true : false;

	// Gather temperature and humidity values
	humidity = dht.readHumidity();
	temperature = dht.readTemperature(useFahrenheit);

	// Exit early to try again if any value failed to be read
	if (isnan(humidity) || isnan(temperature)) {
		Serial.println("# Unable to get value from DHT sensor!");
		return;
	}

	// Light up LED if Code Red
	int severity = codeRed(temperature, useFahrenheit) ? HIGH : LOW;
	digitalWrite(PIN_LEDC, severity);

	// Run Blynk and Blynk timer, then delay
	Blynk.run(); timer.run(); delay(100);
}

// Activate Code Red if temperature is over 50 Celsius
bool codeRed(float temperature, bool useFahrenheit) {
	if (temperature > 50 && !useFahrenheit) return true;
	if (temperature > 122 && useFahrenheit) return true;
	return false;
}

String categoriseByHumid(float humidity, String low, String avg, String high) {
	if (humidity < 20) return low;
	else if (humidity < 60) return avg;
	else return high;
}

// Describe the current weather condition
String describeWeather(float temperature, float humidity, bool useFahrenheit) {
	// Convert to Celsius and try again if using Fahrenheit
	if (useFahrenheit) {
		float tempC = (temperature - 32) * 5 / 9;
		return describeWeather(tempC, humidity, false);
	}

	// Conditional jargon, will tidy later
	if (temperature < -15) {
		return categoriseByHumid(humidity, "Crisp", "Freezing", "Icy");
	} else if (temperature < 20) {
		return categoriseByHumid(humidity, "Harsh", "Cold", "Snowy");
	} else if (temperature < 30) {
		return categoriseByHumid(humidity, "Droughty", "Temperate", "Humid");
	} else if (temperature < 50) {
		return categoriseByHumid(humidity, "Bone-dry",  "Hot", "Sultry");
	} else {
		return categoriseByHumid(humidity, "Desert-like", "Blistering", "Oven-like");
	}
}

BLYNK_CONNECTED() {
	Blynk.syncAll();
}
