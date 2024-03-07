/**
 * @file Minh Triet's Weather Station - Code for ESP8266 module
 * @version 0.7.0
 * @author Nguyen Ta Minh Triet <turitoyuenan@proton.me>
 * @ref https://wiki.dfrobot.com/APRS_Weather_Station_Sensor_Kit_SEN0186
 * @ref https://lastminuteengineers.com/bme280-esp8266-weather-station
 * @ref https://pijaeducation.com/serial-print-and-printf-solved
*/

// Debug mode
#define IS_DEBUGGING false

// Library for non-blocking code
#include <BlockNot.h>

// Libraries for WiFi connection
#include <WiFiClientSecureBearSSL.h>
#include <ESP8266HTTPClient.h>
#include <ESP8266WiFi.h>

// Internal libraries
#include "secrets.h"
#include "wetter-lib.h"
#include "certificate.h"

/// @brief Raw data received from the weather station
char stationData[35];

/// @brief Structured weather station data
WeatherData weatherData;

/// @brief Timer for database sending routine
BlockNot sendTimer(10000);

/// @brief Timer for data printing routine
BlockNot logTimer(2000);

/// @brief Certificate for HTTPS requests
X509List cert(cert_ISRG_Root_X1);

/// @brief Tasks to do once at startup
void setup()
{
	Serial.begin(9600);
	Serial.setDebugOutput(IS_DEBUGGING);

	// Connect to WiFi
	connectWiFi(WIFI_NAME, WIFI_PASS);

	// Do time syncing stuffs
	handleTime();
}

/// @brief Tasks to routinely do
void loop()
{
	// Get data from weather station. Redo if format is wrong
	Serial.readBytes(stationData, sizeof(stationData));
	if (stationData[0] != 'c') { return; }

	// Store weather station data
	weatherData = storeData(stationData);
	// weatherData = generateData();
	String JSON = createJSON(weatherData);

	// Print out data.
	if (logTimer.triggered()) {
		Serial.println(JSON);
	}

	// Send data to Astra DB
	if (sendTimer.triggered()) {
		sendData(JSON);
	}
}

/// @brief Connect to WiFi
/// @param ssid WiFi name / SSID
/// @param password WiFi password
void connectWiFi(const char* ssid, const char* password)
{
	Serial.printf("\nWiFi is %s. Connecting.", ssid);
	WiFi.hostname(F("ZimmerWetter"));
	WiFi.begin(ssid, password);

	while (WiFi.status() != WL_CONNECTED) {
		delay(1000);
		Serial.print(".");
	}

	if (WiFi.status() == WL_CONNECTED) {
		Serial.print("\nConnected! View your weather at: ");
		Serial.println(WiFi.localIP());
	}
}

/// @brief Sync with with NTP servers to get current time
void handleTime()
{
	configTime(7 * 3600, 0, "asia.pool.ntp.org", "pool.ntp.org", "time.nist.gov");

	Serial.print("Waiting for NTP time sync");
	time_t now = time(nullptr);
	while (now < 8 * 3600 * 2) {
		delay(500);
		Serial.print(".");
		now = time(nullptr);
	}
	Serial.println("");
	struct tm timeinfo;
	gmtime_r(&now, &timeinfo);
	Serial.print("Current time: ");
	Serial.print(asctime(&timeinfo));
}

/// @brief Convert characters to integers
/// @param buffer Raw weather station data
/// @param start First character to include
/// @param stop Last character to include
/// @return An integer, negative if there's a minus sign
int charToInt(char* buffer, int start, int stop)
{
	int result = 0;
	int temp[stop - start + 1];

	// Negative number (contains a minus sign)
	if (buffer[start] == '-') {
		for (int i = start + 1; i <= stop; i++) {
			temp[i - start] = buffer[i] - '0';
			result = 10 * result + temp[i - start];
		}
		return 0 - result;
	}

	// Non-negative number
	for (int i = start; i <= stop; i++) {
		temp[i - start] = buffer[i] - '0';
		result = 10 * result + temp[i - start];
	}
	return result;
}

/// @brief Convert raw data from the weather station to structured data
/// @param buffer Raw weather station data
/// @return data Structured weather station data
WeatherData storeData(char* buffer)
{
	WeatherData data;

	data.windDirection = charToInt(buffer, 1, 3);
	data.windSpeedAvg = charToInt(buffer, 5, 7) * 0.44704;
	data.windSpeedMax = charToInt(buffer, 9, 11) * 0.44704;
	data.temperature = (charToInt(buffer, 13, 15) - 32.00) * 5.00 / 9.00;
	data.rainfallH = charToInt(buffer, 17, 19) * 25.40 * 0.01;
	data.rainfallD = charToInt(buffer, 21, 23) * 25.40 * 0.01;
	data.humidity = charToInt(buffer, 25, 26);
	data.pressure = charToInt(buffer, 28, 32) * 10.00;

	return data;
}

/// @brief Randomly generate fake weather data for testing
/// @return Generated structured weather station data
WeatherData generateData()
{
	WeatherData data;

	data.windDirection = random(360);
	data.windSpeedAvg = random(100);
	data.windSpeedMax = random(100);
	data.temperature = random(-30, 70);
	data.rainfallH = random(100);
	data.rainfallD = random(100);
	data.humidity = random(100);
	data.pressure = random(100000, 105000);

	return data;
}

/// @brief Send weather station data to an Astra database
/// @param data Structured weather station data
void sendData(String json)
{
	std::unique_ptr<BearSSL::WiFiClientSecure> client(new BearSSL::WiFiClientSecure);
	HTTPClient https;

	// Provide certificate and connect to endpoint
	client->setTrustAnchors(&cert);
	client->connect(XATA_host, XATA_port);

	// Create a HTTP request with headers
	https.begin(*client, XATA_host, XATA_port, DB_ENDPOINT, true);
	https.addHeader("Content-Type", "application/json");
	https.addHeader("Authorization", "Bearer " API_KEY);

	// Send request to POST weather data
	int responseCode = https.POST(json);

	// Handle response
	if (responseCode > 0) {
		Serial.print("Request sent! Code: ");
		Serial.println(responseCode);

		if (IS_DEBUGGING) {
			String payload = https.getString();
			Serial.println(payload);
		}
	} else {
		Serial.print("Request failed! Code: ");
		Serial.println(responseCode);
	}

	// Free resources
	https.end();
}

/// @brief Create a JSON document from weather station data
/// @param data Structured weather station data
/// @return Weather station data formatted in a JSON document
String createJSON(WeatherData data)
{
	String JSONTemplate = R"({"humidity": {HMDT}, "pressure": {PRSR}, "temperature": {TEMP}, "rainfall_D": {RNFD}, "rainfall_H": {RNFH}, "windSpeedAvg": {WSAG}, "windSpeedMax": {WSMX}, "windDirection": {WDRT}})";

	JSONTemplate.replace("{PRSR}", String(data.pressure));
	JSONTemplate.replace("{HMDT}", String(data.humidity));
	JSONTemplate.replace("{TEMP}", String(data.temperature));
	JSONTemplate.replace("{RNFH}", String(data.rainfallH));
	JSONTemplate.replace("{RNFD}", String(data.rainfallD));
	JSONTemplate.replace("{WSAG}", String(data.windSpeedAvg));
	JSONTemplate.replace("{WSMX}", String(data.windSpeedMax));
	JSONTemplate.replace("{WDRT}", String(data.windDirection));
	return JSONTemplate;
}
