/**
 * @file Minh Triet's Weather Station - Code for ESP8266 module
 * @version 0.7.0
 * @author Nguyen Ta Minh Triet <turitoyuenan@proton.me>
 * @ref https://wiki.dfrobot.com/APRS_Weather_Station_Sensor_Kit_SEN0186
 * @ref https://lastminuteengineers.com/bme280-esp8266-weather-station
 * @ref https://pijaeducation.com/serial-print-and-printf-solved
*/

#include <BlockNot.h>
#include <ESP8266HTTPClient.h>
#include <ESP8266WebServer.h>

#include "secrets.h"
#include "wetter-lib.h"

/// @brief Raw data received from the weather station
char stationData[35];

/// @brief Structured weather station data
WeatherData weatherData;

/// @brief Server for web dashboard
ESP8266WebServer server(80);

/// @brief Timer for data getting routine
BlockNot dataTimer(1000);

/// @brief Timer for data printing routine
BlockNot logTimer(3000);

/// @brief Tasks to do once at startup
void setup()
{
	Serial.begin(9600);

	// Connect to WiFi
	connectWiFi(WIFI_NAME, WIFI_PASS);

	// Setup web server
	server.on("/", []() {server.send(200, "text/html", hydrateHTML(htmlTemplate, weatherData));});
	server.onNotFound([]() {server.send(404, "text/plain", "there's only 1 page bro");});
	server.begin();

	Serial.println("Started web server!");
}

/// @brief Tasks to routinely do
void loop()
{
	if (dataTimer.TRIGGERED) {
		// Get data from weather station. Redo if format is wrong
		Serial.readBytes(stationData, sizeof(stationData));
		if (stationData[0] != 'c') { return; }

		// Convert and store weather station data
		weatherData = storeData(stationData);
	}

	if (logTimer.TRIGGERED) {
		// Print out data. Might be removed in favor of web server
		printData(weatherData);

	}

	// Serve webpage if requested
	server.handleClient();
}

/// @brief Connect to WiFi
/// @param ssid WiFi name / SSID
/// @param password WiFi password
void connectWiFi(const char* ssid, const char* password)
{
	Serial.printf("\nWiFi is %s. Connecting.", ssid);
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

/// @brief Print out weather station data
/// @param data Structured weather station data
void printData(WeatherData data)
{
	Serial.println("");
	Serial.printf("Temperature: %f deg C\n", data.temperature);
	Serial.printf("Humidity: %d percents\n", data.humidity);
	Serial.printf("Pressure: %d Pa\n", data.pressure);
	Serial.printf("Rainfall: (1h) %f mm, (1d) %f mm\n", data.rainfallH, data.rainfallD);
	Serial.printf("Wind Speed: avg %f m/s, max %f m/s\n", data.windSpeedAvg, data.windSpeedMax);
	Serial.printf("Wind Direction: %d deg\n", data.windDirection);
}

/// @brief Fill weather station data into a HTML file for web server
/// @param html A HTML template with placeholders to fill in data
/// @param data Structured weather station data
/// @return A HTML webpage with the numbers plugged in
String hydrateHTML(String html, WeatherData data)
{
	html.replace("{PRSR}", String(data.pressure));
	html.replace("{HMDT}", String(data.humidity));
	html.replace("{TEMP}", String(data.temperature));
	html.replace("{RNFH}", String(data.rainfallH));
	html.replace("{RNFD}", String(data.rainfallD));
	html.replace("{WSAG}", String(data.windSpeedAvg));
	html.replace("{WSMX}", String(data.windSpeedMax));
	html.replace("{WDRT}", String(data.windDirection));
	return html;
}
