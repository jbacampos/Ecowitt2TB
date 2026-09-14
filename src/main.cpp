#include <Arduino.h>
#include <ESP8266WiFi.h>
#include <WiFiClientSecure.h>
#include <ESP8266HTTPClient.h>
#include <ArduinoJson.h>
#include <PubSubClient.h>

#include "config.h"


WiFiClientSecure secureClient;
PubSubClient mqtt(secureClient);

unsigned long lastUpload = 0;


// ==================================================
// Wi-Fi
// ==================================================

void connectWiFi()
{
    if (WiFi.status() == WL_CONNECTED)
        return;

    Serial.print("Conectando ao Wi-Fi");

    WiFi.mode(WIFI_STA);
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

    while (WiFi.status() != WL_CONNECTED)
    {
        delay(500);
        Serial.print(".");
    }

    Serial.println();
    Serial.print("Wi-Fi conectado. IP: ");
    Serial.println(WiFi.localIP());
}


// ==================================================
// MQTT
// ==================================================

bool connectMQTT()
{
    if (mqtt.connected())
        return true;

    Serial.print("Conectando ao ThingsBoard MQTT... ");

    if (mqtt.connect(TB_CLIENT_ID, TB_USERNAME, TB_PASSWORD))
    {
        Serial.println("OK");
        return true;
    }

    Serial.print("FALHA, estado=");
    Serial.println(mqtt.state());

    return false;
}


// ==================================================
// Conversão de valor
// ==================================================

float valueFromString(const char *text)
{
    if (!text)
        return NAN;

    String s = text;

    s.trim();

    if (s.endsWith("%"))
        s.remove(s.length() - 1);

    return s.toFloat();
}


// ==================================================
// Procura ID em common_list
// ==================================================

float getCommonValue(JsonArray commonList, const char *wantedId)
{
    for (JsonObject item : commonList)
    {
        const char *id = item["id"];

        if (id && strcmp(id, wantedId) == 0)
        {
            const char *val = item["val"];

            if (val)
                return valueFromString(val);
        }
    }

    return NAN;
}


// ==================================================
// Leitura do GW3000
// ==================================================

bool readEcowitt(
    float &temperatureOut,
    float &feelsLikeOut,
    float &humidityOut,
    float &windSpeed)
{
    WiFiClient client;
    HTTPClient http;

    String url = String("http://") +
                 ECOWITT_HOST +
                 ":" +
                 ECOWITT_PORT +
                 "/get_livedata_info?";

    Serial.print("GET ");
    Serial.println(url);

    if (!http.begin(client, url))
    {
        Serial.println("Erro iniciando HTTP");
        return false;
    }

    int httpCode = http.GET();

    if (httpCode != HTTP_CODE_OK)
    {
        Serial.print("HTTP erro: ");
        Serial.println(httpCode);

        http.end();
        return false;
    }

    String payload = http.getString();

    http.end();

    Serial.print("JSON recebido: ");
    Serial.println(payload);

    JsonDocument doc;

    DeserializationError error = deserializeJson(doc, payload);

    if (error)
    {
        Serial.print("Erro JSON: ");
        Serial.println(error.c_str());
        return false;
    }

    JsonArray commonList = doc["common_list"];

    if (commonList.isNull())
    {
        Serial.println("common_list nao encontrado");
        return false;
    }

    temperatureOut = getCommonValue(commonList, "0x02");
    humidityOut    = getCommonValue(commonList, "0x07");
    feelsLikeOut   = getCommonValue(commonList, "3");
    windSpeed      = getCommonValue(commonList, "0x0B");

    return true;
}


// ==================================================
// Publicação ThingsBoard
// ==================================================

bool publishTelemetry(
    float temperatureOut,
    float feelsLikeOut,
    float humidityOut,
    float windSpeed)
{
    JsonDocument doc;

    doc["temperature_out"] = temperatureOut;
    doc["feels_like_out"]  = feelsLikeOut;
    doc["humidity_out"]    = humidityOut;
    doc["wind_speed"]      = windSpeed;

    String payload;

    serializeJson(doc, payload);

    Serial.print("MQTT -> ");
    Serial.println(payload);

    if (!mqtt.publish(TB_TOPIC, payload.c_str()))
    {
        Serial.println("ERRO publicando MQTT");
        return false;
    }

    Serial.println("Telemetria publicada.");

    return true;
}


// ==================================================
// SETUP
// ==================================================

void setup()
{
    Serial.begin(115200);

    delay(1000);

    Serial.println();
    Serial.println("================================");
    Serial.println("        Ecowitt2TB V1");
    Serial.println("================================");

    connectWiFi();

    // Somente para a prova de conceito.
    // Posteriormente vamos validar o certificado TLS.
    secureClient.setInsecure();

    mqtt.setServer(TB_HOST, TB_PORT);
}


// ==================================================
// LOOP
// ==================================================

void loop()
{
    connectWiFi();

    if (!connectMQTT())
    {
        delay(5000);
        return;
    }

    mqtt.loop();

    if (millis() - lastUpload >= UPLOAD_INTERVAL)
    {
        lastUpload = millis();

        float temperatureOut;
        float feelsLikeOut;
        float humidityOut;
        float windSpeed;

        if (readEcowitt(
                temperatureOut,
                feelsLikeOut,
                humidityOut,
                windSpeed))
        {
            Serial.println();
            Serial.println("Dados recebidos:");

            Serial.print("Temperatura: ");
            Serial.println(temperatureOut);

            Serial.print("Feel Like: ");
            Serial.println(feelsLikeOut);

            Serial.print("Umidade: ");
            Serial.println(humidityOut);

            Serial.print("Vento: ");
            Serial.println(windSpeed);

            publishTelemetry(
                temperatureOut,
                feelsLikeOut,
                humidityOut,
                windSpeed);
        }
        else
        {
            Serial.println("Falha na leitura do GW3000.");
        }
    }

    delay(100);
}