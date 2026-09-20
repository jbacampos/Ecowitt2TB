#include <Arduino.h>
#include <ArduinoJson.h>
#include <ESP8266HTTPClient.h>
#include <ESP8266WiFi.h>
#include <WiFiClientSecure.h>

#define MQTT_MAX_PACKET_SIZE 1024
#include <PubSubClient.h>

#include "config.h"

// ============================================================
// Estrutura dos dados meteorológicos
// ============================================================

struct WeatherData
{
    float temperature_out;
    float feels_like_out;
    float humidity_out;
    float dew_point_out;
    float pressure_relative;

    float wind_speed;
    float wind_gust;
    float wind_direction;
    float wind_direction_10min;

    float solar_radiation;

    float temperature_in;
    float humidity_in;

    float rain_rate;
    float rain_event;
    float rain_hour;
    float rain_day;
    float rain_week;
    float rain_month;
    float rain_year;
    float rain_total;
};

// ============================================================
// Objetos
// ============================================================

WiFiClientSecure secureClient;
PubSubClient mqtt(secureClient);

// ============================================================
// Utilitário
// ============================================================

float getValue(JsonArray array, const char *id)
{
    for (JsonObject item : array)
    {
        const char *itemId = item["id"];

        if (itemId && strcmp(itemId, id) == 0)
        {
            const char *value = item["val"];

            if (value)
            {
                String s = value;

                // Remove unidade, se existir
                int space = s.indexOf(' ');
                if (space >= 0)
                    s = s.substring(0, space);

                return s.toFloat();
            }
        }
    }

    return NAN;
}

// ============================================================
// Conexão Wi-Fi
// ============================================================

void connectWiFi()
{
    Serial.print("Conectando ao Wi-Fi");

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

// ============================================================
// Conexão ThingsBoard MQTT
// ============================================================

void connectMQTT()
{
    while (!mqtt.connected())
    {
        Serial.print("Conectando ao ThingsBoard MQTT...");

        if (mqtt.connect(
                TB_CLIENT_ID,
                TB_USERNAME,
                TB_PASSWORD))
        {
            Serial.println(" OK");
        }
        else
        {
            Serial.print(" FALHOU, estado = ");
            Serial.println(mqtt.state());

            delay(5000);
        }
    }
}

// ============================================================
// Leitura do GW3000
// ============================================================

bool readEcowitt(WeatherData &data)
{
    WiFiClient httpClient;
    HTTPClient http;

    String url =
        String("http://") +
        ECOWITT_HOST +
        ":" +
        ECOWITT_PORT +
        "/get_livedata_info?";

    Serial.print("GET ");
    Serial.println(url);

    if (!http.begin(httpClient, url))
    {
        Serial.println("Falha ao iniciar HTTP.");
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

    DeserializationError error =
        deserializeJson(doc, payload);

    if (error)
    {
        Serial.print("Erro JSON: ");
        Serial.println(error.c_str());
        return false;
    }

    // --------------------------------------------------------
    // common_list
    // --------------------------------------------------------

    JsonArray common = doc["common_list"].as<JsonArray>();

    data.temperature_out =
        getValue(common, "0x02");

    data.feels_like_out =
        getValue(common, "3");

    data.humidity_out =
        getValue(common, "0x07");

    data.dew_point_out =
        getValue(common, "0x03");

    data.wind_speed =
        getValue(common, "0x0B");

    data.wind_gust =
        getValue(common, "0x0C");

    data.wind_direction =
        getValue(common, "0x0A");

    data.wind_direction_10min =
        getValue(common, "0x6D");

    data.solar_radiation =
        getValue(common, "0x15");

    // --------------------------------------------------------
    // wh25
    // --------------------------------------------------------

    JsonArray wh25 = doc["wh25"].as<JsonArray>();

    if (!wh25.isNull() && wh25.size() > 0)
    {
        JsonObject indoor = wh25[0];

        data.temperature_in =
            String((const char *)indoor["intemp"]).toFloat();

        data.humidity_in =
            String((const char *)indoor["inhumi"]).toFloat();

        data.pressure_relative =
            String((const char *)indoor["rel"]).toFloat();
    }
    else
    {
        data.temperature_in = NAN;
        data.humidity_in = NAN;
        data.pressure_relative = NAN;
    }

    // --------------------------------------------------------
    // piezoRain
    // --------------------------------------------------------

    JsonArray rain = doc["piezoRain"].as<JsonArray>();

    data.rain_event =
        getValue(rain, "0x0D");

    data.rain_rate =
        getValue(rain, "0x0E");

    // 0x7D = novo Rain Hour
    data.rain_hour =
        getValue(rain, "0x7D");

    data.rain_day =
        getValue(rain, "0x10");

    data.rain_week =
        getValue(rain, "0x11");

    data.rain_month =
        getValue(rain, "0x12");

    data.rain_year =
        getValue(rain, "0x13");

    // 0x14 = Rain Totals
    data.rain_total =
        getValue(rain, "0x14");

    return true;
}

// ============================================================
// Exibição dos dados
// ============================================================

void printWeatherData(const WeatherData &d)
{
    Serial.println("========== DADOS ==========");

    Serial.printf("Temperatura externa : %.2f\n",
                  d.temperature_out);

    Serial.printf("Feel Like           : %.2f\n",
                  d.feels_like_out);

    Serial.printf("Umidade externa     : %.2f\n",
                  d.humidity_out);

    Serial.printf("Ponto de orvalho    : %.2f\n",
                  d.dew_point_out);

    Serial.printf("Pressao relativa    : %.2f\n",
                  d.pressure_relative);

    Serial.printf("Vento               : %.2f\n",
                  d.wind_speed);

    Serial.printf("Rajada              : %.2f\n",
                  d.wind_gust);

    Serial.printf("Direcao             : %.2f\n",
                  d.wind_direction);

    Serial.printf("Direcao media 10 min : %.2f\n",
                  d.wind_direction_10min);

    Serial.printf("Radiacao solar      : %.2f\n",
                  d.solar_radiation);

    Serial.printf("Temperatura interna : %.2f\n",
                  d.temperature_in);

    Serial.printf("Umidade interna     : %.2f\n",
                  d.humidity_in);

    Serial.printf("Chuva evento        : %.2f\n",
                  d.rain_event);

    Serial.printf("Chuva taxa          : %.2f\n",
                  d.rain_rate);

    Serial.printf("Chuva hora          : %.2f\n",
                  d.rain_hour);

    Serial.printf("Chuva dia           : %.2f\n",
                  d.rain_day);

    Serial.printf("Chuva semana        : %.2f\n",
                  d.rain_week);

    Serial.printf("Chuva mes           : %.2f\n",
                  d.rain_month);

    Serial.printf("Chuva ano           : %.2f\n",
                  d.rain_year);

    Serial.printf("Chuva total         : %.2f\n",
                  d.rain_total);

    Serial.println("===========================");
}

// ============================================================
// Publicação MQTT
// ============================================================

bool publishWeather(const WeatherData &d)
{
    JsonDocument doc;

    doc["temperature_out"] = d.temperature_out;
    doc["feels_like_out"] = d.feels_like_out;
    doc["humidity_out"] = d.humidity_out;
    doc["dew_point_out"] = d.dew_point_out;
    doc["pressure_relative"] = d.pressure_relative;

    doc["wind_speed"] = d.wind_speed;
    doc["wind_gust"] = d.wind_gust;
    doc["wind_direction"] = d.wind_direction;
    doc["wind_direction_10min"] = d.wind_direction_10min;

    doc["solar_radiation"] = d.solar_radiation;

    doc["temperature_in"] = d.temperature_in;
    doc["humidity_in"] = d.humidity_in;

    doc["rain_rate"] = d.rain_rate;
    doc["rain_event"] = d.rain_event;
    doc["rain_hour"] = d.rain_hour;
    doc["rain_day"] = d.rain_day;
    doc["rain_week"] = d.rain_week;
    doc["rain_month"] = d.rain_month;
    doc["rain_year"] = d.rain_year;
    doc["rain_total"] = d.rain_total;

    char payload[1024];

    size_t len =
        serializeJson(doc, payload, sizeof(payload));

    Serial.print("MQTT -> ");
    Serial.println(payload);

    bool result =
        mqtt.publish(
            TB_TOPIC,
            payload,
            len);

    if (result)
        Serial.println("Telemetria publicada.");
    else
    {
        Serial.print("Falha ao publicar. MQTT state = ");
        Serial.println(mqtt.state());
    }

    return result;
}

// ============================================================
// SETUP
// ============================================================

void setup()
{
    Serial.begin(115200);
    delay(1000);

    Serial.println();
    Serial.println("================================");
    Serial.println("        Ecowitt2TB V2");
    Serial.println("================================");

    // --------------------------------------------------------
    // Wi-Fi
    // --------------------------------------------------------

    connectWiFi();

    // --------------------------------------------------------
    // MQTT
    // --------------------------------------------------------

    secureClient.setInsecure();

    mqtt.setServer(TB_HOST, TB_PORT);

    // Payload V2 ultrapassa o buffer padrão do PubSubClient
    mqtt.setBufferSize(1024);

    connectMQTT();
}

// ============================================================
// LOOP
// ============================================================

void loop()
{
    if (WiFi.status() != WL_CONNECTED)
    {
        connectWiFi();
    }

    if (!mqtt.connected())
    {
        connectMQTT();
    }

    mqtt.loop();

    static unsigned long lastUpload = 0;

    if (millis() - lastUpload >= UPLOAD_INTERVAL ||
        lastUpload == 0)
    {
        lastUpload = millis();

        WeatherData data;

        if (readEcowitt(data))
        {
            printWeatherData(data);

            publishWeather(data);
        }
        else
        {
            Serial.println("Falha na leitura do GW3000.");
        }
    }

    delay(100);
}