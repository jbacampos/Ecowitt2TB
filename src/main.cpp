#include <Arduino.h>
#include <ArduinoJson.h>
#include <ESP8266HTTPClient.h>
#include <ESP8266WiFi.h>
#include <LittleFS.h>
#include <WiFiClientSecure.h>
#include <time.h>

#define MQTT_MAX_PACKET_SIZE 1024
#include <PubSubClient.h>

#include "config.h"

// ============================================================
// Estrutura dos dados meteorológicos
// ============================================================

struct WeatherData
{
    uint64_t collectedAt;
    bool timestampFromGateway;

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

const char *PENDING_TELEMETRY_FILE = "/pending-telemetry.jsonl";
const char *PENDING_TELEMETRY_TEMP_FILE = "/pending-telemetry.tmp";
const char *LEGACY_TELEMETRY_FILE = "/pending-telemetry-legacy.jsonl";

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

String getTextValue(JsonArray array, const char *id)
{
    for (JsonObject item : array)
    {
        const char *itemId = item["id"];
        const char *value = item["val"];

        if (itemId && value && strcmp(itemId, id) == 0)
            return String(value);
    }

    return "";
}

bool isClockSynchronized()
{
    // Datas posteriores a 2024 indicam que o NTP já ajustou o relógio.
    return time(nullptr) >= 1704067200;
}

uint64_t currentTimestampMs()
{
    return static_cast<uint64_t>(time(nullptr)) * 1000ULL +
           (millis() % 1000UL);
}

String formatTimestamp(uint64_t timestampMs)
{
    time_t timestamp = static_cast<time_t>(timestampMs / 1000ULL);
    tm timeInfo;
    localtime_r(&timestamp, &timeInfo);

    char timeText[9];
    strftime(timeText, sizeof(timeText), "%H:%M:%S", &timeInfo);

    return String(timeText);
}

uint64_t parseGatewayTimestamp(const String &timestampText)
{
    int year, month, day, hour, minute, second;
    int valuesRead = sscanf(timestampText.c_str(),
                            "%d%*[-/]%d%*[-/]%d %d:%d:%d",
                            &year, &month, &day,
                            &hour, &minute, &second);

    if (valuesRead != 6 || year < 2024 || month < 1 || month > 12 ||
        day < 1 || day > 31 || hour > 23 || minute > 59 || second > 59)
        return 0;

    tm timeInfo = {};
    timeInfo.tm_year = year - 1900;
    timeInfo.tm_mon = month - 1;
    timeInfo.tm_mday = day;
    timeInfo.tm_hour = hour;
    timeInfo.tm_min = minute;
    timeInfo.tm_sec = second;
    timeInfo.tm_isdst = -1;

    time_t timestamp = mktime(&timeInfo);
    return timestamp > 0 ? static_cast<uint64_t>(timestamp) * 1000ULL : 0;
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
    static unsigned long lastAttempt = 0;

    if (mqtt.connected())
        return;

    if (lastAttempt != 0 &&
        millis() - lastAttempt < MQTT_RETRY_INTERVAL)
        return;

    lastAttempt = millis();

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

    // O item 0x18 contém a data e hora do snapshot do GW3000.
    String gatewayTime = getTextValue(common, "0x18");
    data.collectedAt = parseGatewayTimestamp(gatewayTime);
    data.timestampFromGateway = data.collectedAt != 0;

    if (!data.timestampFromGateway && isClockSynchronized())
    {
        data.collectedAt = currentTimestampMs();
        Serial.println("Timestamp do GW3000 indisponível; usando NTP do ESP.");
    }

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
// Publicação MQTT e fila LittleFS
// ============================================================

void makeTelemetryPayload(const WeatherData &d,
                          uint64_t collectedAt,
                          String &payload)
{
    JsonDocument doc;
    JsonObject values = doc["values"].to<JsonObject>();

    doc["ts"] = collectedAt;

    values["temperature_out"] = d.temperature_out;
    values["feels_like_out"] = d.feels_like_out;
    values["humidity_out"] = d.humidity_out;
    values["dew_point_out"] = d.dew_point_out;
    values["pressure_relative"] = d.pressure_relative;

    values["wind_speed"] = d.wind_speed;
    values["wind_gust"] = d.wind_gust;
    values["wind_direction"] = d.wind_direction;
    values["wind_direction_10min"] = d.wind_direction_10min;

    values["solar_radiation"] = d.solar_radiation;

    values["temperature_in"] = d.temperature_in;
    values["humidity_in"] = d.humidity_in;

    values["rain_rate"] = d.rain_rate;
    values["rain_event"] = d.rain_event;
    values["rain_hour"] = d.rain_hour;
    values["rain_day"] = d.rain_day;
    values["rain_week"] = d.rain_week;
    values["rain_month"] = d.rain_month;
    values["rain_year"] = d.rain_year;
    values["rain_total"] = d.rain_total;

    payload = "";
    serializeJson(doc, payload);
}

bool publishPayload(const String &payload, uint64_t collectedAt)
{
    if (!mqtt.connected())
        return false;

    Serial.print("MQTT -> ");
    Serial.println(payload);

    bool result = mqtt.publish(
        TB_TOPIC,
        payload.c_str(),
        payload.length());

    if (result)
    {
        Serial.print("Telemetria coletada às ");
        Serial.print(formatTimestamp(collectedAt));
        Serial.print(" publicada às ");
        Serial.println(formatTimestamp(currentTimestampMs()));
    }
    else
    {
        Serial.print("Falha ao publicar. MQTT state = ");
        Serial.println(mqtt.state());
    }

    return result;
}

bool savePendingPayload(const String &payload)
{
    File file = LittleFS.open(PENDING_TELEMETRY_FILE, "a");

    if (!file)
    {
        Serial.println("Falha ao abrir a fila LittleFS.");
        return false;
    }

    bool saved = file.println(payload) > 0;
    file.close();

    if (saved)
        Serial.println("Telemetria guardada na fila LittleFS.");
    else
        Serial.println("Falha ao gravar a fila LittleFS.");

    return saved;
}

void publishPendingTelemetry()
{
    if (!mqtt.connected() || !LittleFS.exists(PENDING_TELEMETRY_FILE))
        return;

    File pending = LittleFS.open(PENDING_TELEMETRY_FILE, "r");
    File remaining = LittleFS.open(PENDING_TELEMETRY_TEMP_FILE, "w");

    if (!pending || !remaining)
    {
        Serial.println("Falha ao abrir a fila LittleFS para envio.");
        if (pending)
            pending.close();
        if (remaining)
            remaining.close();
        return;
    }

    bool keepRemaining = false;
    unsigned int published = 0;

    while (pending.available())
    {
        String record = pending.readStringUntil('\n');
        record.trim();

        if (record.length() == 0)
            continue;

        JsonDocument doc;
        DeserializationError error = deserializeJson(doc, record);

        if (error || !doc["ts"].is<uint64_t>() || !doc["values"].is<JsonObject>())
        {
            File legacy = LittleFS.open(LEGACY_TELEMETRY_FILE, "a");
            if (legacy)
            {
                legacy.println(record);
                legacy.close();
                Serial.println("Registro antigo movido para a fila legada sem timestamp.");
            }
            else
            {
                Serial.println("Falha ao separar registro pendente sem timestamp.");
                keepRemaining = true;
                remaining.println(record);
            }
            continue;
        }

        uint64_t collectedAt = doc["ts"].as<uint64_t>();

        if (!keepRemaining && publishPayload(record, collectedAt))
        {
            published++;
        }
        else
        {
            keepRemaining = true;
            remaining.println(record);
        }
    }

    pending.close();
    remaining.close();

    LittleFS.remove(PENDING_TELEMETRY_FILE);

    if (keepRemaining)
        LittleFS.rename(PENDING_TELEMETRY_TEMP_FILE, PENDING_TELEMETRY_FILE);
    else
        LittleFS.remove(PENDING_TELEMETRY_TEMP_FILE);

    if (published > 0)
    {
        Serial.print("Telemetrias recuperadas da fila: ");
        Serial.println(published);
    }
}

bool publishWeather(const WeatherData &d)
{
    if (d.collectedAt == 0)
    {
        Serial.println("Sem timestamp do GW3000 ou NTP; telemetria não será enviada.");
        return false;
    }

    String payload;
    makeTelemetryPayload(d, d.collectedAt, payload);

    if (publishPayload(payload, d.collectedAt))
        return true;

    return savePendingPayload(payload);
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

    if (!LittleFS.begin())
    {
        Serial.println("Falha ao montar LittleFS.");
    }

    // --------------------------------------------------------
    // Wi-Fi
    // --------------------------------------------------------

    connectWiFi();

    configTime(UTC_OFFSET_SECONDS, 0,
               NTP_SERVER_1, NTP_SERVER_2, NTP_SERVER_3);
    Serial.println("Sincronização NTP iniciada.");

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

    static bool clockReported = false;
    if (isClockSynchronized() && !clockReported)
    {
        clockReported = true;
        Serial.print("Relógio NTP sincronizado: ");
        Serial.println(formatTimestamp(currentTimestampMs()));
    }

    // Envia primeiro as amostras antigas, preservando a ordem da fila.
    publishPendingTelemetry();

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
