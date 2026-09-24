#include <Arduino.h>
#include <ArduinoJson.h>
#include <ESP8266HTTPClient.h>
#include <ESP8266WiFi.h>
#include <LittleFS.h>
#include <WiFiClientSecure.h>
#include <time.h>
#include <algorithm>
#include <vector>

#define MQTT_MAX_PACKET_SIZE 1024
#include <PubSubClient.h>

#include "config.h"
#include "telemetry_record.h"

// Opt-in LittleFS integration test. Build with -DECOWITT_QUEUE_FS_TEST=1,
// then send 't' on the serial monitor. Normal firmware builds keep this off.
#ifndef ECOWITT_QUEUE_FS_TEST
#define ECOWITT_QUEUE_FS_TEST 0
#endif

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
bool publishPayload(const String &payload, uint64_t collectedAt);

namespace {
const uint32_t SEGMENT_MAGIC = 0x53435154UL;
const uint32_t FOOTER_MAGIC = 0x46515453UL;
const uint8_t SEGMENT_VERSION = 1;
const size_t SEGMENT_HEADER_SIZE = 24;
const size_t SEGMENT_FOOTER_SIZE = 12;
const size_t SEGMENT_MAX_SIZE = SEGMENT_HEADER_SIZE +
    telemetry_queue::MAX_RECORDS_PER_SEGMENT * telemetry_queue::RECORD_SIZE +
    SEGMENT_FOOTER_SIZE;
const size_t REPLAY_MAX_RECORDS = 5;
const unsigned long REPLAY_BUDGET_MS = 20;
const size_t MAX_SEGMENTS = 128;
const size_t FILESYSTEM_SAFETY_MARGIN = 16 * 1024;
const uint32_t SEGMENT_SEQUENCE_RESERVATION = 16;
const uint32_t RECORD_SEQUENCE_RESERVATION = 64;
const uint32_t SEQUENCE_META_MAGIC = 0x4D515451UL;
const size_t SEQUENCE_META_SIZE = 20;
const char *SEQUENCE_META_FILES[2] = {"/q-seq-a.dat", "/q-seq-b.dat"};

struct SegmentInfo {
    uint32_t sequence;
    uint32_t firstRecordSequence;
    String path;
    uint16_t count;
    bool closed;
    bool valid;
    bool recoverableTail;
};

std::vector<SegmentInfo> segments;
uint32_t nextSegmentSequence = 1;
uint32_t nextRecordSequence = 1;
int replaySegment = 0;
uint16_t replayRecord = 0;
bool queueReady = false;
size_t legacyPosition = 0;
uint32_t highestSegmentSequence = 0;
uint32_t reservedSegmentLimit = 1;
uint32_t reservedRecordLimit = 1;
uint32_t sequenceMetaGeneration = 0;
int sequenceMetaSlot = -1;
}

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

namespace {

telemetry_queue::Values weatherValues(const WeatherData &d) {
    telemetry_queue::Values values = {{
        d.temperature_out, d.feels_like_out, d.humidity_out, d.dew_point_out,
        d.pressure_relative, d.wind_speed, d.wind_gust, d.wind_direction,
        d.wind_direction_10min, d.solar_radiation, d.temperature_in,
        d.humidity_in, d.rain_rate, d.rain_event, d.rain_hour, d.rain_day,
        d.rain_week, d.rain_month, d.rain_year, d.rain_total
    }};
    return values;
}

WeatherData valuesWeather(const telemetry_queue::Values &v, uint64_t timestamp) {
    WeatherData d{};
    d.collectedAt = timestamp;
    d.temperature_out = v.value[0]; d.feels_like_out = v.value[1];
    d.humidity_out = v.value[2]; d.dew_point_out = v.value[3];
    d.pressure_relative = v.value[4]; d.wind_speed = v.value[5];
    d.wind_gust = v.value[6]; d.wind_direction = v.value[7];
    d.wind_direction_10min = v.value[8]; d.solar_radiation = v.value[9];
    d.temperature_in = v.value[10]; d.humidity_in = v.value[11];
    d.rain_rate = v.value[12]; d.rain_event = v.value[13];
    d.rain_hour = v.value[14]; d.rain_day = v.value[15];
    d.rain_week = v.value[16]; d.rain_month = v.value[17];
    d.rain_year = v.value[18]; d.rain_total = v.value[19];
    return d;
}

bool writeExact(File &file, const uint8_t *data, size_t size) {
    return file.write(data, size) == size;
}

bool readExact(File &file, uint8_t *data, size_t size) {
    return file.read(data, size) == static_cast<int>(size);
}

String segmentPath(uint32_t sequence) {
    char name[24];
    snprintf(name, sizeof(name), "/q-%08lu.seg", static_cast<unsigned long>(sequence));
    return String(name);
}

bool readSequenceMeta(int slot, uint32_t &generation,
                      uint32_t &segmentLimit, uint32_t &recordLimit) {
    File file = LittleFS.open(SEQUENCE_META_FILES[slot], "r");
    uint8_t data[SEQUENCE_META_SIZE];
    bool valid = file && file.size() == sizeof(data) &&
        readExact(file, data, sizeof(data));
    if (file) file.close();
    if (!valid || telemetry_queue::get32(data) != SEQUENCE_META_MAGIC ||
        telemetry_queue::get32(data + 16) != telemetry_queue::crc32(data, 16))
        return false;
    generation = telemetry_queue::get32(data + 4);
    segmentLimit = telemetry_queue::get32(data + 8);
    recordLimit = telemetry_queue::get32(data + 12);
    return segmentLimit != 0 && recordLimit != 0;
}

bool writeSequenceMeta(uint32_t segmentLimit, uint32_t recordLimit) {
    const int target = sequenceMetaSlot == 0 ? 1 : 0;
    const uint32_t generation = sequenceMetaGeneration + 1;
    uint8_t data[SEQUENCE_META_SIZE] = {};
    telemetry_queue::put32(data, SEQUENCE_META_MAGIC);
    telemetry_queue::put32(data + 4, generation);
    telemetry_queue::put32(data + 8, segmentLimit);
    telemetry_queue::put32(data + 12, recordLimit);
    telemetry_queue::put32(data + 16, telemetry_queue::crc32(data, 16));
    File file = LittleFS.open(SEQUENCE_META_FILES[target], "w");
    bool ok = file && writeExact(file, data, sizeof(data));
    if (file) { file.flush(); file.close(); }
    uint32_t checkGeneration, checkSegments, checkRecords;
    bool verified = ok && readSequenceMeta(target, checkGeneration,
                                           checkSegments, checkRecords) &&
        checkGeneration == generation && checkSegments == segmentLimit &&
        checkRecords == recordLimit;
    if (!verified) return false;
    sequenceMetaSlot = target;
    sequenceMetaGeneration = generation;
    reservedSegmentLimit = segmentLimit;
    reservedRecordLimit = recordLimit;
    return true;
}

bool reserveSequenceRanges(uint32_t segmentFloor, uint32_t recordFloor) {
    const uint32_t segmentBase = max(reservedSegmentLimit,
        max(segmentFloor, nextSegmentSequence));
    const uint32_t recordBase = max(reservedRecordLimit,
        max(recordFloor, nextRecordSequence));
    if (segmentBase > 99999999UL - SEGMENT_SEQUENCE_RESERVATION ||
        recordBase > UINT32_MAX - RECORD_SEQUENCE_RESERVATION - 1) {
        Serial.println("Espaço de sequências esgotado; fila desativada.");
        return false;
    }
    return writeSequenceMeta(segmentBase + SEGMENT_SEQUENCE_RESERVATION,
                             recordBase + RECORD_SEQUENCE_RESERVATION);
}

bool initializeSequenceMeta(uint32_t segmentFloor, uint32_t recordFloor,
                            uint32_t &segmentResume, uint32_t &recordResume) {
    const bool exists[2] = {
        LittleFS.exists(SEQUENCE_META_FILES[0]),
        LittleFS.exists(SEQUENCE_META_FILES[1])
    };
    uint32_t generation[2] = {}, segmentLimit[2] = {}, recordLimit[2] = {};
    bool valid[2] = {
        readSequenceMeta(0, generation[0], segmentLimit[0], recordLimit[0]),
        readSequenceMeta(1, generation[1], segmentLimit[1], recordLimit[1])
    };
    sequenceMetaSlot = -1;
    const bool hadValidMeta = valid[0] || valid[1];
    if (!hadValidMeta && (exists[0] || exists[1])) {
        Serial.println("Metadados de sequência inválidos; fila em quarentena.");
        return false;
    }
    if (valid[0] || valid[1]) {
        sequenceMetaSlot = valid[1] && (!valid[0] ||
            static_cast<int32_t>(generation[1] - generation[0]) > 0) ? 1 : 0;
        sequenceMetaGeneration = generation[sequenceMetaSlot];
        reservedSegmentLimit = segmentLimit[sequenceMetaSlot];
        reservedRecordLimit = recordLimit[sequenceMetaSlot];
        segmentResume = reservedSegmentLimit;
        recordResume = reservedRecordLimit;
    } else {
        sequenceMetaGeneration = 0;
        reservedSegmentLimit = segmentFloor;
        reservedRecordLimit = recordFloor;
        segmentResume = segmentFloor;
        recordResume = recordFloor;
    }
    if (!reserveSequenceRanges(segmentFloor, recordFloor)) return false;
    if (hadValidMeta) {
        segmentResume = max(segmentResume, reservedSegmentLimit - SEGMENT_SEQUENCE_RESERVATION);
        recordResume = max(recordResume, reservedRecordLimit - RECORD_SEQUENCE_RESERVATION);
    }
    return true;
}

bool parseSegmentName(const String &name, uint32_t &sequence) {
    if (name.length() != 14 || name[0] != 'q' || name[1] != '-')
        return false;
    for (uint8_t i = 2; i < 10; ++i)
        if (name[i] < '0' || name[i] > '9') return false;
    if (name.substring(10) != ".seg") return false;
    sequence = static_cast<uint32_t>(strtoul(name.substring(2, 10).c_str(), nullptr, 10));
    return sequence != 0;
}

bool getFilesystemSpace(size_t &total, size_t &used) {
    FSInfo info;
    if (!LittleFS.info(info)) return false;
    total = info.totalBytes;
    used = info.usedBytes;
    return true;
}

bool writeSegmentHeader(File &file, uint32_t sequence) {
    uint8_t header[SEGMENT_HEADER_SIZE] = {};
    telemetry_queue::put32(header, SEGMENT_MAGIC);
    header[4] = SEGMENT_VERSION;
    header[5] = static_cast<uint8_t>(SEGMENT_HEADER_SIZE);
    telemetry_queue::put32(header + 8, sequence);
    telemetry_queue::put32(header + 20, telemetry_queue::crc32(header, 20));
    return writeExact(file, header, sizeof(header));
}

bool createSegment(uint32_t sequence, String &path) {
    size_t total, used;
    if (!getFilesystemSpace(total, used) || total <= used ||
        total - used <= FILESYSTEM_SAFETY_MARGIN + SEGMENT_MAX_SIZE) {
        Serial.println("LittleFS sem espaço seguro para novo segmento.");
        return false;
    }
    path = segmentPath(sequence);
    if (LittleFS.exists(path)) {
        Serial.println("Nome de segmento já existe; recusa sobrescrever arquivo.");
        return false;
    }
    File file = LittleFS.open(path, "w");
    if (!file) return false;
    bool ok = writeSegmentHeader(file, sequence);
    file.flush();
    file.close();
    File check = LittleFS.open(path, "r");
    bool headerValid = check && check.size() == SEGMENT_HEADER_SIZE;
    if (check) check.close();
    if (!ok || !headerValid) {
        Serial.println("Falha ao validar header do segmento novo.");
        return false;
    }
    return true;
}

bool closeSegment(SegmentInfo &segment) {
    if (segment.closed) return true;
    const size_t recordsEnd = SEGMENT_HEADER_SIZE +
        segment.count * telemetry_queue::RECORD_SIZE;
    // A previous interrupted footer write may have left a partial footer.
    // Re-establish the canonical end of records before retrying the footer.
    File repair = LittleFS.open(segment.path, "r+");
    bool repaired = repair && repair.truncate(recordsEnd);
    if (repair) {
        repair.flush();
        repair.close();
    }
    if (!repaired) return false;
    File file = LittleFS.open(segment.path, "a");
    if (!file) return false;
    uint8_t footer[SEGMENT_FOOTER_SIZE] = {};
    telemetry_queue::put32(footer, FOOTER_MAGIC);
    telemetry_queue::put32(footer + 4, segment.count);
    telemetry_queue::put32(footer + 8, telemetry_queue::crc32(footer, 8));
    bool ok = writeExact(file, footer, sizeof(footer));
    file.flush();
    file.close();
    if (!ok) return false;
    File check = LittleFS.open(segment.path, "r");
    bool footerValid = false;
    if (check && check.size() == SEGMENT_HEADER_SIZE +
            segment.count * telemetry_queue::RECORD_SIZE + SEGMENT_FOOTER_SIZE &&
        check.seek(check.size() - SEGMENT_FOOTER_SIZE)) {
        uint8_t stored[SEGMENT_FOOTER_SIZE];
        footerValid = readExact(check, stored, sizeof(stored)) &&
            telemetry_queue::get32(stored) == FOOTER_MAGIC &&
            telemetry_queue::get32(stored + 4) == segment.count &&
            telemetry_queue::get32(stored + 8) == telemetry_queue::crc32(stored, 8);
    }
    if (check) check.close();
    if (!footerValid) return false;
    segment.closed = true;
    return true;
}

bool appendWeather(const WeatherData &data) {
    if (!queueReady) return false;
    if (nextRecordSequence >= reservedRecordLimit ||
        nextSegmentSequence >= reservedSegmentLimit) {
        SegmentInfo *current = nullptr;
        for (auto &segment : segments)
            if (segment.valid && !segment.closed) current = &segment;
        if (current && nextRecordSequence >= reservedRecordLimit &&
            current->count && !closeSegment(*current)) return false;
        if (!reserveSequenceRanges(nextSegmentSequence, nextRecordSequence)) return false;
    }
    if (nextSegmentSequence > 99999999UL) {
        Serial.println("Espaço de sequências de segmento esgotado.");
        return false;
    }
    if (nextRecordSequence == UINT32_MAX) {
        Serial.println("Espaço de sequências de registro esgotado.");
        return false;
    }
    SegmentInfo *active = nullptr;
    for (auto &segment : segments)
        if (segment.valid && !segment.closed) active = &segment;

    if (active && active->count >= telemetry_queue::MAX_RECORDS_PER_SEGMENT) {
        if (!closeSegment(*active)) {
            if (active->closed) active->recoverableTail = false;
            return false;
        }
        active = nullptr;
    }
    if (!active && !segments.empty()) {
        SegmentInfo &last = segments.back();
        if (!last.closed) return false;
        const uint32_t expectedFirst = last.firstRecordSequence + last.count;
        if (nextRecordSequence < expectedFirst) return false;
    }
    if (!active) {
        String path;
        if (!createSegment(nextSegmentSequence, path)) return false;
        SegmentInfo segment = {nextSegmentSequence++, nextRecordSequence, path, 0, false, true, false};
        segments.push_back(segment);
        active = &segments.back();
    }

    size_t total, used;
    if (!getFilesystemSpace(total, used) || total <= used ||
        total - used <= FILESYSTEM_SAFETY_MARGIN +
                            (active->count == 0 ? SEGMENT_MAX_SIZE :
                             telemetry_queue::RECORD_SIZE +
                             (active->count + 1 == telemetry_queue::MAX_RECORDS_PER_SEGMENT ?
                                  SEGMENT_FOOTER_SIZE : 0))) {
        Serial.println("LittleFS: margem de segurança impede enfileirar amostra.");
        return false;
    }

    File file = LittleFS.open(active->path, "a");
    if (!file || file.size() != SEGMENT_HEADER_SIZE +
                              active->count * telemetry_queue::RECORD_SIZE) {
        if (file) file.close();
        Serial.println("Segmento ativo com tamanho inesperado; append cancelado.");
        return false;
    }
    uint8_t record[telemetry_queue::RECORD_SIZE];
    telemetry_queue::encodeRecord(record, nextRecordSequence, data.collectedAt,
                                  weatherValues(data));
    bool ok = writeExact(file, record, sizeof(record));
    file.flush();
    file.close();
    File verify = LittleFS.open(active->path, "r");
    const size_t expectedSize = SEGMENT_HEADER_SIZE +
        (active->count + 1) * telemetry_queue::RECORD_SIZE;
    const bool verified = verify && verify.size() == expectedSize;
    if (verify) verify.close();
    if (!ok || !verified) {
        Serial.println("Falha ao gravar registro binário completo.");
        return false;
    }
    ++active->count;
    ++nextRecordSequence;
    if (active->count == telemetry_queue::MAX_RECORDS_PER_SEGMENT &&
        !closeSegment(*active)) {
        if (active->closed) active->recoverableTail = false;
        Serial.println("Falha ao fechar segmento cheio; dados preservados.");
        return false;
    }
    return true;
}

bool inspectSegment(SegmentInfo &segment, uint32_t &lastRecordSequence,
                    bool &haveRecordSequence) {
    File file = LittleFS.open(segment.path, "r");
    if (!file) return false;
    uint8_t header[SEGMENT_HEADER_SIZE];
    bool valid = readExact(file, header, sizeof(header)) &&
        telemetry_queue::get32(header) == SEGMENT_MAGIC &&
        header[4] == SEGMENT_VERSION && header[5] == SEGMENT_HEADER_SIZE &&
        telemetry_queue::get32(header + 8) == segment.sequence &&
        telemetry_queue::get32(header + 20) == telemetry_queue::crc32(header, 20);
    uint32_t expected = 0;
    uint16_t count = 0;
    while (valid && count < telemetry_queue::MAX_RECORDS_PER_SEGMENT &&
           file.size() - file.position() >= telemetry_queue::RECORD_SIZE) {
        uint8_t record[telemetry_queue::RECORD_SIZE];
        uint64_t timestamp;
        telemetry_queue::Values values;
        if (!readExact(file, record, sizeof(record))) {
            valid = false;
            break;
        }
        const uint32_t storedSequence = telemetry_queue::recordSequence(record);
        if (count == 0) {
            expected = storedSequence;
            segment.firstRecordSequence = expected;
            if (haveRecordSequence && expected <= lastRecordSequence) {
                valid = false;
                break;
            }
        } else if (storedSequence != expected) {
            valid = false;
            break;
        }
        if (!telemetry_queue::decodeRecord(record, expected, timestamp, values)) {
            valid = false;
            break;
        }
        ++count;
        lastRecordSequence = expected;
        haveRecordSequence = true;
        ++expected;
    }
    size_t tail = file.size() - file.position();
    bool closed = false;
    if (valid && tail == SEGMENT_FOOTER_SIZE) {
        uint8_t footer[SEGMENT_FOOTER_SIZE];
        closed = readExact(file, footer, sizeof(footer)) &&
            telemetry_queue::get32(footer) == FOOTER_MAGIC &&
            telemetry_queue::get32(footer + 4) == count &&
            telemetry_queue::get32(footer + 8) == telemetry_queue::crc32(footer, 8);
        valid = closed;
    } else if (count == telemetry_queue::MAX_RECORDS_PER_SEGMENT &&
               tail < SEGMENT_FOOTER_SIZE) {
        // Includes no footer and a power-loss-truncated footer.
        segment.recoverableTail = true;
    } else if (tail != 0 && count < telemetry_queue::MAX_RECORDS_PER_SEGMENT) {
        segment.recoverableTail = true;
    } else if (tail != 0 || count == telemetry_queue::MAX_RECORDS_PER_SEGMENT) {
        valid = false;
    }
    file.close();
    segment.count = count;
    segment.closed = closed;
    segment.valid = valid;
    return valid;
}

bool recoverQueue() {
    segments.clear();
    highestSegmentSequence = 0;
    Dir dir = LittleFS.openDir("/");
    while (dir.next()) {
        uint32_t sequence;
        if (!parseSegmentName(dir.fileName(), sequence)) continue;
        if (sequence > highestSegmentSequence) highestSegmentSequence = sequence;
        if (segments.size() >= MAX_SEGMENTS) {
            Serial.println("Limite de segmentos excedido; recuperação interrompida.");
            return false;
        }
        SegmentInfo segment = {sequence, 0, String("/") + dir.fileName(), 0, false, false, false};
        segments.push_back(segment);
    }
    std::sort(segments.begin(), segments.end(),
              [](const SegmentInfo &a, const SegmentInfo &b) { return a.sequence < b.sequence; });
    if (highestSegmentSequence >= 99999999UL) {
        Serial.println("Sequências de segmento esgotadas; fila preservada.");
        return false;
    }
    nextSegmentSequence = highestSegmentSequence + 1;
    uint32_t lastRecord = 0;
    bool haveRecord = false;
    for (size_t i = 0; i < segments.size(); ++i) {
        if (i && segments[i - 1].sequence == segments[i].sequence) {
            Serial.println("Sequência de segmento duplicada; fila em quarentena lógica.");
            return false;
        }
        if (!inspectSegment(segments[i], lastRecord, haveRecord)) {
            Serial.print("Segmento inválido preservado: "); Serial.println(segments[i].path);
            return false;
        }
        if (segments[i].recoverableTail && i + 1 != segments.size()) {
            Serial.print("Cauda incompleta fora do segmento mais recente: ");
            Serial.println(segments[i].path);
            return false;
        }
        if (!segments[i].closed && i + 1 != segments.size()) {
            Serial.print("Segmento aberto não é o mais recente: ");
            Serial.println(segments[i].path);
            return false;
        }
    }
    // The first remaining record may follow segments removed by an earlier replay.
    if (haveRecord && lastRecord == UINT32_MAX) {
        Serial.println("Sequências de registro esgotadas; fila preservada.");
        return false;
    }
    nextRecordSequence = lastRecord + 1;
    if (!segments.empty() && segments.back().recoverableTail) {
        // ESP8266 LittleFS implements truncate for a writable file handle.
        File active = LittleFS.open(segments.back().path, "r+");
        const size_t validEnd = SEGMENT_HEADER_SIZE +
            segments.back().count * telemetry_queue::RECORD_SIZE;
        bool truncated = active && active.truncate(validEnd);
        if (!truncated) {
            if (active) active.close();
            Serial.println("Não foi possível remover a cauda parcial; fila permanece preservada.");
            return false;
        }
        active.flush();
        active.close();
        File verify = LittleFS.open(segments.back().path, "r");
        bool verified = verify && verify.size() == validEnd;
        if (verify) verify.close();
        if (!verified) {
            Serial.println("Tamanho após truncate inválido; fila permanece em quarentena.");
            return false;
        }
        segments.back().recoverableTail = false;
    }
    uint32_t segmentResume, recordResume;
    if (!initializeSequenceMeta(nextSegmentSequence, nextRecordSequence,
                                segmentResume, recordResume)) return false;
    if (segmentResume > 99999999UL || recordResume == UINT32_MAX) return false;
    if (!segments.empty() && !segments.back().closed) {
        if (!closeSegment(segments.back())) return false;
    }
    nextSegmentSequence = max(nextSegmentSequence, segmentResume);
    nextRecordSequence = max(nextRecordSequence, recordResume);
    replaySegment = 0;
    replayRecord = 0;
    queueReady = true;
    size_t total, used;
    if (getFilesystemSpace(total, used)) {
        Serial.printf("LittleFS total=%u usado=%u livre=%u\n",
                      static_cast<unsigned>(total), static_cast<unsigned>(used),
                      static_cast<unsigned>(total - used));
    }
    return true;
}

bool readQueuedRecord(const SegmentInfo &segment, uint16_t index,
                      uint64_t &timestamp, telemetry_queue::Values &values) {
    File file = LittleFS.open(segment.path, "r");
    if (!file || !file.seek(SEGMENT_HEADER_SIZE +
                            static_cast<size_t>(index) * telemetry_queue::RECORD_SIZE)) {
        if (file) file.close();
        return false;
    }
    uint8_t bytes[telemetry_queue::RECORD_SIZE];
    bool ok = readExact(file, bytes, sizeof(bytes)) &&
        telemetry_queue::decodeRecord(bytes, telemetry_queue::recordSequence(bytes),
                                      timestamp, values) &&
        telemetry_queue::recordSequence(bytes) == segment.firstRecordSequence + index;
    file.close();
    return ok;
}

bool removeCompletedSegment(const SegmentInfo &segment) {
    if (!LittleFS.remove(segment.path)) {
        Serial.print("Falha ao remover segmento concluído: ");
        Serial.println(segment.path);
        return false;
    }
    return true;
}

#if ECOWITT_QUEUE_FS_TEST
const char *QUEUE_FS_TEST_STATE = "/q-fs-test.state";
const uint32_t QUEUE_FS_TEST_MAGIC = 0x54534651UL;
const size_t QUEUE_FS_TEST_STATE_SIZE = 20;
enum QueueFsTestPhase : uint32_t { QUEUE_FS_TEST_IDLE = 0, QUEUE_FS_TEST_EXPECT_SEGMENT = 1,
                                  QUEUE_FS_TEST_EXPECT_EMPTY = 2 };
struct QueueFsTestState {
    uint32_t phase;
    uint32_t segmentSequence;
    uint32_t firstRecordSequence;
};
struct QueueFsTestMetaSnapshot {
    uint32_t generation[2];
    uint32_t segmentLimit[2];
    uint32_t recordLimit[2];
    bool valid[2];
};
QueueFsTestState queueFsTestState = {QUEUE_FS_TEST_IDLE, 0, 0};
bool queueFsTestStateValid = true;
QueueFsTestMetaSnapshot queueFsTestMetaBeforeRecovery = {};
bool queueFsTestMetaSnapshotValid = false;

bool writeQueueFsTestState(const QueueFsTestState &state) {
    uint8_t bytes[QUEUE_FS_TEST_STATE_SIZE] = {};
    telemetry_queue::put32(bytes, QUEUE_FS_TEST_MAGIC);
    telemetry_queue::put32(bytes + 4, state.phase);
    telemetry_queue::put32(bytes + 8, state.segmentSequence);
    telemetry_queue::put32(bytes + 12, state.firstRecordSequence);
    telemetry_queue::put32(bytes + 16, telemetry_queue::crc32(bytes, 16));
    File file = LittleFS.open(QUEUE_FS_TEST_STATE, "w");
    bool ok = file && writeExact(file, bytes, sizeof(bytes));
    if (file) { file.flush(); file.close(); }
    File verify = LittleFS.open(QUEUE_FS_TEST_STATE, "r");
    uint8_t check[QUEUE_FS_TEST_STATE_SIZE];
    bool valid = verify && verify.size() == sizeof(check) && readExact(verify, check, sizeof(check));
    if (verify) verify.close();
    if (!ok || !valid || memcmp(bytes, check, sizeof(bytes)) != 0) return false;
    queueFsTestState = state;
    return true;
}

bool loadQueueFsTestState() {
    if (!LittleFS.exists(QUEUE_FS_TEST_STATE)) return true;
    File file = LittleFS.open(QUEUE_FS_TEST_STATE, "r");
    uint8_t bytes[QUEUE_FS_TEST_STATE_SIZE];
    bool valid = file && file.size() == sizeof(bytes) && readExact(file, bytes, sizeof(bytes));
    if (file) file.close();
    if (!valid || telemetry_queue::get32(bytes) != QUEUE_FS_TEST_MAGIC ||
        telemetry_queue::get32(bytes + 16) != telemetry_queue::crc32(bytes, 16)) return false;
    queueFsTestState.phase = telemetry_queue::get32(bytes + 4);
    queueFsTestState.segmentSequence = telemetry_queue::get32(bytes + 8);
    queueFsTestState.firstRecordSequence = telemetry_queue::get32(bytes + 12);
    return queueFsTestState.phase <= QUEUE_FS_TEST_EXPECT_EMPTY;
}

bool queueFsTestSegmentExists() {
    Dir dir = LittleFS.openDir("/");
    while (dir.next()) {
        uint32_t sequence;
        if (parseSegmentName(dir.fileName(), sequence)) return true;
    }
    return false;
}

bool queueIsEmptyForTest() {
    return segments.empty() && !queueFsTestSegmentExists() &&
        !LittleFS.exists(PENDING_TELEMETRY_FILE);
}

bool verifyTestRecords(const SegmentInfo &segment, uint32_t firstRecord) {
    if (!segment.valid || !segment.closed || segment.count != 3 ||
        segment.firstRecordSequence != firstRecord ||
        nextRecordSequence <= firstRecord + 2) return false;
    for (uint16_t i = 0; i < 3; ++i) {
        uint64_t timestamp;
        telemetry_queue::Values values;
        if (!readQueuedRecord(segment, i, timestamp, values) ||
            timestamp != 1700000000000ULL + (i + 1) * 1000 ||
            values.value[0] != 21.5f + i + 1 || values.value[2] != 57.0f) return false;
    }
    return true;
}

bool printQueueStorageMetrics() {
    size_t totalBytes, usedBytes;
    if (!getFilesystemSpace(totalBytes, usedBytes) || segments.empty()) {
        Serial.println("QUEUE: medição indisponível (LittleFS/segmentos).");
        return false;
    }

    uint32_t recordCount = 0;
    size_t queueBytes = 0;
    for (const auto &segment : segments) {
        File file = LittleFS.open(segment.path, "r");
        if (!file) {
            Serial.println("QUEUE: medição indisponível (falha ao abrir segmento).");
            return false;
        }
        queueBytes += file.size();
        file.close();
        recordCount += segment.count;
    }
    if (!recordCount || usedBytes < queueBytes ||
        totalBytes <= usedBytes - queueBytes + FILESYSTEM_SAFETY_MARGIN) {
        Serial.println("QUEUE: medição indisponível (capacidade útil insuficiente).");
        return false;
    }

    const double bytesPerRecord = static_cast<double>(queueBytes) / recordCount;
    const double recordsPerMb = 1000000.0 / bytesPerRecord;
    // Preserve current non-queue filesystem usage and the queue's existing
    // safety margin when estimating the space available to future queue data.
    const size_t queueCapacityBytes = totalBytes - (usedBytes - queueBytes) -
                                      FILESYSTEM_SAFETY_MARGIN;
    const double estimatedRecords = queueCapacityBytes / bytesPerRecord;
    const double estimatedHours = estimatedRecords / 60.0;
    const double estimatedDays = estimatedHours / 24.0;

    Serial.printf("QUEUE: %lu registros\n", static_cast<unsigned long>(recordCount));
    Serial.printf("QUEUE: %lu segmentos\n", static_cast<unsigned long>(segments.size()));
    Serial.printf("QUEUE: %lu bytes\n", static_cast<unsigned long>(queueBytes));
    Serial.printf("QUEUE: %.1f bytes/registro\n", bytesPerRecord);
    Serial.printf("QUEUE: ~%.0f registros/MB\n", recordsPerMb);
    Serial.printf("QUEUE: ~%.0f horas @ 1 registro/min\n", estimatedHours);
    Serial.printf("QUEUE: ~%.2f dias @ 1 registro/min\n", estimatedDays);
    Serial.printf("QUEUE: capacidade estimada=%lu bytes (LittleFS útil menos uso não-fila e margem de %lu bytes)\n",
                  static_cast<unsigned long>(queueCapacityBytes),
                  static_cast<unsigned long>(FILESYSTEM_SAFETY_MARGIN));
    return true;
}

bool captureSequenceMetadataBeforeRecovery() {
    queueFsTestMetaSnapshotValid = false;
    for (int slot = 0; slot < 2; ++slot) {
        queueFsTestMetaBeforeRecovery.valid[slot] =
            LittleFS.exists(SEQUENCE_META_FILES[slot]) &&
            readSequenceMeta(slot,
                queueFsTestMetaBeforeRecovery.generation[slot],
                queueFsTestMetaBeforeRecovery.segmentLimit[slot],
                queueFsTestMetaBeforeRecovery.recordLimit[slot]);
    }
    queueFsTestMetaSnapshotValid = queueFsTestMetaBeforeRecovery.valid[0] &&
        queueFsTestMetaBeforeRecovery.valid[1];
    return queueFsTestMetaSnapshotValid;
}

bool verifyRecoveredSequenceMetadata(uint32_t previousSegment,
                                     uint32_t previousRecord,
                                     uint32_t &expectedSegment,
                                     uint32_t &expectedRecord) {
    if (!queueFsTestMetaSnapshotValid || sequenceMetaSlot < 0 ||
        sequenceMetaSlot > 1) return false;

    // Recovery writes its reservation to the opposite slot. The still-active
    // slot after recovery therefore identifies the slot selected beforehand.
    const int selectedSlot = 1 - sequenceMetaSlot;
    const uint32_t selectedGeneration =
        queueFsTestMetaBeforeRecovery.generation[selectedSlot];
    const uint32_t selectedSegmentLimit =
        queueFsTestMetaBeforeRecovery.segmentLimit[selectedSlot];
    const uint32_t selectedRecordLimit =
        queueFsTestMetaBeforeRecovery.recordLimit[selectedSlot];
    const uint32_t otherGeneration =
        queueFsTestMetaBeforeRecovery.generation[sequenceMetaSlot];
    const bool selectedWasLatest = selectedSlot == 1
        ? (!queueFsTestMetaBeforeRecovery.valid[0] ||
           static_cast<int32_t>(selectedGeneration - otherGeneration) > 0)
        : (!queueFsTestMetaSnapshotValid ||
           static_cast<int32_t>(otherGeneration - selectedGeneration) <= 0);

    if (!queueFsTestMetaBeforeRecovery.valid[selectedSlot] || !selectedWasLatest ||
        sequenceMetaGeneration != selectedGeneration + 1 ||
        reservedSegmentLimit != selectedSegmentLimit + SEGMENT_SEQUENCE_RESERVATION ||
        reservedRecordLimit != selectedRecordLimit + RECORD_SEQUENCE_RESERVATION ||
        nextSegmentSequence != selectedSegmentLimit ||
        nextRecordSequence != selectedRecordLimit ||
        nextSegmentSequence <= previousSegment ||
        nextRecordSequence <= previousRecord) return false;

    expectedSegment = selectedSegmentLimit;
    expectedRecord = selectedRecordLimit;
    return true;
}

bool startQueueFsRebootTest() {
    if (!queueFsTestStateValid || queueFsTestState.phase != QUEUE_FS_TEST_IDLE ||
        !queueIsEmptyForTest()) {
        Serial.println("TEST ABORTADO: fase inicial exige fila de teste vazia.");
        return false;
    }
    // Exercise both alternating high-water slots even for this short test.
    if (!reserveSequenceRanges(nextSegmentSequence, nextRecordSequence) ||
        !reserveSequenceRanges(nextSegmentSequence, nextRecordSequence)) return false;
    const uint32_t firstSegment = nextSegmentSequence;
    const uint32_t firstRecord = nextRecordSequence;
    WeatherData sample{};
    sample.collectedAt = 1700000000000ULL;
    sample.temperature_out = 21.5f;
    sample.humidity_out = 57.0f;
    for (uint8_t i = 0; i < 3; ++i) {
        sample.collectedAt += 1000;
        sample.temperature_out += 1.0f;
        if (!appendWeather(sample)) return false;
    }
    if (segments.size() != 1 || segments[0].sequence != firstSegment ||
        segments[0].firstRecordSequence != firstRecord || !closeSegment(segments[0])) return false;
    if (!printQueueStorageMetrics()) return false;
    QueueFsTestState state = {QUEUE_FS_TEST_EXPECT_SEGMENT, firstSegment, firstRecord};
    if (!writeQueueFsTestState(state)) return false;
    Serial.println("TEST: segmento com footer e 3 registros persistido; não removido.");
    Serial.println("TEST PRONTO PARA REBOOT FÍSICO. Envie 'r' e pressione RESET no ESP8266.");
    return true;
}

bool handleQueueFsTestBoot() {
    if (!queueFsTestStateValid) return false;
    if (queueFsTestState.phase == QUEUE_FS_TEST_IDLE) return true;
    if (queueFsTestState.phase == QUEUE_FS_TEST_EXPECT_SEGMENT) {
        if (segments.size() != 1 || segments[0].sequence != queueFsTestState.segmentSequence ||
            !verifyTestRecords(segments[0], queueFsTestState.firstRecordSequence)) return false;
        Serial.println("TEST: reboot físico confirmado; recoverQueue encontrou segmento e sequências íntegros.");
        if (!removeCompletedSegment(segments[0])) return false;
        segments.clear();
        if (!queueIsEmptyForTest()) return false;
        QueueFsTestState state = {QUEUE_FS_TEST_EXPECT_EMPTY,
                                  queueFsTestState.segmentSequence,
                                  queueFsTestState.firstRecordSequence};
        if (!writeQueueFsTestState(state)) return false;
        Serial.println("TEST: segmento removido; fila vazia confirmada.");
        Serial.println("TEST PRONTO PARA SEGUNDO REBOOT FÍSICO. Envie 'r' e pressione RESET.");
        return true;
    }
    if (queueFsTestState.phase == QUEUE_FS_TEST_EXPECT_EMPTY) {
        uint32_t expectedSegment, expectedRecord;
        if (!queueIsEmptyForTest() ||
            !verifyRecoveredSequenceMetadata(queueFsTestState.segmentSequence,
                queueFsTestState.firstRecordSequence + 3,
                expectedSegment, expectedRecord)) return false;
        Serial.printf("TEST: fila vazia; slot A/B selecionado=%c, geração=%lu, limites segmento=%lu registro=%lu; próximos recuperados=%lu/%lu.\n",
            sequenceMetaSlot == 0 ? 'B' : 'A',
            static_cast<unsigned long>(queueFsTestMetaBeforeRecovery.generation[1 - sequenceMetaSlot]),
            static_cast<unsigned long>(expectedSegment),
            static_cast<unsigned long>(expectedRecord),
            static_cast<unsigned long>(nextSegmentSequence),
            static_cast<unsigned long>(nextRecordSequence));
        WeatherData sample{};
        sample.collectedAt = 1700000010000ULL;
        sample.temperature_out = 42.0f;
        if (!appendWeather(sample) || segments.size() != 1 ||
            segments[0].sequence != expectedSegment ||
            segments[0].firstRecordSequence != expectedRecord ||
            !closeSegment(segments[0])) return false;
        const uint32_t previousSegment = queueFsTestState.segmentSequence;
        const uint32_t previousRecord = queueFsTestState.firstRecordSequence;
        uint64_t timestamp;
        telemetry_queue::Values values;
        if (!readQueuedRecord(segments[0], 0, timestamp, values) ||
            timestamp != sample.collectedAt || values.value[0] != sample.temperature_out) return false;
        const uint32_t newSegment = segments[0].sequence;
        const uint32_t newRecord = segments[0].firstRecordSequence;
        if (!removeCompletedSegment(segments[0])) return false;
        segments.clear();
        if (!queueIsEmptyForTest() || !LittleFS.remove(QUEUE_FS_TEST_STATE)) return false;
        queueFsTestState.phase = QUEUE_FS_TEST_IDLE;
        Serial.printf("RESULTADO: PASS; segmento %lu -> %lu, registro %lu -> %lu\n",
                      static_cast<unsigned long>(previousSegment),
                      static_cast<unsigned long>(newSegment),
                      static_cast<unsigned long>(previousRecord),
                      static_cast<unsigned long>(newRecord));
    }
    return true;
}
#endif

bool processLegacyBatch(size_t &budget) {
    if (!LittleFS.exists(PENDING_TELEMETRY_FILE)) return true;
    File file = LittleFS.open(PENDING_TELEMETRY_FILE, "r");
    if (!file) return false;
    if (legacyPosition > file.size()) {
        // The legacy file may have grown while new samples were being appended.
        file.close();
        legacyPosition = 0;
        return false;
    }
    if (!file.seek(legacyPosition)) { file.close(); return false; }
    const unsigned long started = millis();
    bool reachedEnd = true;
    while (budget && file.available() && millis() - started < REPLAY_BUDGET_MS) {
        const size_t recordStart = file.position();
        String record = file.readStringUntil('\n');
        const size_t recordEnd = file.position();
        record.trim();
        if (!record.length()) {
            legacyPosition = recordEnd;
            continue;
        }
        JsonDocument doc;
        if (deserializeJson(doc, record) || !doc["ts"].is<uint64_t>() ||
            !doc["values"].is<JsonObject>()) {
            Serial.println("Linha JSONL inválida; arquivo original preservado e replay bloqueado.");
            file.close();
            return false;
        }
        uint64_t timestamp = doc["ts"].as<uint64_t>();
        if (!publishPayload(record, timestamp)) {
            file.seek(recordStart);
            legacyPosition = recordStart;
            reachedEnd = false;
            break;
        }
        legacyPosition = recordEnd;
        --budget;
    }
    bool drained = reachedEnd && !file.available() &&
        legacyPosition == file.size() && budget > 0;
    file.close();
    if (drained) {
        if (LittleFS.remove(PENDING_TELEMETRY_FILE)) {
            legacyPosition = 0;
        } else {
            Serial.println("Falha ao remover fila JSONL legada drenada.");
            return false;
        }
    }
    return true;
}

void publishPendingTelemetry() {
    if (!queueReady || !mqtt.connected()) return;
    // JSONL has absolute replay priority. Any invalid or failed line blocks
    // binary replay, but does not prevent appendWeather() from queuing samples.
    if (LittleFS.exists(PENDING_TELEMETRY_FILE)) {
        size_t budget = REPLAY_MAX_RECORDS;
        processLegacyBatch(budget);
        return;
    }
    const unsigned long started = millis();
    size_t budget = REPLAY_MAX_RECORDS;

    while (budget && replaySegment < static_cast<int>(segments.size()) &&
           millis() - started < REPLAY_BUDGET_MS) {
        SegmentInfo &segment = segments[replaySegment];
        if (!segment.valid) return;
        if (replayRecord >= segment.count) {
            if (segment.closed) {
                if (!removeCompletedSegment(segment)) return;
                segments.erase(segments.begin() + replaySegment);
                replayRecord = 0;
                continue;
            }
            if (!closeSegment(segment)) return;
            segment.recoverableTail = false;
            continue;
        }
        uint64_t timestamp;
        telemetry_queue::Values values;
        if (!readQueuedRecord(segment, replayRecord, timestamp, values)) {
            Serial.println("Falha de integridade durante replay; segmento preservado.");
            return;
        }
        WeatherData data = valuesWeather(values, timestamp);
        String payload;
        makeTelemetryPayload(data, timestamp, payload);
        if (!publishPayload(payload, timestamp)) return;
        ++replayRecord;
        --budget;
    }

    if (replaySegment < static_cast<int>(segments.size())) {
        SegmentInfo &segment = segments[replaySegment];
        if (replayRecord == segment.count && segment.closed &&
            removeCompletedSegment(segment)) {
            segments.erase(segments.begin() + replaySegment);
            replayRecord = 0;
        }
    }
}

} // namespace

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
    JsonDocument doc;
    if (deserializeJson(doc, payload) || !doc["ts"].is<uint64_t>() ||
        !doc["values"].is<JsonObject>()) {
        Serial.println("Payload não pode ser codificado na fila binária.");
        return false;
    }
    WeatherData data{};
    JsonObject values = doc["values"].as<JsonObject>();
    const char *keys[20] = {"temperature_out", "feels_like_out", "humidity_out",
        "dew_point_out", "pressure_relative", "wind_speed", "wind_gust",
        "wind_direction", "wind_direction_10min", "solar_radiation",
        "temperature_in", "humidity_in", "rain_rate", "rain_event", "rain_hour",
        "rain_day", "rain_week", "rain_month", "rain_year", "rain_total"};
    float decoded[20];
    for (uint8_t i = 0; i < 20; ++i) {
        if (!values.containsKey(keys[i])) decoded[i] = NAN;
        else if (values[keys[i]].is<float>() || values[keys[i]].is<double>())
            decoded[i] = values[keys[i]].as<float>();
        else return false;
    }
    telemetry_queue::Values fields;
    memcpy(fields.value, decoded, sizeof(decoded));
    data = valuesWeather(fields, doc["ts"].as<uint64_t>());
    bool saved = appendWeather(data);
    if (!saved)
        Serial.println("Falha ao persistir telemetria; amostra não foi enfileirada.");
    return saved;
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

    if (LittleFS.exists(PENDING_TELEMETRY_FILE) || !segments.empty())
        return savePendingPayload(payload);

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

    LittleFSConfig fsConfig(false);
    LittleFS.setConfig(fsConfig);
    if (!LittleFS.begin())
    {
        Serial.println("Falha ao montar LittleFS.");
    }
#if ECOWITT_QUEUE_FS_TEST
    else
    {
        queueReady = false;
        queueFsTestStateValid = loadQueueFsTestState();
        if (!queueFsTestStateValid) {
            Serial.println("TEST: marcador de fase inválido; não alterando arquivos da fila.");
        } else if (queueFsTestState.phase == QUEUE_FS_TEST_EXPECT_EMPTY &&
                   !captureSequenceMetadataBeforeRecovery()) {
            Serial.println("TEST: A/B inválidos antes da recuperação da fila vazia.");
        } else if (!recoverQueue()) {
            Serial.println("TEST: recoverQueue falhou; dados preservados.");
        } else if (!handleQueueFsTestBoot()) {
            Serial.println("RESULTADO: FAIL na verificação deste boot; dados preservados.");
        } else if (queueFsTestState.phase == QUEUE_FS_TEST_IDLE) {
            Serial.println("Fila recuperada pelo fluxo normal; inicie com 't' em LittleFS de teste vazio.");
        }
    }
#else
    else if (!recoverQueue())
    {
        Serial.println("Fila não recuperada; arquivos preservados e novos appends desativados.");
    }
#endif

#if ECOWITT_QUEUE_FS_TEST
    Serial.println("Modo de teste LittleFS ativo, sem Wi-Fi/MQTT.");
    Serial.println("Envie 't' para iniciar (somente LittleFS vazio); 'r' confirma que vai pressionar RESET.");
    return;
#endif

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
#if ECOWITT_QUEUE_FS_TEST
    if (Serial.available()) {
        const int command = Serial.read();
        if (command == 't' || command == 'T') {
            const bool passed = startQueueFsRebootTest();
            if (!passed) Serial.println("RESULTADO: FAIL ao preparar segmento.");
        } else if (command == 'r' || command == 'R') {
            if (queueFsTestState.phase == QUEUE_FS_TEST_IDLE) {
                Serial.println("Nenhum reboot está pendente.");
            } else if (queueFsTestState.phase == QUEUE_FS_TEST_EXPECT_SEGMENT &&
                       segments.size() == 1 && segments[0].closed) {
                Serial.println("AÇÃO EXPLÍCITA CONFIRMADA: pressione agora o botão RESET do ESP8266.");
            } else if (queueFsTestState.phase == QUEUE_FS_TEST_EXPECT_EMPTY &&
                       queueIsEmptyForTest()) {
                Serial.println("AÇÃO EXPLÍCITA CONFIRMADA: pressione agora o botão RESET do ESP8266.");
            } else {
                Serial.println("Reboot ainda não liberado: conclua/verifique a fase atual primeiro.");
            }
        }
    }
    delay(10);
    return;
#endif

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
