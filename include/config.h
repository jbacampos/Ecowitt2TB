#pragma once

#include "secrets.h"

// ---------- Wi-Fi ----------
#define WIFI_SSID       "DispIoT"

// ---------- Ecowitt GW3000 ----------
#define ECOWITT_HOST    "192.168.71.50"
#define ECOWITT_PORT    80

// ---------- ThingsBoard ----------
#define TB_HOST         "mqtt.datalog.top"
#define TB_PORT         8883

#define TB_CLIENT_ID    "GW3000-D4E9F4F53A47"
#define TB_USERNAME     "ecowitt-gw3000"

#define TB_TOPIC        "v1/devices/me/telemetry"

// ---------- Coleta ----------
#define UPLOAD_INTERVAL 60000UL

// ---------- Fila local ----------
// Intervalo entre tentativas MQTT; a leitura do gateway continua mesmo sem Internet.
#define MQTT_RETRY_INTERVAL 10000UL

// ---------- Relógio NTP ----------
#define NTP_SERVER_1 "time.cloudflare.com"
#define NTP_SERVER_2 "time.google.com"
#define NTP_SERVER_3 "pool.ntp.org"
#define UTC_OFFSET_SECONDS (-3 * 3600) // Horário de Brasília (UTC-3)
