#pragma once

#include <math.h>
#include <stdint.h>
#include <string.h>

namespace telemetry_queue {

static const uint16_t RECORD_SIZE = 104;
static const uint8_t RECORD_VERSION = 1;
static const uint16_t RECORD_MAGIC = 0x5154;
static const uint32_t MAX_RECORDS_PER_SEGMENT = 32;

struct Values {
    float value[20];
};

inline uint32_t crc32(const uint8_t *data, size_t length) {
    uint32_t crc = 0xFFFFFFFFUL;
    for (size_t i = 0; i < length; ++i) {
        crc ^= data[i];
        for (uint8_t bit = 0; bit < 8; ++bit)
            crc = (crc >> 1) ^ (0xEDB88320UL & (0UL - (crc & 1UL)));
    }
    return ~crc;
}

inline void put16(uint8_t *out, uint16_t value) {
    out[0] = static_cast<uint8_t>(value);
    out[1] = static_cast<uint8_t>(value >> 8);
}

inline void put32(uint8_t *out, uint32_t value) {
    for (uint8_t i = 0; i < 4; ++i)
        out[i] = static_cast<uint8_t>(value >> (8 * i));
}

inline void put64(uint8_t *out, uint64_t value) {
    for (uint8_t i = 0; i < 8; ++i)
        out[i] = static_cast<uint8_t>(value >> (8 * i));
}

inline uint16_t get16(const uint8_t *in) {
    return static_cast<uint16_t>(in[0]) |
           (static_cast<uint16_t>(in[1]) << 8);
}

inline uint32_t get32(const uint8_t *in) {
    uint32_t value = 0;
    for (uint8_t i = 0; i < 4; ++i)
        value |= static_cast<uint32_t>(in[i]) << (8 * i);
    return value;
}

inline uint64_t get64(const uint8_t *in) {
    uint64_t value = 0;
    for (uint8_t i = 0; i < 8; ++i)
        value |= static_cast<uint64_t>(in[i]) << (8 * i);
    return value;
}

inline void encodeRecord(uint8_t out[RECORD_SIZE], uint32_t sequence,
                         uint64_t timestamp, const Values &values) {
    static_assert(sizeof(float) == 4, "Record v1 requires IEEE-754 float32");
    memset(out, 0, RECORD_SIZE);
    put16(out, RECORD_MAGIC);
    out[2] = RECORD_VERSION;
    put16(out + 3, RECORD_SIZE);
    put32(out + 5, sequence);
    put64(out + 9, timestamp);

    uint32_t presence = 0;
    for (uint8_t i = 0; i < 20; ++i) {
        uint8_t *slot = out + 20 + 4 * i;
        if (!isnan(values.value[i])) {
            presence |= (1UL << i);
            uint32_t bits;
            memcpy(&bits, &values.value[i], sizeof(bits));
            put32(slot, bits);
        }
    }
    out[17] = static_cast<uint8_t>(presence);
    out[18] = static_cast<uint8_t>(presence >> 8);
    out[19] = static_cast<uint8_t>(presence >> 16);
    put32(out + 100, crc32(out, 100));
}

inline bool decodeRecord(const uint8_t in[RECORD_SIZE], uint32_t expectedSequence,
                         uint64_t &timestamp, Values &values) {
    if (get16(in) != RECORD_MAGIC || in[2] != RECORD_VERSION ||
        get16(in + 3) != RECORD_SIZE || get32(in + 5) != expectedSequence ||
        get32(in + 100) != crc32(in, 100))
        return false;

    timestamp = get64(in + 9);
    const uint32_t presence = static_cast<uint32_t>(in[17]) |
        (static_cast<uint32_t>(in[18]) << 8) |
        (static_cast<uint32_t>(in[19]) << 16);
    for (uint8_t i = 0; i < 20; ++i) {
        if (presence & (1UL << i))
        {
            uint32_t bits = get32(in + 20 + 4 * i);
            memcpy(&values.value[i], &bits, sizeof(bits));
        }
        else
            values.value[i] = NAN;
    }
    return true;
}

inline uint32_t recordSequence(const uint8_t in[RECORD_SIZE]) {
    return get32(in + 5);
}

} // namespace telemetry_queue
