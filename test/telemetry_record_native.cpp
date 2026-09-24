#include <assert.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>

#include "telemetry_record.h"

int main() {
    telemetry_queue::Values input = {};
    for (uint8_t i = 0; i < 20; ++i)
        input.value[i] = static_cast<float>(i) * 1.25f;
    input.value[7] = NAN;

    uint8_t encoded[telemetry_queue::RECORD_SIZE];
    telemetry_queue::encodeRecord(encoded, 0x12345678u,
                                  0x123456789abcdef0ULL, input);

    uint64_t timestamp = 0;
    telemetry_queue::Values decoded = {};
    assert(telemetry_queue::decodeRecord(encoded, 0x12345678u,
                                         timestamp, decoded));
    assert(timestamp == 0x123456789abcdef0ULL);
    for (uint8_t i = 0; i < 20; ++i) {
        if (i == 7)
            assert(isnan(decoded.value[i]));
        else
            assert(decoded.value[i] == input.value[i]);
    }

    assert(!telemetry_queue::decodeRecord(encoded, 1u, timestamp, decoded));
    encoded[40] ^= 1u;
    assert(!telemetry_queue::decodeRecord(encoded, 0x12345678u,
                                          timestamp, decoded));
    puts("telemetry_record native tests passed");
    return 0;
}