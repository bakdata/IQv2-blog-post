package com.bakdata.kafka.example.model;

import java.time.Instant;

public record CustomerSession(
        Instant start,
        Instant end,
        long count
) {
}
