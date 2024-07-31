package com.bakdata.kafka.example.model;

public record Promotion(
        String code,
        long endTimestamp
) {
}
