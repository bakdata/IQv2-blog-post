package com.bakdata.kafka.example.model;

public record Price(
        int price,
        long validFrom
) {
}
