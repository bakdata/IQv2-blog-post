package com.bakdata.kafka.example.model;

public record Order(
        String customerId,
        String menuItem,
        long timestamp
) {
}
