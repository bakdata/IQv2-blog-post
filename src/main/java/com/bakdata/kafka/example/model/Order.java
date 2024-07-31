package com.bakdata.kafka.example.model;

public record Order(
        String menuItem,
        long timestamp
) {
}
