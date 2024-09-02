package com.bakdata.kafka.example.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.experimental.UtilityClass;

import java.time.Instant;
import java.time.LocalDateTime;

import static java.time.ZoneOffset.UTC;

@UtilityClass
public class Utils {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public LocalDateTime toLocalDateTime(final Long timestamp) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), UTC);
    }

    public <T> T readToObject(String jsonString, Class<T> clazz) {
        try {
            return MAPPER.readValue(jsonString, clazz);
        } catch (final JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
