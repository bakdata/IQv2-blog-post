package com.bakdata.kafka.example.utils;

import com.bakdata.kafka.example.read.Storage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.experimental.UtilityClass;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.stream.Collectors;

import static java.time.ZoneOffset.UTC;

@UtilityClass
public class Utils {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public LocalDateTime toLocalDateTime(final Long timestamp) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), UTC);
    }

    public <T> T readToObject(final String jsonString, final Class<T> clazz) {
        try {
            return MAPPER.readValue(jsonString, clazz);
        } catch (final JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
