package com.bakdata.kafka.example.utils;

import lombok.experimental.UtilityClass;

import java.time.Instant;
import java.time.LocalDateTime;

import static java.time.ZoneOffset.UTC;

@UtilityClass
public class Utils {
    public LocalDateTime toLocalDateTime(final Long timestamp) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), UTC);
    }
}
