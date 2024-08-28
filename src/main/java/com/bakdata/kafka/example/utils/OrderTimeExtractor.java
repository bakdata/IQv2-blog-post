package com.bakdata.kafka.example.utils;

import com.bakdata.kafka.example.model.Order;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.util.Optional;

public class OrderTimeExtractor implements TimestampExtractor {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public long extract(final ConsumerRecord<Object, Object> record, final long partitionTime) {
        try {
            final Order order = MAPPER.readValue((String) record.value(), Order.class);
            return Optional.of(order.timestamp())
                    .orElse(partitionTime);
        } catch (final JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
