package com.bakdata.kafka.example.utils;

import com.bakdata.kafka.example.model.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.util.Optional;

public class OrderTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(final ConsumerRecord<Object, Object> record, final long partitionTime) {
        final Order order = Utils.readToObject((String) record.value(), Order.class);
        return Optional.of(order.timestamp())
                .orElse(partitionTime);
    }
}
