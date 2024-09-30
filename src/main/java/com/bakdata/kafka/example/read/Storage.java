package com.bakdata.kafka.example.read;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.query.StateQueryRequest;

import static org.apache.kafka.streams.query.StateQueryRequest.inStore;

/**
 * Storage class that encapsulates access to a Kafka Streams instance, a store name, and an in-store query
 * configuration. It provides a factory method to create instances of the class with a private constructor.
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class Storage {
    private final @NonNull KafkaStreams streams;
    private final @NonNull String storeName;
    private final @NonNull StateQueryRequest.InStore inStore;

    public static Storage create(final KafkaStreams streams, final String storeName) {
        return new Storage(streams, storeName, inStore(storeName));
    }
}
