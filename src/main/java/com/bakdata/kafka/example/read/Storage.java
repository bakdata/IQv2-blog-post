package com.bakdata.kafka.example.read;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.query.StateQueryRequest;

import static org.apache.kafka.streams.query.StateQueryRequest.inStore;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public final class Storage {
    private final @NonNull KafkaStreams streams;
    private final @NonNull String storeName;
    private final @NonNull StateQueryRequest.InStore inStore;

    public static Storage create(final KafkaStreams streams, final String storeName) {
        return new Storage(streams, storeName, inStore(storeName));
    }
}
