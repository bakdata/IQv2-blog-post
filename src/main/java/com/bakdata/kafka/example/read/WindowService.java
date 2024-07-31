package com.bakdata.kafka.example.read;

import lombok.NonNull;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public interface WindowService<K, V> extends Kirekhar<K, V> {
    Optional<V> getWindowedValueForKey(final @NonNull K key, Instant from, Instant to);
}
