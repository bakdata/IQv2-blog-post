package com.bakdata.kafka.example.read;

import lombok.NonNull;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@FunctionalInterface
public interface Service<K, V> extends AutoCloseable {
    default Optional<V> getValueForKey(final @NonNull K key) {
        return Optional.empty();
    }

    default List<V> getValuesForRange(final K lower, final K upper) {
        return Collections.emptyList();
    }

    default Optional<V> getVersionedValueForKey(final @NonNull K key, final Instant asOfTime) {
        return Optional.empty();
    }

    default List<V> getVersionedValuesForRange(final @NonNull K key, final Instant from, final Instant to) {
        return Collections.emptyList();
    }

    default List<V> getWindowedValueForKey(final @NonNull K key, final @NonNull Instant from, final @NonNull Instant to) {
        return Collections.emptyList();
    }

    default List<V> getWindowedRange(final @NonNull Instant from, final @NonNull Instant to) {
        return Collections.emptyList();
    }

    default List<V> getSessionRangeForKey(final @NonNull K key) {
        return Collections.emptyList();
    }

}
