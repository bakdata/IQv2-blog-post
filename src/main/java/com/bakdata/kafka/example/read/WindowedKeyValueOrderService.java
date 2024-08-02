package com.bakdata.kafka.example.read;

import com.bakdata.kafka.example.StoreType;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Contains services for accessing the (versioned) state store
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class WindowedKeyValueOrderService implements Service<String, Integer> {
    private final @NonNull Storage storage;

    public static WindowedKeyValueOrderService setUp(final KafkaStreams streams) {
        final String storeName = StoreType.WINDOWED_KEY_VALUE.getStoreName();
        log.info("Setting up order service for store '{}'", storeName);
        return new WindowedKeyValueOrderService(Storage.create(streams, storeName));
    }

    @Override
    public List<Integer> getWindowedValueForKey(@NonNull final String menuItem, @NonNull final Instant from, @NonNull final Instant to) {
        log.debug("Querying menu item '{}'", menuItem);

        final WindowKeyQuery<String, Integer> keyQuery =
                WindowKeyQuery.withKeyAndWindowStartRange(menuItem, from, to.minusMillis(1));

        final KeyQueryMetadata keyQueryMetadata = this.storage.getStreams()
                .queryMetadataForKey(this.storage.getStoreName(), menuItem, Serdes.String().serializer());

        final StateQueryRequest<WindowStoreIterator<Integer>> queryRequest =
                this.storage.getInStore()
                        .withQuery(keyQuery)
                        .withPartitions(Collections.singleton(keyQueryMetadata.partition()))
                        .enableExecutionInfo();

        final QueryResult<WindowStoreIterator<Integer>> onlyPartitionResult = this.storage.getStreams()
                .query(queryRequest)
                .getOnlyPartitionResult();

        final List<Integer> results = new ArrayList<>();
        if (onlyPartitionResult != null && onlyPartitionResult.isSuccess()) {
            onlyPartitionResult.getResult()
                    .forEachRemaining(result -> results.add(result.value));
        }
        return results;
    }

    // TODO: implement
    @Override
    public List<Integer> getWindowedRange(@NonNull Instant from, @NonNull Instant to) {
        WindowRangeQuery<String, Integer> rangeQuery = WindowRangeQuery.withWindowStartRange(from, to);
        return List.of();
    }

    // TODO: implement
    @Override
    public List<Integer> getWindowedRangeForKey(final String key) {
        WindowRangeQuery<String, Integer> rangeQuery = WindowRangeQuery.withKey(key);
        return List.of();
    }

    @Override
    public Optional<Integer> getValueForKey(@NonNull final String menuItem) {
        throw new IllegalCallerException("Window Store can not be called for key query.");
    }

    @Override
    public List<Integer> getValuesForRange(final String lower, final String upper) {
        throw new IllegalCallerException("Window Store can not be called for range query.");
    }

    @Override
    public void close() {
        log.info("Closing order service");

        this.storage.getStreams()
                .close();
    }
}
