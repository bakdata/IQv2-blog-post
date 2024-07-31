package com.bakdata.kafka.example.read;

import com.bakdata.kafka.example.StoreType;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;
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
    public Optional<Integer> getValueForKey(@NonNull final String menuItem) {
        log.debug("Querying menueItem '{}'", menuItem);

        final WindowKeyQuery<String, Integer> keyQuery =
                WindowKeyQuery.withKeyAndWindowStartRange(menuItem, Instant.ofEpochMilli(0), Instant.ofEpochMilli(7200000));

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

        if (onlyPartitionResult != null && onlyPartitionResult.isSuccess() && onlyPartitionResult.getResult().hasNext()) {
            final KeyValue<Long, Integer> next = onlyPartitionResult.getResult().next();
            final int value = next.value;
            return Optional.of(value);
        }
        return Optional.empty();
    }

    @Override
    public List<Integer> getValuesForRange(final String lower, final String upper) {
        final RangeQuery<String, String> rangeQuery = RangeQuery.withRange(lower, upper);
        return List.of();
    }

    @Override
    public void close() {
        log.info("Closing order service");

        this.storage.getStreams()
                .close();
    }
}
