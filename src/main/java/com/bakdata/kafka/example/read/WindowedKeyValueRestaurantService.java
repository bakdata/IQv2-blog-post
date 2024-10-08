package com.bakdata.kafka.example.read;

import com.bakdata.kafka.example.StoreType;
import com.bakdata.kafka.example.utils.QueryHelper;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.bakdata.kafka.example.utils.QueryHelper.gatherQueryResults;
import static com.bakdata.kafka.example.utils.QueryHelper.queryInstance;

/**
 * Contains services for accessing the {@link org.apache.kafka.streams.state.WindowStore}
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class WindowedKeyValueRestaurantService implements Service<String, Long> {
    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();
    private final @NonNull Storage storage;

    public static WindowedKeyValueRestaurantService setUp(final KafkaStreams streams) {
        final String storeName = StoreType.WINDOWED_KEY_VALUE.getStoreName();
        log.info("Setting up order service for store '{}'", storeName);
        return new WindowedKeyValueRestaurantService(Storage.create(streams, storeName));
    }

    @Override
    public List<Long> getWindowedValueForKey(final @NonNull String menuItem, final @NonNull Instant from, final @NonNull Instant to) {
        log.debug("Querying order count of '{}' from '{}' to '{}'", menuItem, from.toEpochMilli(), to.toEpochMilli());

        final WindowKeyQuery<String, ValueAndTimestamp<Long>> keyQuery =
                WindowKeyQuery.withKeyAndWindowStartRange(menuItem, from, to.minusMillis(1));

        final KeyQueryMetadata keyQueryMetadata = this.storage.getStreams()
                .queryMetadataForKey(this.storage.getStoreName(), menuItem, STRING_SERIALIZER);

        final StateQueryRequest<WindowStoreIterator<ValueAndTimestamp<Long>>> queryRequest =
                this.storage.getInStore()
                        .withQuery(keyQuery)
                        .withPartitions(Collections.singleton(keyQueryMetadata.partition()))
                        .enableExecutionInfo();

        final QueryResult<WindowStoreIterator<ValueAndTimestamp<Long>>> onlyPartitionResult = this.storage.getStreams()
                .query(queryRequest)
                .getOnlyPartitionResult();

        final List<Long> results = new ArrayList<>();
        if (onlyPartitionResult != null && onlyPartitionResult.isSuccess()) {
            onlyPartitionResult.getResult()
                    .forEachRemaining(result -> results.add(result.value.value()));
        }

        return results;
    }

    @Override
    public List<Long> getWindowedRange(final @NonNull Instant from, final @NonNull Instant to) {
        log.debug("Querying range from '{}' to '{}'", from, to);

        final WindowRangeQuery<String, ValueAndTimestamp<Long>> rangeQuery = WindowRangeQuery.withWindowStartRange(from, to);

        final Collection<StreamsMetadata> streamsMetadata =
                this.storage.getStreams()
                        .streamsMetadataForStore(this.storage.getStoreName());

        return QueryHelper.executeQuery(streamsMetadata, metadata -> this.windowCounts(metadata, rangeQuery));
    }

    private List<Long> windowCounts(final StreamsMetadata metadata, final WindowRangeQuery<String, ValueAndTimestamp<Long>> rangeQuery) {
        final StateQueryResult<KeyValueIterator<Windowed<String>, ValueAndTimestamp<Long>>> stateQueryResult = queryInstance(this.storage, metadata, rangeQuery);
        return gatherQueryResults(stateQueryResult).stream()
                .map(kv -> kv.value.value())
                .toList();
    }

    @Override
    public void close() {
        log.info("Closing order service");

        this.storage.getStreams()
                .close();
    }
}
