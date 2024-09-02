package com.bakdata.kafka.example.read;

import com.bakdata.kafka.example.StoreType;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.query.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Contains services for accessing the (versioned) state store
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class WindowedKeyValueOrderService implements Service<String, Long> {
    private final @NonNull Storage storage;

    public static WindowedKeyValueOrderService setUp(final KafkaStreams streams) {
        final String storeName = StoreType.WINDOWED_KEY_VALUE.getStoreName();
        log.info("Setting up order service for store '{}'", storeName);
        return new WindowedKeyValueOrderService(Storage.create(streams, storeName));
    }

    private static <K, V> List<V> extractStateQueryResults(final StateQueryResult<KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>>> result) {
        final Map<Integer, QueryResult<KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>>>> allPartitionsResult =
                result.getPartitionResults();
        final List<V> aggregationResult = new ArrayList<>();
        allPartitionsResult.forEach(
                (key, queryResult) ->
                        queryResult.getResult()
                                .forEachRemaining(kv -> aggregationResult.add(kv.value.value()))
        );
        return aggregationResult;
    }

    @Override
    public List<Long> getWindowedValueForKey(final @NonNull String menuItem, final @NonNull Instant from, final @NonNull Instant to) {
        log.debug("Querying order count of '{}' from '{}' to '{}'", menuItem, from.toEpochMilli(), to.toEpochMilli());

        final WindowKeyQuery<String, ValueAndTimestamp<Long>> keyQuery =
                WindowKeyQuery.withKeyAndWindowStartRange(menuItem, from, to.minusMillis(1));

        final KeyQueryMetadata keyQueryMetadata = this.storage.getStreams()
                .queryMetadataForKey(this.storage.getStoreName(), menuItem, Serdes.String().serializer());

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

        final List<Long> results = new ArrayList<>();

        final Collection<StreamsMetadata> streamsMetadata =
                this.storage.getStreams()
                        .streamsMetadataForStore(this.storage.getStoreName());

        for (final StreamsMetadata metadata : streamsMetadata) {
            final Set<Integer> topicPartitions = metadata.topicPartitions()
                    .stream()
                    .map(TopicPartition::partition)
                    .collect(Collectors.toSet());

            final StateQueryRequest<KeyValueIterator<Windowed<String>, ValueAndTimestamp<Long>>> queryRequest =
                    this.storage.getInStore()
                            .withQuery(rangeQuery)
                            .withPartitions(topicPartitions)
                            .enableExecutionInfo();

            final StateQueryResult<KeyValueIterator<Windowed<String>, ValueAndTimestamp<Long>>> stateQueryResult =
                    this.storage.getStreams()
                            .query(queryRequest);

            results.addAll(extractStateQueryResults(stateQueryResult));
        }

        return results;
    }

    @Override
    public Optional<Long> getValueForKey(final @NonNull String menuItem) {
        throw new IllegalCallerException("Window Store can not be called for key query.");
    }

    @Override
    public List<Long> getValuesForRange(final String lower, final String upper) {
        throw new IllegalCallerException("Window Store can not be called for range query.");
    }

    @Override
    public void close() {
        log.info("Closing order service");

        this.storage.getStreams()
                .close();
    }
}
