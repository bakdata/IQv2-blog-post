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
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;


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

    private static <K, V> List<V> extractStateQueryResults(final StateQueryResult<KeyValueIterator<Windowed<K>, V>> result) {
        final Map<Integer, QueryResult<KeyValueIterator<Windowed<K>, V>>> allPartitionsResult =
                result.getPartitionResults();
        final List<V> aggregationResult = new ArrayList<>();
        allPartitionsResult.forEach(
                (key, queryResult) ->
                        queryResult.getResult()
                                .forEachRemaining(kv -> aggregationResult.add(kv.value))
        );
        return aggregationResult;
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

    @Override
    public List<Integer> getWindowedRange(@NonNull final Instant from, @NonNull final Instant to) {
        final WindowRangeQuery<String, Integer> rangeQuery = WindowRangeQuery.withWindowStartRange(from, to);

        final List<Integer> results = new ArrayList<>();

        final Collection<StreamsMetadata> streamsMetadata =
                this.storage.getStreams()
                        .streamsMetadataForStore(this.storage.getStoreName());

        for (final StreamsMetadata metadata : streamsMetadata) {
            final Set<Integer> topicPartitions = metadata.topicPartitions()
                    .stream()
                    .map(TopicPartition::partition)
                    .collect(Collectors.toSet());

            final StateQueryRequest<KeyValueIterator<Windowed<String>, Integer>> queryRequest =
                    this.storage.getInStore()
                            .withQuery(rangeQuery)
                            .withPartitions(topicPartitions)
                            .enableExecutionInfo();

            final StateQueryResult<KeyValueIterator<Windowed<String>, Integer>> stateQueryResult =
                    this.storage.getStreams()
                            .query(queryRequest);

            results.addAll(extractStateQueryResults(stateQueryResult));
        }

        return results;
    }

    // TODO: Not supported!
    // https://github.com/apache/kafka/blob/0eaaff88cf68bc2c24d4874ff9bc1cc2b493c24b/streams/src/main/java/org/apache/kafka/streams/state/internals/MeteredWindowStore.java#L464C25-L464C91
    @Override
    public List<Integer> getWindowedRangeForKey(@NonNull final String menuItem) {
        final WindowRangeQuery<String, Integer> rangeQuery = WindowRangeQuery.withKey(menuItem);
        final List<Integer> results = new ArrayList<>();

        final Collection<StreamsMetadata> streamsMetadata =
                this.storage.getStreams()
                        .streamsMetadataForStore(this.storage.getStoreName());

        for (final StreamsMetadata metadata : streamsMetadata) {
            final Set<Integer> topicPartitions = metadata.topicPartitions()
                    .stream()
                    .map(TopicPartition::partition)
                    .collect(Collectors.toSet());

            final StateQueryRequest<KeyValueIterator<Windowed<String>, Integer>> queryRequest =
                    this.storage.getInStore()
                            .withQuery(rangeQuery)
                            .withPartitions(topicPartitions)
                            .enableExecutionInfo();

            final StateQueryResult<KeyValueIterator<Windowed<String>, Integer>> stateQueryResult =
                    this.storage.getStreams()
                            .query(queryRequest);

            results.addAll(extractStateQueryResults(stateQueryResult));
        }

        return results;
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
