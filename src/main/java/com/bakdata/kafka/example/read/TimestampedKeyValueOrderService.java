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
import org.apache.kafka.streams.query.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Contains services for accessing the (versioned) state store
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class TimestampedKeyValueOrderService implements Service<String, ValueAndTimestamp<String>> {
    private final @NonNull Storage storage;

    public static TimestampedKeyValueOrderService setUp(final KafkaStreams streams) {
        final String storeName = StoreType.TIMESTAMPED_KEY_VALUE.getStoreName();
        log.info("Setting up order service for store '{}'", storeName);
        return new TimestampedKeyValueOrderService(Storage.create(streams, storeName));
    }

    /**
     * KeyQuery and Range Query are similar to TimestampedKeyQuery and TimestampedRangeQuery
     * The difference is the TimestampedKeyQuery returns the ValueAndTimestamp<V> but KeyQuery returns only <V>
     */

    @Override
    public Optional<ValueAndTimestamp<String>> getValueForKey(@NonNull final String key) {
        log.debug("Querying key '{}'", key);

        final TimestampedKeyQuery<String, String> keyQuery = TimestampedKeyQuery.withKey(key);

        final KeyQueryMetadata keyQueryMetadata = this.storage.getStreams()
                .queryMetadataForKey(this.storage.getStoreName(), key, Serdes.String().serializer());

        final StateQueryRequest<ValueAndTimestamp<String>> queryRequest =
                this.storage.getInStore()
                        .withQuery(keyQuery)
                        .withPartitions(Collections.singleton(keyQueryMetadata.partition()))
                        .enableExecutionInfo();

        final QueryResult<ValueAndTimestamp<String>> onlyPartitionResult = this.storage.getStreams()
                .query(queryRequest)
                .getOnlyPartitionResult();

        if (onlyPartitionResult != null && onlyPartitionResult.isSuccess()) {
            return Optional.of(onlyPartitionResult.getResult());
        }
        return Optional.empty();
    }

    @Override
    public List<ValueAndTimestamp<String>> getValuesForRange(final String lower, final String upper) {
        final TimestampedRangeQuery<String, String> rangeQuery = TimestampedRangeQuery.withRange(lower, upper);
        final List<ValueAndTimestamp<String>> results = new ArrayList<>();

        final Collection<StreamsMetadata> streamsMetadata =
                this.storage.getStreams()
                        .streamsMetadataForStore(this.storage.getStoreName());

        for (final StreamsMetadata metadata : streamsMetadata) {
            final Set<Integer> topicPartitions = metadata.topicPartitions()
                    .stream()
                    .map(TopicPartition::partition)
                    .collect(Collectors.toSet());

            final StateQueryRequest<KeyValueIterator<String, ValueAndTimestamp<String>>> queryRequest =
                    this.storage.getInStore()
                            .withQuery(rangeQuery)
                            .withPartitions(topicPartitions)
                            .enableExecutionInfo();

            final StateQueryResult<KeyValueIterator<String, ValueAndTimestamp<String>>> stateQueryResult = this.storage.getStreams()
                    .query(queryRequest);

            results.addAll(extractStateQueryResults(stateQueryResult));
        }
        return results;
    }

    private static <K, V> List<ValueAndTimestamp<V>> extractStateQueryResults(final StateQueryResult<KeyValueIterator<K, ValueAndTimestamp<V>>> result) {
        final Map<Integer, QueryResult<KeyValueIterator<K, ValueAndTimestamp<V>>>> allPartitionsResult =
                result.getPartitionResults();
        final List<ValueAndTimestamp<V>> aggregationResult = new ArrayList<>();
        allPartitionsResult.forEach(
                (key, queryResult) ->
                        queryResult.getResult()
                                .forEachRemaining(kv -> aggregationResult.add(kv.value))
        );
        return aggregationResult;
    }


    @Override
    public void close() {
        log.info("Closing order service");

        this.storage.getStreams()
                .close();
    }
}
