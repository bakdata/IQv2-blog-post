package com.bakdata.kafka.example.read;

import com.bakdata.kafka.example.StoreType;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.query.*;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Contains services for accessing the {@link org.apache.kafka.streams.state.KeyValueStore}
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class KeyValueOrderService implements Service<String, String> {
    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();
    private final @NonNull Storage storage;

    public static KeyValueOrderService setUp(final KafkaStreams streams) {
        final String storeName = StoreType.KEY_VALUE.getStoreName();
        log.info("Setting up order service for store '{}'", storeName);
        return new KeyValueOrderService(Storage.create(streams, storeName));
    }

    private static <K, V> List<V> extractStateQueryResults(final StateQueryResult<KeyValueIterator<K, V>> result) {
        final Map<Integer, QueryResult<KeyValueIterator<K, V>>> allPartitionsResult =
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
    public Optional<String> getValueForKey(final @NonNull String menuItem) {
        log.debug("Querying menuItem '{}'", menuItem);

        final KeyQuery<String, String> keyQuery = KeyQuery.withKey(menuItem);

        final KeyQueryMetadata keyQueryMetadata = this.storage.getStreams()
                .queryMetadataForKey(this.storage.getStoreName(), menuItem, STRING_SERIALIZER);

        final StateQueryRequest<String> queryRequest =
                this.storage.getInStore()
                        .withQuery(keyQuery)
                        .withPartitions(Collections.singleton(keyQueryMetadata.partition()))
                        .enableExecutionInfo();

        final QueryResult<String> onlyPartitionResult = this.storage.getStreams()
                .query(queryRequest)
                .getOnlyPartitionResult();

        if (onlyPartitionResult != null && onlyPartitionResult.isSuccess()) {
            return Optional.of(onlyPartitionResult.getResult());
        }
        return Optional.empty();
    }

    @Override
    public List<String> getValuesForRange(final String lower, final String upper) {
        final RangeQuery<String, String> rangeQuery = RangeQuery.withRange(lower, upper);

        final Collection<StreamsMetadata> streamsMetadata =
                this.storage.getStreams()
                        .streamsMetadataForStore(this.storage.getStoreName());

        return streamsMetadata.stream()
                .findFirst()
                .map(metadata -> this.queryInstance(metadata, rangeQuery))
                .orElse(Collections.emptyList());
    }

    private List<String> queryInstance(final StreamsMetadata metadata, final Query<KeyValueIterator<String, String>> rangeQuery) {
        final Set<Integer> topicPartitions = metadata.topicPartitions()
                .stream()
                .map(TopicPartition::partition)
                .collect(Collectors.toSet());

        final StateQueryRequest<KeyValueIterator<String, String>> queryRequest =
                this.storage.getInStore()
                        .withQuery(rangeQuery)
                        .withPartitions(topicPartitions)
                        .enableExecutionInfo();

        final StateQueryResult<KeyValueIterator<String, String>> stateQueryResult = this.storage.getStreams()
                .query(queryRequest);

        return extractStateQueryResults(stateQueryResult);
    }

    @Override
    public void close() {
        log.info("Closing order service");

        this.storage.getStreams()
                .close();
    }
}
