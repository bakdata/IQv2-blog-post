package com.bakdata.kafka.example.read;

import com.bakdata.kafka.example.StoreType;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.*;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Slf4j
public class SessionedKeyValueOrderService implements Service<String, Long> {
    private final @NonNull Storage storage;

    public static SessionedKeyValueOrderService setUp(final KafkaStreams streams) {
        final String storeName = StoreType.SESSION_KEY_VALUE.getStoreName();
        log.info("Setting up order service for store '{}'", storeName);
        return new SessionedKeyValueOrderService(Storage.create(streams, storeName));
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
    public List<Long> getSessionRangeForKey(final @NonNull String menuItem) {
        final WindowRangeQuery<String, Long> rangeQuery = WindowRangeQuery.withKey(menuItem);
        final List<Long> results = new ArrayList<>();

        final Collection<StreamsMetadata> streamsMetadata =
                this.storage.getStreams()
                        .streamsMetadataForStore(this.storage.getStoreName());

        for (final StreamsMetadata metadata : streamsMetadata) {
            final Set<Integer> topicPartitions = metadata.topicPartitions()
                    .stream()
                    .map(TopicPartition::partition)
                    .collect(Collectors.toSet());

            final StateQueryRequest<KeyValueIterator<Windowed<String>, Long>> queryRequest =
                    this.storage.getInStore()
                            .withQuery(rangeQuery)
                            .withPartitions(topicPartitions)
                            .enableExecutionInfo();

            final StateQueryResult<KeyValueIterator<Windowed<String>, Long>> stateQueryResult =
                    this.storage.getStreams()
                            .query(queryRequest);

            results.addAll(extractStateQueryResults(stateQueryResult));
        }

        return results;
    }

    @Override
    public void close() throws Exception {
        log.info("Closing order service");

        this.storage.getStreams()
                .close();
    }

    @Override
    public Optional<Long> getValueForKey(@NonNull final String key) {
        return Optional.empty();
    }

    @Override
    public List<Long> getValuesForRange(final String lower, final String upper) {
        return List.of();
    }

}
