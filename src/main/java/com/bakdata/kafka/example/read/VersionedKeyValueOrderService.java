package com.bakdata.kafka.example.read;

import com.bakdata.kafka.example.StoreType;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.query.*;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.VersionedRecordIterator;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Slf4j
public class VersionedKeyValueOrderService implements Service<String, Integer> {
    private final @NonNull Storage storage;

    public static VersionedKeyValueOrderService setUp(final KafkaStreams streams) {
        final String storeName = StoreType.VERSIONED_KEY_VALUE.getStoreName();
        log.info("Setting up order service for store '{}'", storeName);
        return new VersionedKeyValueOrderService(Storage.create(streams, storeName));
    }

    private static <V> List<V> extractStateQueryResults(final StateQueryResult<VersionedRecordIterator<V>> result) {
        final Map<Integer, QueryResult<VersionedRecordIterator<V>>> allPartitionsResult =
                result.getPartitionResults();
        final List<V> aggregationResult = new ArrayList<>();
        allPartitionsResult.forEach(
                (key, queryResult) ->
                        queryResult.getResult()
                                .forEachRemaining(kv -> aggregationResult.add(kv.value()))
        );
        return aggregationResult;
    }

    @Override
    public Optional<Integer> getValueForKey(@NonNull final String key) {
        throw new UnsupportedOperationException("Not supported yet");
    }

    @Override
    public List<Integer> getValuesForRange(final String lower, final String upper) {
        throw new UnsupportedOperationException("Not supported yet");
    }

    @Override
    public Optional<Integer> getVersionedValueForKey(@NonNull final String menuItem, final Instant asOf) {
        final VersionedKeyQuery<String, Integer> query = asOf == null ? VersionedKeyQuery.withKey(menuItem) :
                VersionedKeyQuery.<String, Integer>withKey(menuItem).asOf(asOf);

        final KeyQueryMetadata keyQueryMetadata = this.storage.getStreams()
                .queryMetadataForKey(this.storage.getStoreName(), menuItem, Serdes.String().serializer());

        final StateQueryRequest<VersionedRecord<Integer>> queryRequest = this.storage.getInStore()
                .withQuery(query)
                .withPartitions(Collections.singleton(keyQueryMetadata.partition()))
                .enableExecutionInfo();

        final QueryResult<VersionedRecord<Integer>> onlyPartitionResult = this.storage.getStreams()
                .query(queryRequest)
                .getOnlyPartitionResult();

        if (onlyPartitionResult != null && onlyPartitionResult.isSuccess()) {
            return Optional.of(onlyPartitionResult.getResult().value());
        }
        return Optional.empty();
    }

    // TODO: Order is ascending timestamp do we need to change or configurable it?
    @Override
    public List<Integer> getVersionedValuesForRange(@NonNull final String key, final Instant from, final Instant to) {
        final MultiVersionedKeyQuery<String, Integer> multiVersionedKeyQuery = MultiVersionedKeyQuery
                .<String, Integer>withKey(key)
                .fromTime(from)
                .toTime(to)
                .withAscendingTimestamps();

        final List<Integer> results = new ArrayList<>();

        final Collection<StreamsMetadata> streamsMetadata =
                this.storage.getStreams()
                        .streamsMetadataForStore(this.storage.getStoreName());

        for (final StreamsMetadata metadata : streamsMetadata) {
            final Set<Integer> topicPartitions = metadata.topicPartitions()
                    .stream()
                    .map(TopicPartition::partition)
                    .collect(Collectors.toSet());

            final StateQueryRequest<VersionedRecordIterator<Integer>> queryRequest =
                    this.storage.getInStore()
                            .withQuery(multiVersionedKeyQuery)
                            .withPartitions(topicPartitions)
                            .enableExecutionInfo();

            final StateQueryResult<VersionedRecordIterator<Integer>> stateQueryResult = this.storage.getStreams()
                    .query(queryRequest);

            results.addAll(extractStateQueryResults(stateQueryResult));
        }

        return results;
    }

    @Override
    public void close() {
        log.info("Closing versioned order service");

        this.storage.getStreams()
                .close();
    }
}
