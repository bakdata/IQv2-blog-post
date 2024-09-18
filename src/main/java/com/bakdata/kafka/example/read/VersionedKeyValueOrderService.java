package com.bakdata.kafka.example.read;

import com.bakdata.kafka.example.StoreType;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.query.*;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.VersionedRecordIterator;

import java.time.Instant;
import java.util.*;

import static com.bakdata.kafka.example.utils.QueryHelper.queryInstance;

/**
 * Contains services for accessing the {@link org.apache.kafka.streams.state.VersionedKeyValueStore}
 */
@RequiredArgsConstructor
@Slf4j
public class VersionedKeyValueOrderService implements Service<String, Integer> {
    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();
    private final @NonNull Storage storage;

    public static VersionedKeyValueOrderService setUp(final KafkaStreams streams) {
        final String storeName = StoreType.VERSIONED_KEY_VALUE.getStoreName();
        log.info("Setting up order service for store '{}'", storeName);
        return new VersionedKeyValueOrderService(Storage.create(streams, storeName));
    }

    private static List<Integer> gatherQueryResults(final StateQueryResult<VersionedRecordIterator<Integer>> result) {
        final Map<Integer, QueryResult<VersionedRecordIterator<Integer>>> allPartitionsResult =
                result.getPartitionResults();
        final List<Integer> aggregationResult = new ArrayList<>();
        allPartitionsResult.forEach(
                (key, queryResult) ->
                        queryResult.getResult()
                                .forEachRemaining(kv -> aggregationResult.add(kv.value()))
        );
        return aggregationResult;
    }

    @Override
    public Optional<Integer> getValueForKey(final @NonNull String key) {
        throw new UnsupportedOperationException("Not supported yet");
    }

    @Override
    public List<Integer> getValuesForRange(final String lower, final String upper) {
        throw new UnsupportedOperationException("Not supported yet");
    }

    @Override
    public Optional<Integer> getVersionedValueForKey(final @NonNull String menuItem, final Instant asOf) {
        final VersionedKeyQuery<String, Integer> query = asOf == null ? VersionedKeyQuery.withKey(menuItem) :
                VersionedKeyQuery.<String, Integer>withKey(menuItem).asOf(asOf);

        final KeyQueryMetadata keyQueryMetadata = this.storage.getStreams()
                .queryMetadataForKey(this.storage.getStoreName(), menuItem, STRING_SERIALIZER);

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

    @Override
    public List<Integer> getVersionedValuesForRange(final @NonNull String key, final Instant from, final Instant to) {
        final MultiVersionedKeyQuery<String, Integer> multiVersionedKeyQuery = MultiVersionedKeyQuery
                .<String, Integer>withKey(key)
                .fromTime(from)
                .toTime(to)
                .withAscendingTimestamps();

        final Collection<StreamsMetadata> streamsMetadata =
                this.storage.getStreams()
                        .streamsMetadataForStore(this.storage.getStoreName());

        return streamsMetadata.stream()
                .findFirst()
                .map(metadata -> {
                    final StateQueryResult<VersionedRecordIterator<Integer>> stateQueryResult = queryInstance(this.storage, metadata, multiVersionedKeyQuery);
                    return gatherQueryResults(stateQueryResult);
                })
                .orElse(Collections.emptyList());
    }

    @Override
    public void close() {
        log.info("Closing versioned order service");

        this.storage.getStreams()
                .close();
    }
}
