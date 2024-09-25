package com.bakdata.kafka.example.read;

import com.bakdata.kafka.example.StoreType;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.query.MultiVersionedKeyQuery;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.VersionedKeyQuery;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.VersionedRecordIterator;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.bakdata.kafka.example.utils.QueryHelper.*;

/**
 * Contains services for accessing the {@link org.apache.kafka.streams.state.VersionedKeyValueStore}
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class VersionedKeyValueRestaurantService implements Service<String, Integer> {
    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();
    private final @NonNull Storage storage;

    public static VersionedKeyValueRestaurantService setUp(final KafkaStreams streams) {
        final String storeName = StoreType.VERSIONED_KEY_VALUE.getStoreName();
        log.info("Setting up order service for store '{}'", storeName);
        return new VersionedKeyValueRestaurantService(Storage.create(streams, storeName));
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

        final QueryResult<VersionedRecord<Integer>> queryResult = this.storage.getStreams()
                .query(queryRequest)
                .getOnlyPartitionResult();

        return getQueryResults(queryResult)
                .map(VersionedRecord::value);
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
                    return gatherQueryResults(stateQueryResult).stream()
                            .map(VersionedRecord::value)
                            .toList();
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
