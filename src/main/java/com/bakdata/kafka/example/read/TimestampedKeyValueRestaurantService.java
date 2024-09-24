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
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.TimestampedKeyQuery;
import org.apache.kafka.streams.query.TimestampedRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.bakdata.kafka.example.utils.QueryHelper.*;

/**
 * Contains services for accessing the {@link org.apache.kafka.streams.state.TimestampedKeyValueStore}
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class TimestampedKeyValueRestaurantService implements Service<String, ValueAndTimestamp<String>> {
    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();
    private final @NonNull Storage storage;

    public static TimestampedKeyValueRestaurantService setUp(final KafkaStreams streams) {
        final String storeName = StoreType.TIMESTAMPED_KEY_VALUE.getStoreName();
        log.info("Setting up order service for store '{}'", storeName);
        return new TimestampedKeyValueRestaurantService(Storage.create(streams, storeName));
    }

    @Override
    public Optional<ValueAndTimestamp<String>> getValueForKey(final @NonNull String promotionCode) {
        log.debug("Querying key '{}'", promotionCode);

        final TimestampedKeyQuery<String, String> keyQuery = TimestampedKeyQuery.withKey(promotionCode);

        final KeyQueryMetadata keyQueryMetadata = this.storage.getStreams()
                .queryMetadataForKey(this.storage.getStoreName(), promotionCode, STRING_SERIALIZER);

        final StateQueryRequest<ValueAndTimestamp<String>> queryRequest =
                this.storage.getInStore()
                        .withQuery(keyQuery)
                        .withPartitions(Collections.singleton(keyQueryMetadata.partition()))
                        .enableExecutionInfo();

        final QueryResult<ValueAndTimestamp<String>> onlyPartitionResult = this.storage.getStreams()
                .query(queryRequest)
                .getOnlyPartitionResult();

        return getQueryResults(onlyPartitionResult);
    }

    @Override
    public List<ValueAndTimestamp<String>> getValuesForRange(final String lower, final String upper) {
        final TimestampedRangeQuery<String, String> rangeQuery = TimestampedRangeQuery.withRange(lower, upper);

        final Collection<StreamsMetadata> streamsMetadata =
                this.storage.getStreams()
                        .streamsMetadataForStore(this.storage.getStoreName());

        return streamsMetadata.stream()
                .findFirst()
                .map(metadata -> {
                    final StateQueryResult<KeyValueIterator<String, ValueAndTimestamp<String>>> stateQueryResult = queryInstance(this.storage, metadata, rangeQuery);
                    return gatherQueryResults(stateQueryResult).stream()
                            .map(kv -> kv.value)
                            .toList();
                })
                .orElse(Collections.emptyList());
    }


    @Override
    public void close() {
        log.info("Closing order service");

        this.storage.getStreams()
                .close();
    }
}
