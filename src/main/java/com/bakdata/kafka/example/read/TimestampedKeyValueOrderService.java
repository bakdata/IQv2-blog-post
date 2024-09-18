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
import org.apache.kafka.streams.query.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.*;

import static com.bakdata.kafka.example.utils.QueryHelper.gatherQueryResults;
import static com.bakdata.kafka.example.utils.QueryHelper.queryInstance;

/**
 * Contains services for accessing the {@link org.apache.kafka.streams.state.TimestampedKeyValueStore}
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class TimestampedKeyValueOrderService implements Service<String, ValueAndTimestamp<String>> {
    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();
    private final @NonNull Storage storage;

    public static TimestampedKeyValueOrderService setUp(final KafkaStreams streams) {
        final String storeName = StoreType.TIMESTAMPED_KEY_VALUE.getStoreName();
        log.info("Setting up order service for store '{}'", storeName);
        return new TimestampedKeyValueOrderService(Storage.create(streams, storeName));
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

        if (onlyPartitionResult != null && onlyPartitionResult.isSuccess()) {
            return Optional.of(onlyPartitionResult.getResult());
        }
        return Optional.empty();
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
                    return gatherQueryResults(stateQueryResult);
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
