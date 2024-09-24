package com.bakdata.kafka.example.read;

import com.bakdata.kafka.example.StoreType;
import com.bakdata.kafka.example.utils.QueryHelper;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.bakdata.kafka.example.utils.QueryHelper.queryInstance;

/**
 * Contains services for accessing the {@link org.apache.kafka.streams.state.KeyValueStore}
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class KeyValueRestaurantService implements Service<String, String> {
    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();
    private final @NonNull Storage storage;

    public static KeyValueRestaurantService setUp(final KafkaStreams streams) {
        final String storeName = StoreType.KEY_VALUE.getStoreName();
        log.info("Setting up order service for store '{}'", storeName);
        return new KeyValueRestaurantService(Storage.create(streams, storeName));
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

        return QueryHelper.getQueryResults(onlyPartitionResult);
    }

    @Override
    public List<String> getValuesForRange(final String lower, final String upper) {
        final Query<KeyValueIterator<String, String>> rangeQuery = RangeQuery.withRange(lower, upper);

        final Collection<StreamsMetadata> streamsMetadata =
                this.storage.getStreams()
                        .streamsMetadataForStore(this.storage.getStoreName());

        return streamsMetadata.stream()
                .findFirst()
                .map(metadata -> {
                    final StateQueryResult<KeyValueIterator<String, String>> stateQueryResult = queryInstance(this.storage, metadata, rangeQuery);
                    return QueryHelper.gatherQueryResults(stateQueryResult)
                            .stream()
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
