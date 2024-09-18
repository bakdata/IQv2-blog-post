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
import org.apache.kafka.streams.query.*;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.*;

import static com.bakdata.kafka.example.utils.QueryHelper.queryInstance;

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

    private static List<String> gatherQueryResults(final StateQueryResult<KeyValueIterator<String, String>> result) {
        final Map<Integer, QueryResult<KeyValueIterator<String, String>>> allPartitionsResult =
                result.getPartitionResults();
        final List<String> aggregationResult = new ArrayList<>();
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
        final Query<KeyValueIterator<String, String>> rangeQuery = RangeQuery.withRange(lower, upper);

        final Collection<StreamsMetadata> streamsMetadata =
                this.storage.getStreams()
                        .streamsMetadataForStore(this.storage.getStoreName());

        return streamsMetadata.stream()
                .findFirst()
                .map(metadata -> {
                    final StateQueryResult<KeyValueIterator<String, String>> stateQueryResult = queryInstance(this.storage, metadata, rangeQuery);
                    return QueryHelper.gatherQueryResults(stateQueryResult);
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
