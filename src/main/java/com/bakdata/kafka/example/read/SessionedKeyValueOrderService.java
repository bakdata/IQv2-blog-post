package com.bakdata.kafka.example.read;

import com.bakdata.kafka.example.StoreType;
import com.bakdata.kafka.example.model.CustomerSession;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.*;

import static com.bakdata.kafka.example.utils.Utils.queryInstance;

/**
 * Contains services for accessing the {@link org.apache.kafka.streams.state.SessionStore}
 */
@RequiredArgsConstructor
@Slf4j
public class SessionedKeyValueOrderService implements Service<String, CustomerSession> {
    private final @NonNull Storage storage;

    public static SessionedKeyValueOrderService setUp(final KafkaStreams streams) {
        final String storeName = StoreType.SESSION_KEY_VALUE.getStoreName();
        log.info("Setting up order service for store '{}'", storeName);
        return new SessionedKeyValueOrderService(Storage.create(streams, storeName));
    }

    private static <K> List<CustomerSession> extractStateQueryResults(final StateQueryResult<KeyValueIterator<Windowed<K>, Long>> result) {
        final Map<Integer, QueryResult<KeyValueIterator<Windowed<K>, Long>>> allPartitionsResult =
                result.getPartitionResults();
        final List<CustomerSession> aggregationResult = new ArrayList<>();
        allPartitionsResult.forEach(
                (key, queryResult) ->
                        queryResult.getResult()
                                .forEachRemaining(kv -> {
                                    final Window window = kv.key.window();
                                    aggregationResult.add(new CustomerSession(window.startTime(), window.endTime(), kv.value));
                                })
        );
        return aggregationResult;
    }

    @Override
    public List<CustomerSession> getSessionRangeForKey(final @NonNull String customerId) {
        final WindowRangeQuery<String, Long> rangeQuery = WindowRangeQuery.withKey(customerId);

        final Collection<StreamsMetadata> streamsMetadata =
                this.storage.getStreams()
                        .streamsMetadataForStore(this.storage.getStoreName());

        return streamsMetadata.stream()
                .findFirst()
                .map(metadata -> {
                    final StateQueryResult<KeyValueIterator<Windowed<String>, Long>> stateQueryResult = queryInstance(this.storage, metadata, rangeQuery);
                    return extractStateQueryResults(stateQueryResult);
                })
                .orElse(Collections.emptyList());
    }

    @Override
    public void close() throws Exception {
        log.info("Closing order service");

        this.storage.getStreams()
                .close();
    }
}
