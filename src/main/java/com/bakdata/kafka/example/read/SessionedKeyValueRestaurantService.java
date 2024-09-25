package com.bakdata.kafka.example.read;

import com.bakdata.kafka.example.StoreType;
import com.bakdata.kafka.example.model.CustomerSession;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.bakdata.kafka.example.utils.QueryHelper.gatherQueryResults;
import static com.bakdata.kafka.example.utils.QueryHelper.queryInstance;


/**
 * Contains services for accessing the {@link org.apache.kafka.streams.state.SessionStore}
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class SessionedKeyValueRestaurantService implements Service<String, CustomerSession> {
    private final @NonNull Storage storage;

    public static SessionedKeyValueRestaurantService setUp(final KafkaStreams streams) {
        final String storeName = StoreType.SESSION_KEY_VALUE.getStoreName();
        log.info("Setting up order service for store '{}'", storeName);
        return new SessionedKeyValueRestaurantService(Storage.create(streams, storeName));
    }

    private static CustomerSession toCustomerSession(final KeyValue<? extends Windowed<String>, Long> kv) {
        final Window window = kv.key.window();
        return new CustomerSession(window.startTime(), window.endTime(), kv.value);
    }

    @Override
    public List<CustomerSession> getSessionRangeForKey(final @NonNull String customerId) {
        final WindowRangeQuery<String, Long> rangeQuery = WindowRangeQuery.withKey(customerId);

        final Collection<StreamsMetadata> streamsMetadata =
                this.storage.getStreams()
                        .streamsMetadataForStore(this.storage.getStoreName());

        return streamsMetadata.stream()
                .findFirst()
                .map(metadata -> this.getCustomerSessions(metadata, rangeQuery))
                .orElse(Collections.emptyList());
    }

    private List<CustomerSession> getCustomerSessions(final StreamsMetadata metadata, final Query<KeyValueIterator<Windowed<String>, Long>> rangeQuery) {
        final StateQueryResult<KeyValueIterator<Windowed<String>, Long>> stateQueryResult = queryInstance(this.storage, metadata, rangeQuery);
        return gatherQueryResults(stateQueryResult).stream()
                .map(SessionedKeyValueRestaurantService::toCustomerSession)
                .toList();
    }

    @Override
    public void close() throws Exception {
        log.info("Closing order service");

        this.storage.getStreams()
                .close();
    }
}
