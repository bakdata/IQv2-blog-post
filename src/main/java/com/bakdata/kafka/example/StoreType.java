package com.bakdata.kafka.example;


import com.bakdata.kafka.example.model.Order;
import com.bakdata.kafka.example.read.KeyValueRestaurantService;
import com.bakdata.kafka.example.read.Service;
import com.bakdata.kafka.example.read.SessionedKeyValueRestaurantService;
import com.bakdata.kafka.example.read.TimestampedKeyValueRestaurantService;
import com.bakdata.kafka.example.read.VersionedKeyValueRestaurantService;
import com.bakdata.kafka.example.read.WindowedKeyValueRestaurantService;
import com.bakdata.kafka.example.utils.OrderTimeExtractor;
import com.bakdata.kafka.example.utils.Utils;
import com.bakdata.kafka.example.write.WriteTimestampedKeyValueDataProcessorSupplier;
import com.bakdata.kafka.example.write.WriteVersionedKeyValueDataProcessorSupplier;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;

import static com.bakdata.kafka.example.RestaurantManagmentApplication.MENU_ITEM_DESCRIPTION_TOPIC;

/**
 * {@code StoreType} is an enum representing different types of state stores that can be used in a Kafka Streams application.
 * Each store type provides its own implementation for setting up the query service and building the stream topology.
 *
 * <p>This enum defines multiple store types such as {@link org.apache.kafka.streams.state.KeyValueStore},
 * {@link org.apache.kafka.streams.state.TimestampedKeyValueStore},
 * {@link org.apache.kafka.streams.state.VersionedKeyValueStore},
 * {@link org.apache.kafka.streams.state.WindowStore}, and {@link org.apache.kafka.streams.state.SessionStore}.
 * Each type offers a way to manage and process streaming data according to different state management requirements.
 *
 * @see KafkaStreams
 * @see StreamsBuilder
 * @see Service
 */
@RequiredArgsConstructor
@Getter
@Slf4j
public enum StoreType {
    KEY_VALUE("kv-store") {
        @Override
        public <K, V> Service<K, V> createQueryService(final KafkaStreams streams) {
            return (Service<K, V>) KeyValueRestaurantService.setUp(streams);
        }

        @Override
        public void addTopology(final StreamsBuilder builder) {
            final KStream<String, String> inputStream = builder.stream(MENU_ITEM_DESCRIPTION_TOPIC);

            inputStream.toTable(Materialized.as(this.getStoreName()));
        }
    },
    TIMESTAMPED_KEY_VALUE("timestamped-kv-store") {
        @Override
        public <K, V> Service<K, V> createQueryService(final KafkaStreams streams) {
            return (Service<K, V>) TimestampedKeyValueRestaurantService.setUp(streams);
        }

        @Override
        public void addTopology(final StreamsBuilder builder) {
            final KStream<String, String> inputStream = builder.stream(MENU_ITEM_DESCRIPTION_TOPIC);

            inputStream.processValues(new WriteTimestampedKeyValueDataProcessorSupplier());
        }
    },
    VERSIONED_KEY_VALUE("versioned-kv-store") {
        @Override
        public <K, V> Service<K, V> createQueryService(final KafkaStreams streams) {
            return (Service<K, V>) VersionedKeyValueRestaurantService.setUp(streams);
        }

        @Override
        public void addTopology(final StreamsBuilder builder) {
            final KStream<String, String> inputStream = builder.stream(MENU_ITEM_DESCRIPTION_TOPIC);
            inputStream.processValues(new WriteVersionedKeyValueDataProcessorSupplier());
        }

    },
    WINDOWED_KEY_VALUE("windowed-kv-store") {
        @Override
        public <K, V> Service<K, V> createQueryService(final KafkaStreams streams) {
            return (Service<K, V>) WindowedKeyValueRestaurantService.setUp(streams);
        }

        @Override
        public void addTopology(final StreamsBuilder builder) {
            final KStream<String, String> inputStream =
                    builder.stream(MENU_ITEM_DESCRIPTION_TOPIC, Consumed.with(new OrderTimeExtractor()));

            final KGroupedStream<String, String> groupedFoodOrders =
                    inputStream
                            .groupBy(((key, value) -> Utils.readToObject(value, Order.class).menuItem()));

            final TimeWindows hourlyWindow = TimeWindows.ofSizeAndGrace(Duration.ofHours(1), Duration.ofMinutes(5));
            final KTable<Windowed<String>, Long> count = groupedFoodOrders.windowedBy(hourlyWindow)
                    .count(Materialized.as(this.getStoreName()));

            count.toStream()
                    .peek(((key, value) -> log.debug("Count '{}', '{}' from '{}' to '{}'", value, key.key(), key.window().start(), key.window().end())));
        }

    }, SESSION_KEY_VALUE("session-kv-store") {
        @Override
        public <K, V> Service<K, V> createQueryService(final KafkaStreams streams) {
            return (Service<K, V>) SessionedKeyValueRestaurantService.setUp(streams);
        }

        @Override
        public void addTopology(final StreamsBuilder builder) {
            final KStream<String, String> inputStream =
                    builder.stream(MENU_ITEM_DESCRIPTION_TOPIC, Consumed.with(new OrderTimeExtractor()));

            final KGroupedStream<String, String> groupedFoodOrders =
                    inputStream.groupBy(((key, value) -> Utils.readToObject(value, Order.class).customerId()));

            // TODO: new DSL is not working...
//            final SessionWindows sessionWindows = SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(30));
            final SessionWindows sessionWindows = SessionWindows.with(Duration.ofMinutes(30));
            final KTable<Windowed<String>, Long> sessionCounts = groupedFoodOrders.windowedBy(sessionWindows)
                    .count(Materialized.as(this.getStoreName()));

            sessionCounts.toStream()
                    .peek((windowedKey, count) -> {
                        final String customerId = windowedKey.key();
                        final long start = windowedKey.window().start();
                        final long end = windowedKey.window().end();
                        log.debug("Dining session for customer '{}': count = {}, from {} to {}", customerId, count, start, end);

                        // Here, you can add logic to calculate the session duration or other statistics
                    });
        }
    };
    private final String storeName;

    /**
     * Abstract method to create a query service for a specific state store type.
     *
     * @param streams The Kafka Streams instance used for querying state.
     * @param <K>     The key type of the store.
     * @param <V>     The value type of the store.
     * @return A {@code Service} for querying the store.
     */
    public abstract <K, V> Service<K, V> createQueryService(final KafkaStreams streams);

    /**
     * Abstract method to add a topology to the Kafka Streams builder for this specific store type.
     *
     * @param builder The Kafka Streams builder to which the topology will be added.
     */
    public abstract void addTopology(final StreamsBuilder builder);

}
