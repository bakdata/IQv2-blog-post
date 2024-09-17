package com.bakdata.kafka.example;


import com.bakdata.kafka.example.model.Order;
import com.bakdata.kafka.example.read.*;
import com.bakdata.kafka.example.utils.OrderTimeExtractor;
import com.bakdata.kafka.example.utils.Utils;
import com.bakdata.kafka.example.write.WriteTimestampedKeyValueDataProcessorSupplier;
import com.bakdata.kafka.example.write.WriteVersionedKeyValueDataProcessorSupplier;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

import static com.bakdata.kafka.example.KeyValueStoreApplication.MENU_ITEM_DESCRIPTION_TOPIC;

@RequiredArgsConstructor
@Getter
@Slf4j
public enum StoreType {
    KEY_VALUE("kv-store") {
        @Override
        public <K, V> Service<K, V> createQueryService(final KafkaStreams streams) {
            return (Service<K, V>) KeyValueOrderService.setUp(streams);
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
            return (Service<K, V>) TimestampedKeyValueOrderService.setUp(streams);
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
            return (Service<K, V>) VersionedKeyValueOrderService.setUp(streams);
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
            return (Service<K, V>) WindowedKeyValueOrderService.setUp(streams);
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
            return (Service<K, V>) SessionedKeyValueOrderService.setUp(streams);
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

    public abstract <K, V> Service<K, V> createQueryService(final KafkaStreams streams);

    public abstract void addTopology(final StreamsBuilder builder);

}
