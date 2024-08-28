package com.bakdata.kafka.example;


import com.bakdata.kafka.example.model.Order;
import com.bakdata.kafka.example.read.*;
import com.bakdata.kafka.example.utils.OrderTimeExtractor;
import com.bakdata.kafka.example.write.WriteKeyValueDataProcessorSupplier;
import com.bakdata.kafka.example.write.WriteTimestampedKeyValueDataProcessorSupplier;
import com.bakdata.kafka.example.write.WriteVersionedKeyValueDataProcessorSupplier;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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

            inputStream.processValues(new WriteKeyValueDataProcessorSupplier());
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
            final ObjectMapper mapper = new ObjectMapper();
            final TimeWindows windows = TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1));

            final KGroupedStream<String, String> groupedFoodOrders =
                    inputStream.mapValues(value -> readToMenuItem(value, mapper))
                            .selectKey((key, value) -> value)
                            .repartition(Repartitioned.as("menu-item"))
                            .groupByKey(Grouped.as("Food orders"));

            final KTable<Windowed<String>, Long> count = groupedFoodOrders.windowedBy(windows)
                    .count(Materialized.as(this.getStoreName()));

            count.toStream()
                    .peek(((key, value) -> log.debug("Count '{}', '{}' from '{}' to '{}''", value, key.key(), key.window().startTime(), key.window().endTime())));
        }

        private static String readToMenuItem(final String value, final ObjectMapper mapper) {
            try {
                return mapper.readValue(value, Order.class).menuItem();
            } catch (final JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    };
    private final String storeName;

    public abstract <K, V> Service<K, V> createQueryService(final KafkaStreams streams);

    public abstract void addTopology(final StreamsBuilder builder);

}
