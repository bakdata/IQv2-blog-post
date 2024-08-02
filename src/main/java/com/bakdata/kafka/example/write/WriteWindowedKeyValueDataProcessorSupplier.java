package com.bakdata.kafka.example.write;

import com.bakdata.kafka.example.StoreType;
import com.bakdata.kafka.example.model.Order;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Duration;
import java.util.Set;

/**
 * Writes the ingested data into the versioned state store
 */
@Slf4j
public class WriteWindowedKeyValueDataProcessorSupplier implements FixedKeyProcessorSupplier<String, String, String> {

    private static final String STORE = StoreType.WINDOWED_KEY_VALUE.getStoreName();
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration WINDOW_SIZE = Duration.ofHours(1);

    @Override
    public FixedKeyProcessor<String, String, String> get() {
        return new WriteDataProcessor();
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return Set.of(
                Stores.windowStoreBuilder(
                        Stores.persistentWindowStore(STORE, Duration.ofDays(365), WINDOW_SIZE, false),
                        Serdes.String(),
                        Serdes.Integer()
                )
        );
    }

    private static class WriteDataProcessor implements FixedKeyProcessor<String, String, String> {
        private WindowStore<String, Integer> windowStore = null;

        @Override
        public void init(final FixedKeyProcessorContext<String, String> context) {
            this.windowStore = context.getStateStore(STORE);
        }

        @Override
        public void process(final FixedKeyRecord<String, String> record) {
            final String key = record.key();
            final String value = record.value();
            try {
                final Order order = MAPPER.readValue(value, Order.class);
                log.debug("Writing recoder with key '{}' and order '{}' in store '{}'", key, order, STORE);
                final long timestamp = order.timestamp();
                final long windowStart = timestamp - (timestamp % WINDOW_SIZE.toMillis());
                final long windowEnd = windowStart + WINDOW_SIZE.toMillis() - 1;
                final int orderCount = this.fetchOrderCountFrom(order, windowStart, windowEnd) + 1;
                this.windowStore.put(order.menuItem(), orderCount, windowStart);
            } catch (final JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        private int fetchOrderCountFrom(final Order order, final long from, final long to) {

            final WindowStoreIterator<Integer> iterator = this.windowStore.fetch(
                    order.menuItem(),
                    from,
                    to
            );

            int total = 0;
            while (iterator.hasNext()) {
                total += iterator.next().value;
            }

            iterator.close();
            return total;
        }
    }
}
