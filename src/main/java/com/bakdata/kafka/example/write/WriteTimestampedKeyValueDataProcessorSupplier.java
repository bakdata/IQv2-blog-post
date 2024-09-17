package com.bakdata.kafka.example.write;

import com.bakdata.kafka.example.StoreType;
import com.bakdata.kafka.example.model.Promotion;
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
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Set;

/**
 * Writes the ingested data into the versioned state store
 */
@Slf4j
public class WriteTimestampedKeyValueDataProcessorSupplier implements FixedKeyProcessorSupplier<String, String, String> {

    private static final String STORE = StoreType.TIMESTAMPED_KEY_VALUE.getStoreName();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public FixedKeyProcessor<String, String, String> get() {
        return new WriteDataProcessor();
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return Set.of(
                Stores.timestampedKeyValueStoreBuilder(
                        Stores.persistentTimestampedKeyValueStore(STORE),
                        Serdes.String(),
                        null)
        );
    }

    private static class WriteDataProcessor implements FixedKeyProcessor<String, String, String> {
        private TimestampedKeyValueStore<String, String> timestampedKeyValueStore = null;

        @Override
        public void init(final FixedKeyProcessorContext<String, String> context) {
            this.timestampedKeyValueStore = context.getStateStore(STORE);
        }

        @Override
        public void process(final FixedKeyRecord<String, String> record) {
            final String menuItem = record.key();
            final String value = record.value();
            try {
                final Promotion promotion = MAPPER.readValue(value, Promotion.class);
                log.debug("Writing recoder with menuItem '{}' and promotion code '{}' in store '{}'", menuItem, promotion, STORE);
                this.timestampedKeyValueStore.put(promotion.code(), ValueAndTimestamp.make(menuItem, promotion.endTimestamp()));
            } catch (final JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
