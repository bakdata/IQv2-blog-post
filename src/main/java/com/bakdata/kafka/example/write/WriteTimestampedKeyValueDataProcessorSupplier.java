package com.bakdata.kafka.example.write;

import com.bakdata.kafka.example.StoreType;
import com.bakdata.kafka.example.model.Promotion;
import com.bakdata.kafka.example.utils.Utils;
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
 * {@code WriteTimestampedKeyValueDataProcessorSupplier} supplies a processor that writes timestamped key-value
 * records to a persistent store.
 *
 * <p>This class implements the {@link FixedKeyProcessorSupplier} interface and provides a processor that
 * deserializes the value (expected to be a JSON string) into a {@link Promotion} object and writes it into a
 * Kafka Streams {@link TimestampedKeyValueStore}.
 *
 * @see FixedKeyProcessor
 * @see TimestampedKeyValueStore
 */
@Slf4j
public class WriteTimestampedKeyValueDataProcessorSupplier implements FixedKeyProcessorSupplier<String, String, String> {

    private static final String STORE = StoreType.TIMESTAMPED_KEY_VALUE.getStoreName();

    @Override
    public FixedKeyProcessor<String, String, String> get() {
        return new WriteDataProcessor();
    }

    /**
     * Provides the set of state stores required by this processor. In this case, a  {@link TimestampedKeyValueStore}
     * is used.
     *
     * @return A set containing the store builders.
     */
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

        /**
         * Initializes the processor by retrieving the timestamped key-value store from the context.
         *
         * @param context The processor context, used to retrieve the state store.
         */
        @Override
        public void init(final FixedKeyProcessorContext<String, String> context) {
            this.timestampedKeyValueStore = context.getStateStore(STORE);
        }

        /**
         * Processes the incoming record by deserializing the value into a {@link Promotion} object, then writes
         * the promotion code and end timestamp into the store.
         *
         * @param record The record containing the key and value to process.
         */
        @Override
        public void process(final FixedKeyRecord<String, String> record) {
            final String menuItem = record.key();
            final String value = record.value();
            final Promotion promotion = Utils.readToObject(value, Promotion.class);
            log.debug("Writing recoder with menuItem '{}' and promotion code '{}' in store '{}'", menuItem, promotion, STORE);
            this.timestampedKeyValueStore.put(promotion.code(), ValueAndTimestamp.make(menuItem, promotion.endTimestamp()));
        }
    }
}
