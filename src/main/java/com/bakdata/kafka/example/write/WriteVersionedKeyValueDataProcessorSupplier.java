package com.bakdata.kafka.example.write;

import com.bakdata.kafka.example.StoreType;
import com.bakdata.kafka.example.model.Price;
import com.bakdata.kafka.example.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.VersionedKeyValueStore;

import java.time.Duration;
import java.util.Set;


/**
 * {@code WriteVersionedKeyValueDataProcessorSupplier} supplies a processor that writes versioned key-value
 * records to a versioned key-value store with a specified retention period.
 *
 * <p>This class implements the {@link FixedKeyProcessorSupplier} interface and provides a processor that
 * processes incoming records by deserializing the value and writing it into a Kafka Streams
 * {@link VersionedKeyValueStore}.
 *
 * @see FixedKeyProcessor
 * @see VersionedKeyValueStore
 */

@Slf4j
public class WriteVersionedKeyValueDataProcessorSupplier implements FixedKeyProcessorSupplier<String, String, Integer> {

    private static final String STORE = StoreType.VERSIONED_KEY_VALUE.getStoreName();

    @Override
    public FixedKeyProcessor<String, String, Integer> get() {
        return new WriteDataProcessor();
    }

    /**
     * Provides the set of state stores required by this processor. In this case, a {@link VersionedKeyValueStore}
     * is used. The store retains data for up to 365 days.
     *
     * @return A set containing the store builders.
     */
    @Override
    public Set<StoreBuilder<?>> stores() {
        return Set.of(
                Stores.versionedKeyValueStoreBuilder(
                        Stores.persistentVersionedKeyValueStore(STORE, Duration.ofDays(365)),
                        Serdes.String(),
                        Serdes.Integer()
                )
        );
    }

    /**
     * {@code WriteDataProcessor} processes each incoming record, deserializing the value into a {@link Price} object
     * and writing the price and its valid-from timestamp into the versioned key-value store.
     */
    private static class WriteDataProcessor implements FixedKeyProcessor<String, String, Integer> {
        private VersionedKeyValueStore<String, Integer> versionedKeyValueStore = null;

        /**
         * Initializes the processor by retrieving the versioned key-value store from the context.
         *
         * @param context The processor context, used to retrieve the state store.
         */
        @Override
        public void init(final FixedKeyProcessorContext<String, Integer> context) {
            this.versionedKeyValueStore = context.getStateStore(STORE);
        }

        /**
         * Processes the incoming record by deserializing the value into a {@link Price} object, then writes
         * the price and its valid-from timestamp into the store.
         *
         * @param record The record containing the key and value to process.
         */
        @Override
        public void process(final FixedKeyRecord<String, String> record) {
            final String key = record.key();
            final String value = record.value();
            final Price price = Utils.readToObject(value, Price.class);
            log.debug("Writing recoder with key '{}' and price '{}' valid from '{}' in store '{}'", key, price.price(), Utils.toLocalDateTime(price.validFrom()), STORE);
            this.versionedKeyValueStore.put(key, price.price(), price.validFrom());
        }
    }
}
