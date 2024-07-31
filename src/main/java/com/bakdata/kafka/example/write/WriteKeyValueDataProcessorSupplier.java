package com.bakdata.kafka.example.write;

import com.bakdata.kafka.example.StoreType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Set;

/**
 * Writes the ingested data into the versioned state store
 */
@Slf4j
public class WriteKeyValueDataProcessorSupplier implements FixedKeyProcessorSupplier<String, String, String> {

    private static final String STORE = StoreType.KEY_VALUE.getStoreName();

    @Override
    public FixedKeyProcessor<String, String, String> get() {
        return new WriteDataProcessor();
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return Set.of(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE),
                        Serdes.String(),
                        null)
        );
    }

    private static class WriteDataProcessor implements FixedKeyProcessor<String, String, String> {
        private KeyValueStore<String, String> keyValueStore = null;

        @Override
        public void init(final FixedKeyProcessorContext<String, String> context) {
            this.keyValueStore = context.getStateStore(STORE);
        }

        @Override
        public void process(final FixedKeyRecord<String, String> record) {
            final String key = record.key();
            final String value = record.value();
            log.debug("Writing recoder with key '{}' and description '{}' in store '{}'", key, value, STORE);
            this.keyValueStore.put(key, value);
        }
    }
}
