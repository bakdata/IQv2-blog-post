package com.bakdata.kafka.example.write;

import com.bakdata.kafka.example.StoreType;
import com.bakdata.kafka.example.model.Price;
import com.bakdata.kafka.example.utils.Utils;
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
import org.apache.kafka.streams.state.VersionedKeyValueStore;

import java.time.Duration;
import java.util.Set;

/**
 * Writes the ingested data into the versioned state store
 */
@Slf4j
public class WriteVersionedKeyValueDataProcessorSupplier implements FixedKeyProcessorSupplier<String, String, Integer> {

    private static final String STORE = StoreType.VERSIONED_KEY_VALUE.getStoreName();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public FixedKeyProcessor<String, String, Integer> get() {
        return new WriteDataProcessor();
    }

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

    private static class WriteDataProcessor implements FixedKeyProcessor<String, String, Integer> {
        private VersionedKeyValueStore<String, Integer> versionedKeyValueStore = null;

        @Override
        public void init(final FixedKeyProcessorContext<String, Integer> context) {
            this.versionedKeyValueStore = context.getStateStore(STORE);
        }

        @Override
        public void process(final FixedKeyRecord<String, String> record) {
            final String key = record.key();
            final String value = record.value();
            try {
                final Price price = MAPPER.readValue(value, Price.class);
                log.debug("Writing recoder with key '{}' and price '{}' valid from '{}' in store '{}'", key, price.price(), Utils.toLocalDateTime(price.validFrom()), STORE);
                this.versionedKeyValueStore.put(key, price.price(), price.validFrom());
            } catch (final JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
