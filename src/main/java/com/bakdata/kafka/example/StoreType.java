package com.bakdata.kafka.example;


import com.bakdata.kafka.example.read.*;
import com.bakdata.kafka.example.write.WriteKeyValueDataProcessorSupplier;
import com.bakdata.kafka.example.write.WriteTimestampedKeyValueDataProcessorSupplier;
import com.bakdata.kafka.example.write.WriteVersionedKeyValueDataProcessorSupplier;
import com.bakdata.kafka.example.write.WriteWindowedKeyValueDataProcessorSupplier;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;

@RequiredArgsConstructor
@Getter
public enum StoreType {
    KEY_VALUE("kv-store") {
        // read
        @Override
        public <K, V> Service<K, V> createQueryService(final KafkaStreams streams) {
            return (Service<K, V>) KeyValueOrderService.setUp(streams);
        }

        // write
        @Override
        public <K, V> FixedKeyProcessorSupplier<K, V, V> createWriteProcessor() {
            return (FixedKeyProcessorSupplier<K, V, V>) new WriteKeyValueDataProcessorSupplier();
        }
    },
    TIMESTAMPED_KEY_VALUE("timestamped-kv-store") {
        @Override
        public <K, V> Service<K, V> createQueryService(final KafkaStreams streams) {
            return (Service<K, V>) TimestampedKeyValueOrderService.setUp(streams);
        }

        @Override
        public <K, V> FixedKeyProcessorSupplier<K, V, V> createWriteProcessor() {
            return (FixedKeyProcessorSupplier<K, V, V>) new WriteTimestampedKeyValueDataProcessorSupplier();
        }
    },
    VERSIONED_KEY_VALUE("versioned-kv-store") {
        @Override
        public <K, V> Service<K, V> createQueryService(final KafkaStreams streams) {
            return (Service<K, V>) VersionedKeyValueOrderService.setUp(streams);
        }

        @Override
        public <K, V> FixedKeyProcessorSupplier<K, V, V> createWriteProcessor() {
            return (FixedKeyProcessorSupplier<K, V, V>) new WriteVersionedKeyValueDataProcessorSupplier();
        }

    },
    WINDOWED_KEY_VALUE("windowed-kv-store") {
        @Override
        public <K, V> Service<K, V> createQueryService(final KafkaStreams streams) {
            return (Service<K, V>) WindowedKeyValueOrderService.setUp(streams);
        }

        @Override
        public <K, V> FixedKeyProcessorSupplier<K, V, V> createWriteProcessor() {
            return (FixedKeyProcessorSupplier<K, V, V>) new WriteWindowedKeyValueDataProcessorSupplier();
        }
    };
    private final String storeName;

    public abstract <K, V> Service<K, V> createQueryService(final KafkaStreams streams);

    public abstract <K, V> FixedKeyProcessorSupplier<K, V, V> createWriteProcessor();

}
