package com.bakdata.kafka.example;

import com.bakdata.kafka.example.read.Service;
import io.confluent.common.utils.TestUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import picocli.CommandLine;

import java.util.Properties;

@Setter
@Slf4j
public final class KeyValueStoreApplication implements Runnable {

    static final String MENU_ITEM_DESCRIPTION_TOPIC = "menu-item-description-topic";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";

    @CommandLine.Option(names = "--service-type")
    private StoreType storeType = StoreType.KEY_VALUE;

    private KeyValueStoreApplication() {
    }

    public static void main(final String[] args) {
        log.info("Starting application");
        new Thread(new KeyValueStoreApplication()).start();
    }

    static <K, V> Service<K, V> startApplication(final StoreType storeType) {
        // Configure the Streams application.
        final Properties streamsConfiguration = getStreamsConfiguration();

        // Define the processing topology of the Streams application.
        final StreamsBuilder builder = new StreamsBuilder();

        writeInStateStore(builder, storeType);

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        streams.cleanUp();

        streams.start();

        Runtime.getRuntime()
                .addShutdownHook(new Thread(streams::close));

        return storeType.createQueryService(streams);
    }

    private static void writeInStateStore(final StreamsBuilder builder, final StoreType storeType) {
        final KStream<String, String> inputStream = builder.stream(MENU_ITEM_DESCRIPTION_TOPIC);

        inputStream.processValues(storeType.createWriteProcessor(), Named.as("write-data"));
    }

    private static Properties getStreamsConfiguration() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once_v2");
        streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION), 1);
        streamsConfiguration.setProperty(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");

        streamsConfiguration.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "food-order");
        streamsConfiguration.setProperty(StreamsConfig.CLIENT_ID_CONFIG, "food-order-client");
        streamsConfiguration.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);

        // TODO: Change this:
        streamsConfiguration.setProperty(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:8080");

        configureDefaultSerde(streamsConfiguration);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfiguration.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        return streamsConfiguration;
    }

    private static void configureDefaultSerde(final Properties streamsConfiguration) {
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    }

    @Override
    public void run() {
        startApplication(this.storeType);
    }

}
