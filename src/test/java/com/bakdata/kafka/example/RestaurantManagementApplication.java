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

import java.util.Properties;

@Setter
@Slf4j
public final class RestaurantManagementApplication implements Runnable {

    static final String MENU_ITEM_DESCRIPTION_TOPIC = "restaurant-management-input-topic";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String APPLICATION_HOST_PORT = "localhost:8080";

    private StoreType storeType = StoreType.KEY_VALUE;

    private RestaurantManagementApplication() {
    }

    public static void main(final String[] args) {
        log.info("Starting application");
        new Thread(new RestaurantManagementApplication()).start();
    }

    static <K, V> Service<K, V> startApplication(final StoreType storeType) {
        // Configure the Streams application.
        final Properties streamsConfiguration = getStreamsConfiguration();

        // Define the processing topology of the Streams application.
        final StreamsBuilder builder = new StreamsBuilder();

        storeType.addTopology(builder, MENU_ITEM_DESCRIPTION_TOPIC);

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        streams.cleanUp();

        streams.start();

        Runtime.getRuntime()
                .addShutdownHook(new Thread(streams::close));

        return storeType.createQueryService(streams);
    }

    private static Properties getStreamsConfiguration() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once_v2");
        streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION), 1);
        streamsConfiguration.setProperty(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");

        streamsConfiguration.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "food-order");
        streamsConfiguration.setProperty(StreamsConfig.CLIENT_ID_CONFIG, "food-order-client");
        streamsConfiguration.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        streamsConfiguration.setProperty(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "gzip");

        streamsConfiguration.setProperty(StreamsConfig.APPLICATION_SERVER_CONFIG, APPLICATION_HOST_PORT);

        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        return streamsConfiguration;
    }

    @Override
    public void run() {
        startApplication(this.storeType);
    }

}
