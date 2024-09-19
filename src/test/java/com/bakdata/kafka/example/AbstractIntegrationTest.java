package com.bakdata.kafka.example;

import net.mguenther.kafka.junit.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collection;

import static com.bakdata.kafka.example.RestaurantManagmentApplication.MENU_ITEM_DESCRIPTION_TOPIC;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;

@ExtendWith(SoftAssertionsExtension.class)
abstract class AbstractIntegrationTest {
    private static final int SLEEP_TIME_TO_INGEST = 5000;

    private static final int NUMBER_OF_PARTITIONS = 3;

    private final EmbeddedKafkaCluster kafkaCluster =
            provisionWith(EmbeddedKafkaClusterConfig.defaultClusterConfig());

    @InjectSoftAssertions
    protected SoftAssertions softly;

    @BeforeEach
    void setUp() throws InterruptedException {
        this.kafkaCluster.start();
        this.kafkaCluster.createTopic(TopicConfig.withName(MENU_ITEM_DESCRIPTION_TOPIC)
                .withNumberOfPartitions(NUMBER_OF_PARTITIONS)
                .build());
        this.send(this.createRecords());
        Thread.sleep(SLEEP_TIME_TO_INGEST);
    }

    @AfterEach
    void tearDown() throws Exception {
        this.kafkaCluster.stop();
    }

    protected abstract Collection<KeyValue<String, String>> createRecords();

    private void send(final Collection<KeyValue<String, String>> keyValues) throws InterruptedException {
        final SendKeyValuesTransactional<String, String> sendRequest = SendKeyValuesTransactional
                .inTransaction(MENU_ITEM_DESCRIPTION_TOPIC, keyValues)
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .build();
        this.kafkaCluster.send(sendRequest);
    }


}
