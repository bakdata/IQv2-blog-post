package com.bakdata.kafka.example;

import com.bakdata.kafka.example.read.Service;
import net.mguenther.kafka.junit.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.bakdata.kafka.example.KeyValueStoreApplication.MENU_ITEM_DESCRIPTION_TOPIC;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;

@ExtendWith(SoftAssertionsExtension.class)
class WindowedKeyValueIntegrationTest extends AbstractIntegrationTest {
    private final Service<String, Integer> timestampedKeyValueStoreApp =
            KeyValueStoreApplication.startApplication(StoreType.WINDOWED_KEY_VALUE);

    @Override
    protected Collection<KeyValue<String, String>> createRecords() {
        return List.of(
                new KeyValue<>(
                        "Pizza",
                        "{\"menuItem\": \"Pizza\", \"timestamp\": 3600000}"
                ),
                new KeyValue<>(
                        "Pizza",
                        "{\"menuItem\": \"Pizza\", \"timestamp\": 3600001}"
                ),
                new KeyValue<>(
                        "Pizza",
                        "{\"menuItem\": \"Pizza\", \"timestamp\": 3600003}"
                ),
                new KeyValue<>(
                        "Pizza",
                        "{\"menuItem\": \"Pizza\", \"timestamp\": 7200002}"
                ),
                new KeyValue<>(
                        "Burger",
                        "{\"menuItem\": \"Burger\", \"timestamp\": 3600005}"
                ),
                new KeyValue<>(
                        "Burger",
                        "{\"menuItem\": \"Burger\", \"timestamp\": 3600006}"
                ),
                new KeyValue<>(
                        "Burger",
                        "{\"menuItem\": \"Burger\", \"timestamp\": 3600007}"
                )
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        this.timestampedKeyValueStoreApp.close();
        super.tearDown();
    }

    @Test
    void shouldQueryCorrectWhenKeyQueryIsRequested() throws InterruptedException {
        Thread.sleep(3000);
        final Optional<Integer> aggregatedOrder = this.timestampedKeyValueStoreApp
                .getValueForKey("Pizza");

        this.softly.assertThat(aggregatedOrder)
                .hasValue(3);
    }

//    @Test
//    void shouldQueryCorrectWhenRangeQueryIsRequested() {
//        final List<ValueAndTimestamp<String>> aggregatedOrder = this.timestampedKeyValueStoreApp
//                .getValuesForRange("Burger", "Sandwich");
//
//        this.softly.assertThat(aggregatedOrder)
//                .hasSize(3)
//                .anySatisfy(value -> this.softly.assertThat(value)
//                        .isEqualTo(ValueAndTimestamp.make("FALL2024",1717920300 )))
//                .anySatisfy(value -> this.softly.assertThat(value)
//                        .isEqualTo(ValueAndTimestamp.make("WINTER2024", 1717920100)))
//                .anySatisfy(value -> this.softly.assertThat(value)
//                        .isEqualTo(ValueAndTimestamp.make("SUMMER2024",1717920000 )));
//    }

}
