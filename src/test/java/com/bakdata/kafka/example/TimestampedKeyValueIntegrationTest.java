package com.bakdata.kafka.example;

import com.bakdata.kafka.example.read.Service;
import net.mguenther.kafka.junit.KeyValue;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

class TimestampedKeyValueIntegrationTest extends AbstractIntegrationTest {

    private final Service<String, ValueAndTimestamp<String>> timestampedKeyValueStoreApp =
            RestaurantManagmentApplication.startApplication(StoreType.TIMESTAMPED_KEY_VALUE);

    @Override
    protected Collection<KeyValue<String, String>> createRecords() {
        return List.of(
                new KeyValue<>(
                        "Pizza",
                        "{\"code\": \"SUMMER2024\", \"endTimestamp\": 10}"
                ),
                new KeyValue<>(
                        "Burger",
                        "{\"code\": \"WINTER2024\", \"endTimestamp\": 20}"
                ),
                new KeyValue<>(
                        "Sushi",
                        "{\"code\": \"SPRING2024\", \"endTimestamp\": 30}"
                ),
                new KeyValue<>(
                        "Sandwich",
                        "{\"code\": \"FALL2024\", \"endTimestamp\": 40}"
                )
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        this.timestampedKeyValueStoreApp.close();
        super.tearDown();
    }

    @Test
    void shouldQueryCorrectWhenKeyQueryIsRequested() {
        final Optional<ValueAndTimestamp<String>> aggregatedOrder = this.timestampedKeyValueStoreApp
                .getValueForKey("SUMMER2024");

        this.softly.assertThat(aggregatedOrder)
                .hasValue(ValueAndTimestamp.make("Pizza", 10));
    }

    @Test
    void shouldQueryCorrectWhenRangeQueryIsRequested() {
        final List<ValueAndTimestamp<String>> aggregatedOrder = this.timestampedKeyValueStoreApp
                .getValuesForRange("FALL2024", "SUMMER2024");

        this.softly.assertThat(aggregatedOrder)
                .hasSize(3)
                .anySatisfy(promotion -> this.softly.assertThat(promotion)
                        .isEqualTo(ValueAndTimestamp.make("Sandwich", 40)))
                .anySatisfy(promotion -> this.softly.assertThat(promotion)
                        .isEqualTo(ValueAndTimestamp.make("Sushi", 30)))
                .anySatisfy(promotion -> this.softly.assertThat(promotion)
                        .isEqualTo(ValueAndTimestamp.make("Pizza", 10)));
    }

}
