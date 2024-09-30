package com.bakdata.kafka.example;

import com.bakdata.kafka.example.read.Service;
import net.mguenther.kafka.junit.KeyValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

class KeyValueIntegrationTest extends AbstractIntegrationTest {
    private final Service<String, String> keyValueStoreApp =
            RestaurantManagementApplication.startApplication(StoreType.KEY_VALUE);

    @Override
    protected Collection<KeyValue<String, String>> createRecords() {
        return List.of(
                new KeyValue<>(
                        "Pizza",
                        "A delicious cheesy pizza with toppings"
                ),
                new KeyValue<>(
                        "Pasta",
                        "A classic Italian pasta with tomato sauce"
                ),
                new KeyValue<>(
                        "Burger",
                        "A juicy beef burger with lettuce, tomato, and cheese"
                ),
                new KeyValue<>(
                        "Salad",
                        "A fresh garden salad with a variety of vegetables"
                ),
                new KeyValue<>(
                        "Sushi",
                        "Freshly made sushi rolls with rice and fish"
                ),
                new KeyValue<>(
                        "Tacos",
                        "Mexican-style tacos with beef, lettuce, and cheese"
                ),
                new KeyValue<>(
                        "Soup",
                        "A warm and hearty chicken soup"
                ),
                new KeyValue<>(
                        "Sandwich",
                        "A classic ham and cheese sandwich"
                ),
                new KeyValue<>(
                        "Steak",
                        "A tender grilled steak cooked to perfection"
                ),
                new KeyValue<>(
                        "Dessert",
                        "A rich chocolate cake with creamy frosting"
                )
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        this.keyValueStoreApp.close();
        super.tearDown();
    }

    @Test
    void shouldQueryCorrectWhenKeyQueryIsRequested() {
        final Optional<String> aggregatedOrder = this.keyValueStoreApp
                .getValueForKey("Pizza");

        this.softly.assertThat(aggregatedOrder)
                .hasValue("A delicious cheesy pizza with toppings");
    }

    @Test
    void shouldQueryCorrectWhenRangeQueryIsRequested() {
        final List<String> aggregatedOrder = this.keyValueStoreApp
                .getValuesForRange("Burger", "Pasta");

        this.softly.assertThat(aggregatedOrder)
                .hasSize(3)
                .anySatisfy(value -> this.softly.assertThat(value)
                        .isEqualTo("A juicy beef burger with lettuce, tomato, and cheese"))
                .anySatisfy(value -> this.softly.assertThat(value)
                        .isEqualTo("A rich chocolate cake with creamy frosting"))
                .anySatisfy(value -> this.softly.assertThat(value)
                        .isEqualTo("A classic Italian pasta with tomato sauce"));
    }
}
