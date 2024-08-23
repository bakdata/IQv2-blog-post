package com.bakdata.kafka.example;

import com.bakdata.kafka.example.read.Service;
import lombok.NonNull;
import net.mguenther.kafka.junit.KeyValue;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

@ExtendWith(SoftAssertionsExtension.class)
class WindowedKeyValueIntegrationTest extends AbstractIntegrationTest {
    private final Service<String, Integer> timestampedKeyValueStoreApp =
            KeyValueStoreApplication.startApplication(StoreType.WINDOWED_KEY_VALUE);

    private static Stream<Arguments> getMenuItemAndPriceAndDateTime() {
        return Stream.of(
                // Pizza
                Arguments.of(new Request("Pizza", Instant.ofEpochMilli(0), Instant.ofEpochMilli(3_600_000)), Collections.emptyList()),
                Arguments.of(new Request("Pizza", Instant.ofEpochMilli(3_600_000), Instant.ofEpochMilli(7_200_000)), List.of(3)),
                Arguments.of(new Request("Pizza", Instant.ofEpochMilli(7_200_000), Instant.ofEpochMilli(10_800_000)), List.of(2)),
                Arguments.of(new Request("Pizza", Instant.ofEpochMilli(0), Instant.ofEpochMilli(7_200_001)), List.of(3, 2))
        );
    }

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
                        "Pizza",
                        "{\"menuItem\": \"Pizza\", \"timestamp\": 7200003}"
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
                ),
                new KeyValue<>(
                        "Burger",
                        "{\"menuItem\": \"Burger\", \"timestamp\": 3600008}"
                )
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        this.timestampedKeyValueStoreApp.close();
        super.tearDown();
    }

    @ParameterizedTest
    @MethodSource("getMenuItemAndPriceAndDateTime")
    void shouldQueryCorrectWhenKeyQueryIsRequested(final Request request, final Collection<Integer> expected) throws InterruptedException {
        Thread.sleep(3000);
        final List<Integer> aggregatedOrder = this.timestampedKeyValueStoreApp
                .getWindowedValueForKey(request.menuItem(), request.from(), request.to());

        this.softly.assertThat(aggregatedOrder)
                .hasSize(expected.size())
                .isEqualTo(expected);
    }

    @Test
    void shouldQueryCorrectWhenRangeQueryIsRequested() {
        final List<Integer> aggregatedOrder = this.timestampedKeyValueStoreApp
                .getWindowedRange(Instant.ofEpochMilli(3600000), Instant.ofEpochMilli(3_600_010));

        this.softly.assertThat(aggregatedOrder)
                .hasSize(2)
                .anySatisfy(countPizza -> this.softly.assertThat(countPizza)
                        .isEqualTo(3))
                .anySatisfy(countBurger -> this.softly.assertThat(countBurger)
                        .isEqualTo(4));
    }

    @Test
    @Disabled("Does not work. WindowStores only supports WindowRangeQuery.withWindowStartRange.")
    void shouldQueryCorrectWhenRangeQueryForKeyIsRequested() {
        final List<Integer> aggregatedOrder = this.timestampedKeyValueStoreApp
                .getSessionRangeForKey("Pizza");

        this.softly.assertThat(aggregatedOrder)
                .hasSize(2)
                .anySatisfy(countPizza -> this.softly.assertThat(countPizza)
                        .isEqualTo(3))
                .anySatisfy(countBurger -> this.softly.assertThat(countBurger)
                        .isEqualTo(2));
    }

    record Request(@NonNull String menuItem, @NonNull Instant from, @NonNull Instant to) {
    }

}
