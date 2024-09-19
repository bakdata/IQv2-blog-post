package com.bakdata.kafka.example;

import com.bakdata.kafka.example.read.Service;
import lombok.NonNull;
import net.mguenther.kafka.junit.KeyValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

class VersionedKeyValueIntegrationTest extends AbstractIntegrationTest {

    private final Service<String, Integer> versionedKeyValueStoreApp =
            RestaurantManagmentApplication.startApplication(StoreType.VERSIONED_KEY_VALUE);

    private static Stream<Arguments> getMenuItemAndPriceAndDateTime() {
        return Stream.of(
                // Pizza
                Arguments.of("Pizza", 10, Instant.ofEpochMilli(2)),
                Arguments.of("Pizza", 12, Instant.ofEpochMilli(6)),
                Arguments.of("Pizza", 15, Instant.ofEpochMilli(12)),
                Arguments.of("Pizza", 15, null),

                // Burger
                Arguments.of("Burger", 7, Instant.ofEpochMilli(4)),
                Arguments.of("Burger", 9, Instant.ofEpochMilli(10)),
                Arguments.of("Burger", 9, null)
        );
    }

    private static Stream<Arguments> getRequestAndPrices() {
        return Stream.of(
                // Pizza
                Arguments.of(
                        new Request("Pizza", null, null),
                        List.of(10, 12, 15)
                ),
                Arguments.of(
                        new Request("Pizza", Instant.ofEpochMilli(9), null),
                        List.of(12, 15)
                ),
                Arguments.of(
                        new Request("Pizza", Instant.ofEpochMilli(6), Instant.ofEpochMilli(9)),
                        List.of(12)
                ),
                Arguments.of(
                        new Request("Pizza", Instant.ofEpochMilli(2), Instant.ofEpochMilli(6)),
                        List.of(10, 12)
                ),
                Arguments.of(
                        new Request("Pizza", null, Instant.ofEpochMilli(4)),
                        List.of(10)
                ),
                Arguments.of(
                        new Request("Pizza", Instant.ofEpochMilli(6), Instant.ofEpochMilli(11)),
                        List.of(12, 15)
                ),

                // Burger
                Arguments.of(
                        new Request("Burger", null, null),
                        List.of(7, 9)
                )

        );
    }

    @Override
    protected Collection<KeyValue<String, String>> createRecords() {
        return List.of(
                new KeyValue<>(
                        "Pizza",
                        "{\"price\": 10, \"validFrom\": 1}"
                ),
                new KeyValue<>(
                        "Pizza",
                        "{\"price\": 12, \"validFrom\": 5}"
                ),
                new KeyValue<>(
                        "Pizza",
                        "{\"price\": 15, \"validFrom\": 10}"
                ),
                new KeyValue<>(
                        "Burger",
                        "{\"price\": 7, \"validFrom\": 3}"
                ),
                new KeyValue<>(
                        "Burger",
                        "{\"price\": 9, \"validFrom\": 7}"
                )
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        this.versionedKeyValueStoreApp.close();
        super.tearDown();
    }

    @ParameterizedTest
    @MethodSource("getMenuItemAndPriceAndDateTime")
    void shouldQueryCorrectWhenKeyQueryIsRequested(final String menuItem, final int expectedPrice, final Instant asOf) {
        final Optional<Integer> aggregatedOrder = this.versionedKeyValueStoreApp
                .getVersionedValueForKey(menuItem, asOf);

        this.softly.assertThat(aggregatedOrder)
                .hasValue(expectedPrice);
    }

    @ParameterizedTest
    @MethodSource("getRequestAndPrices")
    void shouldQueryCorrectWhenRangeQueryIsRequested(final Request request, final Collection<Integer> prices) {
        final List<Integer> aggregatedOrder = this.versionedKeyValueStoreApp
                .getVersionedValuesForRange(request.menuItem(), request.from(), request.to());

        this.softly.assertThat(aggregatedOrder)
                .hasSize(prices.size())
                .isEqualTo(prices);
    }

    record Request(@NonNull String menuItem, @Nullable Instant from, @Nullable Instant to) {
    }
}
