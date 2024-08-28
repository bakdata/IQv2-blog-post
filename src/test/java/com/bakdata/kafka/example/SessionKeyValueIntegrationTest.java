package com.bakdata.kafka.example;

import com.bakdata.kafka.example.read.Service;
import lombok.NonNull;
import net.mguenther.kafka.junit.KeyValue;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.provider.Arguments;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.Thread.sleep;

@ExtendWith(SoftAssertionsExtension.class)
class SessionKeyValueIntegrationTest extends AbstractIntegrationTest {
    private final Service<String, Long> sessionKeyValueStoreApp =
            KeyValueStoreApplication.startApplication(StoreType.SESSION_KEY_VALUE);

    @Override
    protected Collection<KeyValue<String, String>> createRecords() {
        return List.of(
                // 3 Pizza in first window
                new KeyValue<>(
                        "order-1",
                        "{\"menuItem\": \"Pizza\", \"timestamp\": 3600000}"
                ),
                new KeyValue<>(
                        "order-2",
                        "{\"menuItem\": \"Pizza\", \"timestamp\": 3600001}"
                ),
                new KeyValue<>(
                        "order-3",
                        "{\"menuItem\": \"Pizza\", \"timestamp\": 3600003}"
                ),

                // 4 Burgers in first window
                new KeyValue<>(
                        "order-4",
                        "{\"menuItem\": \"Burger\", \"timestamp\": 3600005}"
                ),
                new KeyValue<>(
                        "order-5",
                        "{\"menuItem\": \"Burger\", \"timestamp\": 3600006}"
                ),
                new KeyValue<>(
                        "order-6",
                        "{\"menuItem\": \"Burger\", \"timestamp\": 3600007}"
                ),
                new KeyValue<>(
                        "order-7",
                        "{\"menuItem\": \"Burger\", \"timestamp\": 3600008}"
                ),

                // 2 Pizza in after two hours
                new KeyValue<>(
                        "oroder-8",
                        "{\"menuItem\": \"Pizza\", \"timestamp\": 108000001}"
                ),
                new KeyValue<>(
                        "order-9",
                        "{\"menuItem\": \"Pizza\", \"timestamp\": 108000002}"
                ),
                new KeyValue<>(
                        "order-10",
                        "{\"menuItem\": \"Burger\", \"timestamp\": 108000003}"
                )
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        this.sessionKeyValueStoreApp.close();
        super.tearDown();
    }

//    @ParameterizedTest
//    @MethodSource("getMenuItemAndPriceAndDateTime")
//    void shouldQueryCorrectWhenKeyQueryIsRequested(final Request request, final Collection<Integer> expected) throws InterruptedException {
//        Thread.sleep(8000);
//        final List<Long> aggregatedOrder = this.windowedKeyValueStoreApp
//                .getWindowedValueForKey(request.menuItem(), request.from(), request.to());
//
//        this.softly.assertThat(aggregatedOrder)
//                .hasSize(expected.size())
//                .isEqualTo(expected);
//    }

    @Test
    void shouldQueryCorrectWhenRangeQueryForKeyIsRequested() throws InterruptedException {
        sleep(10000);
        final List<Long> aggregatedOrder = this.sessionKeyValueStoreApp
                .getSessionRangeForKey("Pizza");

        this.softly.assertThat(aggregatedOrder)
                .hasSize(2)
                .anySatisfy(countPizza -> this.softly.assertThat(countPizza)
                        .isEqualTo(3))
                .anySatisfy(countPizza -> this.softly.assertThat(countPizza)
                        .isEqualTo(2));
    }

}
