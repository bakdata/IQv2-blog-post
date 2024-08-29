package com.bakdata.kafka.example;

import com.bakdata.kafka.example.model.CustomerSession;
import com.bakdata.kafka.example.read.Service;
import net.mguenther.kafka.junit.KeyValue;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

import static java.lang.Thread.sleep;

@ExtendWith(SoftAssertionsExtension.class)
class SessionKeyValueIntegrationTest extends AbstractIntegrationTest {

    private final Service<String, CustomerSession> sessionKeyValueStoreApp =
            KeyValueStoreApplication.startApplication(StoreType.SESSION_KEY_VALUE);

    @Override
    protected Collection<KeyValue<String, String>> createRecords() {
        return List.of(
                // Orders for Customer C123
                // session 1
                new KeyValue<>("order-1", "{\"customerId\": \"C123\", \"menuItem\": \"Pasta\", \"timestamp\": 3600000}"),
                new KeyValue<>("order-2", "{\"customerId\": \"C123\", \"menuItem\": \"Salad\", \"timestamp\": 3600300}"),
                new KeyValue<>("order-3", "{\"customerId\": \"C123\", \"menuItem\": \"Pasta\", \"timestamp\": 3600900}"),
                // session 2
                new KeyValue<>("order-4", "{\"customerId\": \"C123\", \"menuItem\": \"Soda\", \"timestamp\": 7200000}"),
                new KeyValue<>("order-5", "{\"customerId\": \"C123\", \"menuItem\": \"Ice-Cream\", \"timestamp\": 7200300}"),

                // Orders for Customer C124
                // session 1
                new KeyValue<>("order-6", "{\"customerId\": \"C124\", \"menuItem\": \"Burger\", \"timestamp\": 3600500}"),
                new KeyValue<>("order-7", "{\"customerId\": \"C124\", \"menuItem\": \"Fries\", \"timestamp\": 3600800}"),
                new KeyValue<>("order-8", "{\"customerId\": \"C124\", \"menuItem\": \"Coke\", \"timestamp\": 3601500}"),
                // session 2
                new KeyValue<>("order-9", "{\"customerId\": \"C124\", \"menuItem\": \"Pizza\", \"timestamp\": 7200000}"),
                new KeyValue<>("order-10", "{\"customerId\": \"C124\", \"menuItem\": \"Water\", \"timestamp\": 7201000}"),


                // Orders for Customer C125
                // session 1
                new KeyValue<>("order-11", "{\"customerId\": \"C125\", \"menuItem\": \"Steak\", \"timestamp\": 3600000}"),
                new KeyValue<>("order-12", "{\"customerId\": \"C125\", \"menuItem\": \"Wine\", \"timestamp\": 3600300}")
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        this.sessionKeyValueStoreApp.close();
        super.tearDown();
    }

    @Test
    void shouldQueryCorrectWhenRangeQueryForKeyIsRequested() throws InterruptedException {
        sleep(10000);
        final List<CustomerSession> aggregatedOrder = this.sessionKeyValueStoreApp
                .getSessionRangeForKey("C123");

        this.softly.assertThat(aggregatedOrder)
                .hasSize(2)
                .anySatisfy(firstSession -> {
                    this.softly.assertThat(firstSession.start())
                            .isEqualTo(Instant.ofEpochMilli(3600000));
                    this.softly.assertThat(firstSession.end())
                            .isEqualTo(Instant.ofEpochMilli(3600900));
                    this.softly.assertThat(firstSession.count())
                            .isEqualTo(3);
                })
                .anySatisfy(secondSession -> {
                    this.softly.assertThat(secondSession.start())
                            .isEqualTo(Instant.ofEpochMilli(7200000));
                    this.softly.assertThat(secondSession.end())
                            .isEqualTo(Instant.ofEpochMilli(7200300));
                    this.softly.assertThat(secondSession.count())
                            .isEqualTo(2);
                });
    }

}
