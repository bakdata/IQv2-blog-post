package com.bakdata.kafka.example.utils;

import com.bakdata.kafka.example.read.Storage;
import lombok.experimental.UtilityClass;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;

import java.util.*;
import java.util.stream.Collectors;

@UtilityClass
public class QueryHelper {

    /**
     * Executes a state store query on a specific instance of a Kafka Streams application.
     *
     * @param <R>        The type of result expected from the query.
     * @param storage    The {@link Storage} class holds references to the {@link org.apache.kafka.streams.KafkaStreams} instance,
     *                   the name of the state store, and an {@link StateQueryRequest.InStore} object used to build queries targeting that state store.
     *                   It is a final class, ensuring immutability, and is constructed via a static factory method.
     * @param metadata   The metadata that contains information about the Kafka topic's partitions.
     *                   It is used to determine which partitions to query in the state store.
     * @param rangeQuery The query to execute on the state store. This is an instance of {@link Query},
     *                   parameterized with the expected result type.
     * @return A {@link StateQueryResult} containing the result of the query, which includes the query
     * result and possibly additional execution information (e.g., which partitions were queried,
     * how long the query took).
     * @see StreamsMetadata
     */
    public static <R> StateQueryResult<R> queryInstance(final Storage storage, final StreamsMetadata metadata, final Query<R> rangeQuery) {
        final Set<Integer> topicPartitions = metadata.topicPartitions()
                .stream()
                .map(TopicPartition::partition)
                .collect(Collectors.toSet());

        final StateQueryRequest<R> queryRequest =
                storage.getInStore()
                        .withQuery(rangeQuery)
                        .withPartitions(topicPartitions)
                        .enableExecutionInfo();

        return storage.getStreams()
                .query(queryRequest);
    }

    public static <K, V, R extends Iterator<KeyValue<K, V>>> List<V> gatherQueryResults(final StateQueryResult<R> result) {
        final Map<Integer, QueryResult<R>> allPartitionsResult =
                result.getPartitionResults();
        final List<V> aggregationResult = new ArrayList<>();
        allPartitionsResult.forEach(
                (key, queryResult) ->
                        queryResult.getResult()
                                .forEachRemaining(kv -> aggregationResult.add(kv.value))
        );
        return aggregationResult;
    }
}
