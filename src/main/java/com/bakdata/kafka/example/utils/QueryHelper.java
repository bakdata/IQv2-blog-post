package com.bakdata.kafka.example.utils;

import com.bakdata.kafka.example.read.Storage;
import lombok.experimental.UtilityClass;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

    /**
     * Gathers query results from a given StateQueryResult by iterating over all partitions
     * and collecting the elements from each partition into a list.
     *
     * @param <T>              The type of elements contained in the result (e.g., {@code KeyValue<K, V>} or {@code VersionedRecord<V>}).
     * @param <R>              The type of iterator that provides the result elements.
     * @param stateQueryResult The state query result containing partitioned results, where each partition contains
     *                         an iterator of elements.
     * @return A list of all the elements collected from the partitioned query results.
     */
    public static <T, R extends Iterator<T>> List<T> gatherQueryResults(final StateQueryResult<R> stateQueryResult) {
        final Map<Integer, QueryResult<R>> allPartitionsResult = stateQueryResult.getPartitionResults();

        return allPartitionsResult.values()
                .stream()
                .map(QueryResult::getResult)
                .flatMap(result -> StreamSupport.stream(Spliterators.spliteratorUnknownSize(result, 0), false))
                .toList();
    }

    public static <T> Optional<T> getQueryResults(final QueryResult<? extends T> onlyPartitionResult) {
        return hasSuccessfulResult(onlyPartitionResult) ? Optional.of(onlyPartitionResult.getResult())
                : Optional.empty();
    }

    private static boolean hasSuccessfulResult(final QueryResult<?> onlyPartitionResult) {
        return onlyPartitionResult != null && onlyPartitionResult.isSuccess();
    }
}
