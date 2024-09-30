package com.bakdata.kafka.example.utils;

import com.bakdata.kafka.example.read.Storage;
import lombok.experimental.UtilityClass;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterators;
import java.util.function.Function;
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

    /**
     * Retrieves the result from a {@link QueryResult} if it has a successful result.
     *
     * @param <T>         the type of the result object
     * @param queryResult the {@link QueryResult} containing the result
     * @return an {@link Optional} containing the result if the query was successful, otherwise {@link Optional#empty()}
     */
    public static <T> Optional<T> getQueryResults(final QueryResult<? extends T> queryResult) {
        return hasSuccessfulResult(queryResult)
                ? Optional.of(queryResult.getResult())
                : Optional.empty();
    }

    /**
     * Executes a query on the first available {@link StreamsMetadata} in the provided collection.
     * <p>
     * <b>{@code findFirst} only works because in our scenario we don't consider multiple running instances.</b>
     *
     * <p>This method takes a collection of {@link StreamsMetadata} and a function that defines
     * how to extract or transform the metadata into a list of results. The function is applied to
     * the first element in the collection, if present. If no metadata is available, an empty list is returned.
     *
     * @param <V>             the type of the result contained in the list
     * @param streamsMetadata a collection of {@link StreamsMetadata} to search through
     * @param function        a function that defines how to query the {@link StreamsMetadata} and extract a list of results
     * @return a list of results produced by the function, or an empty list if no metadata is found
     */
    public static <V> List<V> executeQuery(final Collection<? extends StreamsMetadata> streamsMetadata, final Function<? super StreamsMetadata, List<V>> function) {
        return streamsMetadata.stream()
                .findFirst()
                .map(function)
                .orElse(Collections.emptyList());
    }

    private static boolean hasSuccessfulResult(final QueryResult<?> onlyPartitionResult) {
        return onlyPartitionResult != null && onlyPartitionResult.isSuccess();
    }
}
