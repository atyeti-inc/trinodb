/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import io.trino.plugin.mongodb.MongoIndex.MongodbIndexKey;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.mongodb.MongoErrorCode.MONGODB_CLUSTER_ERROR;
import static io.trino.plugin.mongodb.MongoSessionProperties.DYNAMIC_FILTERING_WAIT_TIMEOUT;
import static io.trino.plugin.mongodb.MongoSessionProperties.MIN_RECORDS_PER_SPLIT;
import static io.trino.plugin.mongodb.MongoSessionProperties.PARALLEL_SPLIT_ENABLED;
import static io.trino.plugin.mongodb.MongoSessionProperties.SPLIT_SIZE;
import static io.trino.plugin.mongodb.MongoSessionProperties.SPLIT_STRATEGY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMongoSplitManager
{
    @Test
    public void testSingleSplitSource()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address));
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .build();

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                new MongoTableHandle(new SchemaTableName("schema", "table"), new RemoteTableName("schema", "table"), Optional.empty()),
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(1);
        MongoSplit split = (MongoSplit) splits.get(0);
        assertThat(split.getAddresses()).containsExactly(address);
        assertThat(splitSource.isFinished()).isTrue();
    }

    @Test
    public void testFallbackWhenParallelSplitDisabled()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        BsonDocument shardKey = new BsonDocument("_id", new BsonInt32(1));
        MongoShardedCollectionMetadata metadata = new MongoShardedCollectionMetadata(shardKey, Optional.empty());
        List<MongoChunk> chunks = List.of(
                new MongoChunk(new BsonDocument("_id", new BsonMinKey()), new BsonDocument("_id", new BsonInt32(50)), "shard0"),
                new MongoChunk(new BsonDocument("_id", new BsonInt32(50)), new BsonDocument("_id", new BsonMaxKey()), "shard1"));

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        // When parallel splitting is enabled (default), chunk splitting is invoked returning multi-splits
        TestingMongoSession enabledSessionMongo = new TestingMongoSession()
                .setShardedMetadata(metadata)
                .setChunks(chunks)
                .setEstimatedDocumentCount(10_000);
        MongoSplitManager enabledSplitManager = new MongoSplitManager(() -> List.of(address), enabledSessionMongo);

        ConnectorSession enabledSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(PARALLEL_SPLIT_ENABLED, true))
                .build();

        ConnectorSplitSource enabledSplitSource = enabledSplitManager.getSplits(
                new MongoTransactionHandle(),
                enabledSession,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        assertThat(enabledSessionMongo.isShardedMetadataCalled()).isTrue();
        List<ConnectorSplit> enabledSplits = enabledSplitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(enabledSplits).hasSize(2);

        // When parallel splitting is disabled, fallback routes directly to single split and bypasses parallel split generation
        TestingMongoSession disabledSessionMongo = new TestingMongoSession()
                .setShardedMetadata(metadata)
                .setChunks(chunks)
                .setEstimatedDocumentCount(10_000);
        MongoSplitManager disabledSplitManager = new MongoSplitManager(() -> List.of(address), disabledSessionMongo);

        ConnectorSession disabledSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(PARALLEL_SPLIT_ENABLED, false))
                .build();

        ConnectorSplitSource disabledSplitSource = disabledSplitManager.getSplits(
                new MongoTransactionHandle(),
                disabledSession,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        assertThat(disabledSessionMongo.isShardedMetadataCalled()).isFalse();
        List<ConnectorSplit> disabledSplits = disabledSplitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(disabledSplits).hasSize(1);
        MongoSplit split = (MongoSplit) disabledSplits.get(0);
        assertThat(split.getAddresses()).containsExactly(address);
        assertThat(disabledSplitSource.isFinished()).isTrue();
    }

    @Test
    public void testDynamicFilteringWaitTimeoutSessionPropertyInspection()
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address));
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(DYNAMIC_FILTERING_WAIT_TIMEOUT, "10s"))
                .build();

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                new MongoTableHandle(new SchemaTableName("schema", "table"), new RemoteTableName("schema", "table"), Optional.empty()),
                Set.of(),
                Constraint.alwaysTrue());

        assertThat(splitSource.getRequestedDynamicFilterWaitTimeoutMillis()).isEqualTo(10_000L);
    }

    @Test
    public void testShortCircuitZeroSplitsOnDomainNone()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        TestingMongoSession mongoSession = new TestingMongoSession();
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(PARALLEL_SPLIT_ENABLED, true))
                .build();

        // 1. Table handle constraint is none
        MongoTableHandle noneTableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty(),
                TupleDomain.none(),
                ImmutableSet.of(),
                OptionalInt.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                noneTableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).isEmpty();
        assertThat(splitSource.isFinished()).isTrue();
        assertThat(mongoSession.isSampleCalled()).isFalse();

        // 2. Constraint summary is none
        MongoTableHandle allTableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        ConnectorSplitSource splitSourceSummaryNone = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                allTableHandle,
                Set.of(),
                new Constraint(TupleDomain.none()));

        List<ConnectorSplit> splitsSummaryNone = splitSourceSummaryNone.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splitsSummaryNone).isEmpty();
        assertThat(splitSourceSummaryNone.isFinished()).isTrue();
        assertThat(mongoSession.isSampleCalled()).isFalse();
    }

    @Test
    public void testFallbackOnTableFunctionQuery()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        TestingMongoSession mongoSession = new TestingMongoSession();
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(PARALLEL_SPLIT_ENABLED, true))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.of("{x: 1}"));

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(1);
        MongoSplit split = (MongoSplit) splits.get(0);
        assertThat(split.getPartitionBounds()).isEmpty();
        assertThat(mongoSession.isSampleCalled()).isFalse();
    }

    @Test
    public void testFallbackOnView()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        TestingMongoSession mongoSession = new TestingMongoSession().setView(true);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(PARALLEL_SPLIT_ENABLED, true))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(1);
        MongoSplit split = (MongoSplit) splits.get(0);
        assertThat(split.getPartitionBounds()).isEmpty();
        assertThat(mongoSession.isSampleCalled()).isFalse();
    }

    @Test
    public void testFallbackWhenSessionIsNullInBaseClass()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address));

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(PARALLEL_SPLIT_ENABLED, true))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(1);
        MongoSplit split = (MongoSplit) splits.get(0);
        assertThat(split.getPartitionBounds()).isEmpty();
    }

    @Test
    public void testNoEligibleIndexStrategyAutomatic()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        TestingMongoSession mongoSession = new TestingMongoSession()
                .setIndexes(List.of(new MongoIndex(List.of(new MongodbIndexKey("_id", Optional.empty()))))); // hashed only
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_STRATEGY, "AUTOMATIC"))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(1);
        assertThat(((MongoSplit) splits.get(0)).getPartitionBounds()).isEmpty();
    }

    @Test
    public void testNoEligibleIndexStrategyRangeThrowsNotSupported()
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        TestingMongoSession mongoSession = new TestingMongoSession()
                .setIndexes(List.of(new MongoIndex(List.of(new MongodbIndexKey("_id", Optional.empty()))))); // hashed only
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_STRATEGY, "RANGE"))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        assertThatThrownBy(() -> splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("does not have a range-scannable index for range partitioning")
                .matches(e -> ((TrinoException) e).getErrorCode().equals(NOT_SUPPORTED.toErrorCode()));
    }

    @Test
    public void testSmallOrEmptyTableFallback()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        TestingMongoSession mongoSession = new TestingMongoSession()
                .setIndexes(List.of(new MongoIndex(List.of(new MongodbIndexKey("_id", Optional.of(SortOrder.ASC_NULLS_LAST))))))
                .setEstimatedDocumentCount(0);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(PARALLEL_SPLIT_ENABLED, true))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        // estimatedDocCount == 0
        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(1);
        assertThat(((MongoSplit) splits.get(0)).getPartitionBounds()).isEmpty();

        // estimatedDocCount < minRecordsPerSplit
        mongoSession.setEstimatedDocumentCount(100);
        ConnectorSession minRecordsSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        MIN_RECORDS_PER_SPLIT, 500L))
                .build();

        ConnectorSplitSource minRecordsSplitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                minRecordsSession,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> minRecordsSplits = minRecordsSplitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(minRecordsSplits).hasSize(1);
        assertThat(((MongoSplit) minRecordsSplits.get(0)).getPartitionBounds()).isEmpty();
    }

    @Test
    public void testTrapPreventionCollectionSizeZero()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        List<BsonValue> samples = IntStream.range(0, 100)
                .mapToObj(BsonInt32::new)
                .collect(toImmutableList());

        TestingMongoSession mongoSession = new TestingMongoSession()
                .setIndexes(List.of(new MongoIndex(List.of(new MongodbIndexKey("_id", Optional.of(SortOrder.ASC_NULLS_LAST))))))
                .setEstimatedDocumentCount(10_000)
                .setCollectionSize(0) // Simulated Atlas / collStats failure
                .setSampleKeys(samples);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        MIN_RECORDS_PER_SPLIT, 2_500L))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        // 10,000 / 2,500 = 4 splits, not capped at 1
        assertThat(splits).hasSize(4);
    }

    @Test
    public void testMultiSplitGenerationExhaustiveAndDisjoint()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        List<BsonValue> samples = IntStream.range(0, 100)
                .mapToObj(BsonInt32::new)
                .collect(toImmutableList());

        TestingMongoSession mongoSession = new TestingMongoSession()
                .setIndexes(List.of(new MongoIndex(List.of(new MongodbIndexKey("_id", Optional.of(SortOrder.ASC_NULLS_LAST))))))
                .setEstimatedDocumentCount(10_000)
                .setCollectionSize(400 * 1024 * 1024) // 400 MB
                .setSampleKeys(samples);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_SIZE, "100MB", // 400MB / 100MB = 4 splits
                        MIN_RECORDS_PER_SPLIT, 1_000L))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(4);

        // Verify split boundaries
        MongoSplit split0 = (MongoSplit) splits.get(0);
        MongoSplit split1 = (MongoSplit) splits.get(1);
        MongoSplit split2 = (MongoSplit) splits.get(2);
        MongoSplit split3 = (MongoSplit) splits.get(3);

        MongoPartitionBounds bounds0 = split0.getPartitionBounds().orElseThrow();
        MongoPartitionBounds bounds1 = split1.getPartitionBounds().orElseThrow();
        MongoPartitionBounds bounds2 = split2.getPartitionBounds().orElseThrow();
        MongoPartitionBounds bounds3 = split3.getPartitionBounds().orElseThrow();

        assertThat(bounds0.keyNames()).containsExactly("_id");
        assertThat(bounds0.lowerInclusive()).isTrue();
        assertThat(bounds0.upperInclusive()).isFalse();
        assertThat(bounds0.parseLowerBoundDocument().orElseThrow().get("_id")).isInstanceOf(BsonMinKey.class);

        assertThat(bounds1.lowerInclusive()).isTrue();
        assertThat(bounds1.upperInclusive()).isFalse();
        assertThat(bounds1.parseLowerBoundDocument()).isEqualTo(bounds0.parseUpperBoundDocument());

        assertThat(bounds2.lowerInclusive()).isTrue();
        assertThat(bounds2.upperInclusive()).isFalse();
        assertThat(bounds2.parseLowerBoundDocument()).isEqualTo(bounds1.parseUpperBoundDocument());

        assertThat(bounds3.lowerInclusive()).isTrue();
        assertThat(bounds3.upperInclusive()).isTrue();
        assertThat(bounds3.parseLowerBoundDocument()).isEqualTo(bounds2.parseUpperBoundDocument());
        assertThat(bounds3.parseUpperBoundDocument().orElseThrow().get("_id")).isInstanceOf(BsonMaxKey.class);

        // Verify generated MongoSplitQueryBuilder filters
        assertThat(MongoSplitQueryBuilder.buildSplitFilter(bounds0).orElseThrow().toJson())
                .isEqualTo("{\"_id\": {\"$lt\": 25}}");
        assertThat(MongoSplitQueryBuilder.buildSplitFilter(bounds1).orElseThrow().toJson())
                .isEqualTo("{\"_id\": {\"$gte\": 25, \"$lt\": 50}}");
        assertThat(MongoSplitQueryBuilder.buildSplitFilter(bounds2).orElseThrow().toJson())
                .isEqualTo("{\"_id\": {\"$gte\": 50, \"$lt\": 75}}");
        assertThat(MongoSplitQueryBuilder.buildSplitFilter(bounds3).orElseThrow().toJson())
                .isEqualTo("{\"_id\": {\"$gte\": 75}}");
    }

    @Test
    public void testLowCardinalityBoundaryDeduplication()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        // All sample keys are identical (constant value)
        List<BsonValue> samples = IntStream.range(0, 100)
                .mapToObj(_ -> new BsonInt32(42))
                .collect(toImmutableList());

        TestingMongoSession mongoSession = new TestingMongoSession()
                .setIndexes(List.of(new MongoIndex(List.of(new MongodbIndexKey("_id", Optional.of(SortOrder.ASC_NULLS_LAST))))))
                .setEstimatedDocumentCount(10_000)
                .setCollectionSize(400 * 1024 * 1024)
                .setSampleKeys(samples);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_SIZE, "100MB",
                        MIN_RECORDS_PER_SPLIT, 1_000L))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        // Since all keys are 42, deduplication yields exactly 1 boundary (42), generating 2 splits: [MinKey, 42) and [42, MaxKey]
        assertThat(splits).hasSize(2);

        // If sample has fewer than 2 elements, falls back to single split
        mongoSession.setSampleKeys(List.of(new BsonInt32(42)));
        ConnectorSplitSource fallbackSplitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());
        List<ConnectorSplit> fallbackSplits = fallbackSplitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(fallbackSplits).hasSize(1);
        assertThat(((MongoSplit) fallbackSplits.get(0)).getPartitionBounds()).isEmpty();
    }

    @Test
    public void testErrorTranslationClusterError()
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        TestingMongoSession mongoSession = new TestingMongoSession()
                .setIndexes(List.of(new MongoIndex(List.of(new MongodbIndexKey("_id", Optional.of(SortOrder.ASC_NULLS_LAST))))))
                .setEstimatedDocumentCount(10_000)
                .setCollectionSize(400 * 1024 * 1024)
                .setSampleException(new MongoTimeoutException("Timed out connecting to cluster"));
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_SIZE, "100MB",
                        MIN_RECORDS_PER_SPLIT, 1_000L))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        assertThatThrownBy(() -> splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Failed to communicate with MongoDB during split generation")
                .matches(e -> ((TrinoException) e).getErrorCode().equals(MONGODB_CLUSTER_ERROR.toErrorCode()));
    }

    @Test
    public void testUnexpectedErrorInAutomaticFallsBack()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        TestingMongoSession mongoSession = new TestingMongoSession()
                .setIndexes(List.of(new MongoIndex(List.of(new MongodbIndexKey("_id", Optional.of(SortOrder.ASC_NULLS_LAST))))))
                .setEstimatedDocumentCount(10_000)
                .setCollectionSize(400 * 1024 * 1024)
                .setSampleException(new RuntimeException("Simulated unexpected failure"));
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_STRATEGY, "AUTOMATIC",
                        SPLIT_SIZE, "100MB",
                        MIN_RECORDS_PER_SPLIT, 1_000L))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(1);
        assertThat(((MongoSplit) splits.get(0)).getPartitionBounds()).isEmpty();
    }

    @Test
    public void testUnexpectedErrorInRangeThrowsClusterError()
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        TestingMongoSession mongoSession = new TestingMongoSession()
                .setIndexes(List.of(new MongoIndex(List.of(new MongodbIndexKey("_id", Optional.of(SortOrder.ASC_NULLS_LAST))))))
                .setEstimatedDocumentCount(10_000)
                .setCollectionSize(400 * 1024 * 1024)
                .setSampleException(new RuntimeException("Simulated unexpected failure"));
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_STRATEGY, "RANGE",
                        SPLIT_SIZE, "100MB",
                        MIN_RECORDS_PER_SPLIT, 1_000L))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        assertThatThrownBy(() -> splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Failed to generate range splits for table")
                .matches(e -> ((TrinoException) e).getErrorCode().equals(MONGODB_CLUSTER_ERROR.toErrorCode()));
    }

    @Test
    public void testChunkSplitsBasicSingleKey()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        HostAddress shard0Address = HostAddress.fromParts("10.0.0.1", 27018);
        HostAddress shard1Address = HostAddress.fromParts("10.0.0.2", 27018);

        BsonDocument shardKey = new BsonDocument("_id", new BsonInt32(1));
        MongoShardedCollectionMetadata metadata = new MongoShardedCollectionMetadata(shardKey, Optional.empty());
        List<MongoChunk> chunks = List.of(
                new MongoChunk(new BsonDocument("_id", new BsonMinKey()), new BsonDocument("_id", new BsonInt32(100)), "shard0"),
                new MongoChunk(new BsonDocument("_id", new BsonInt32(100)), new BsonDocument("_id", new BsonInt32(200)), "shard1"),
                new MongoChunk(new BsonDocument("_id", new BsonInt32(200)), new BsonDocument("_id", new BsonMaxKey()), "shard0"));
        Map<String, List<HostAddress>> shardAddresses = ImmutableMap.of(
                "shard0", List.of(shard0Address),
                "shard1", List.of(shard1Address));

        TestingMongoSession mongoSession = new TestingMongoSession()
                .setShardedMetadata(metadata)
                .setChunks(chunks)
                .setShardAddresses(shardAddresses)
                .setEstimatedDocumentCount(10_000);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_STRATEGY, "CHUNK"))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(3);

        MongoSplit split0 = (MongoSplit) splits.get(0);
        assertThat(split0.getAddresses()).containsExactly(shard0Address);
        MongoPartitionBounds bounds0 = split0.getPartitionBounds().orElseThrow();
        assertThat(bounds0.keyNames()).containsExactly("_id");
        assertThat(bounds0.lowerInclusive()).isTrue();
        assertThat(bounds0.upperInclusive()).isFalse();
        assertThat(bounds0.parseLowerBoundDocument().orElseThrow().get("_id")).isInstanceOf(BsonMinKey.class);
        assertThat(bounds0.parseUpperBoundDocument().orElseThrow().getInt32("_id").getValue()).isEqualTo(100);

        MongoSplit split1 = (MongoSplit) splits.get(1);
        assertThat(split1.getAddresses()).containsExactly(shard1Address);
        MongoPartitionBounds bounds1 = split1.getPartitionBounds().orElseThrow();
        assertThat(bounds1.keyNames()).containsExactly("_id");
        assertThat(bounds1.lowerInclusive()).isTrue();
        assertThat(bounds1.upperInclusive()).isFalse();
        assertThat(bounds1.parseLowerBoundDocument().orElseThrow().getInt32("_id").getValue()).isEqualTo(100);
        assertThat(bounds1.parseUpperBoundDocument().orElseThrow().getInt32("_id").getValue()).isEqualTo(200);

        MongoSplit split2 = (MongoSplit) splits.get(2);
        assertThat(split2.getAddresses()).containsExactly(shard0Address);
        MongoPartitionBounds bounds2 = split2.getPartitionBounds().orElseThrow();
        assertThat(bounds2.keyNames()).containsExactly("_id");
        assertThat(bounds2.lowerInclusive()).isTrue();
        assertThat(bounds2.upperInclusive()).isFalse();
        assertThat(bounds2.parseLowerBoundDocument().orElseThrow().getInt32("_id").getValue()).isEqualTo(200);
        assertThat(bounds2.parseUpperBoundDocument().orElseThrow().get("_id")).isInstanceOf(BsonMaxKey.class);
    }

    @Test
    public void testChunkSplitsCompoundKey()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        BsonDocument shardKey = new BsonDocument("region", new BsonInt32(1)).append("account_id", new BsonInt32(1));
        MongoShardedCollectionMetadata metadata = new MongoShardedCollectionMetadata(shardKey, Optional.empty());
        List<MongoChunk> chunks = List.of(
                new MongoChunk(
                        new BsonDocument("region", new BsonMinKey()).append("account_id", new BsonMinKey()),
                        new BsonDocument("region", new BsonString("us-east")).append("account_id", new BsonInt32(100)),
                        "shard0"),
                new MongoChunk(
                        new BsonDocument("region", new BsonString("us-east")).append("account_id", new BsonInt32(100)),
                        new BsonDocument("region", new BsonMaxKey()).append("account_id", new BsonMaxKey()),
                        "shard1"));

        TestingMongoSession mongoSession = new TestingMongoSession()
                .setShardedMetadata(metadata)
                .setChunks(chunks)
                .setEstimatedDocumentCount(10_000);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_STRATEGY, "CHUNK"))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(2);

        MongoSplit split0 = (MongoSplit) splits.get(0);
        MongoPartitionBounds bounds0 = split0.getPartitionBounds().orElseThrow();
        assertThat(bounds0.keyNames()).containsExactly("region", "account_id");
        assertThat(bounds0.lowerInclusive()).isTrue();
        assertThat(bounds0.upperInclusive()).isFalse();
        assertThat(bounds0.parseLowerBoundDocument().orElseThrow().get("region")).isInstanceOf(BsonMinKey.class);
        assertThat(bounds0.parseUpperBoundDocument().orElseThrow().getString("region").getValue()).isEqualTo("us-east");
        assertThat(bounds0.parseUpperBoundDocument().orElseThrow().getInt32("account_id").getValue()).isEqualTo(100);

        MongoSplit split1 = (MongoSplit) splits.get(1);
        MongoPartitionBounds bounds1 = split1.getPartitionBounds().orElseThrow();
        assertThat(bounds1.keyNames()).containsExactly("region", "account_id");
        assertThat(bounds1.lowerInclusive()).isTrue();
        assertThat(bounds1.upperInclusive()).isFalse();
        assertThat(bounds1.parseLowerBoundDocument().orElseThrow().getString("region").getValue()).isEqualTo("us-east");
        assertThat(bounds1.parseLowerBoundDocument().orElseThrow().getInt32("account_id").getValue()).isEqualTo(100);
        assertThat(bounds1.parseUpperBoundDocument().orElseThrow().get("region")).isInstanceOf(BsonMaxKey.class);
    }

    @Test
    public void testChunkSplitsFailFastOnHashedShardKey()
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        BsonDocument shardKey = new BsonDocument("_id", new BsonString("hashed"));
        MongoShardedCollectionMetadata metadata = new MongoShardedCollectionMetadata(shardKey, Optional.empty());
        TestingMongoSession mongoSession = new TestingMongoSession()
                .setShardedMetadata(metadata);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_STRATEGY, "CHUNK"))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        assertThatThrownBy(() -> splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("hashed shard key")
                .matches(e -> ((TrinoException) e).getErrorCode().equals(NOT_SUPPORTED.toErrorCode()));
    }

    @Test
    public void testChunkSplitsAutomaticFallbackOnHashedShardKey()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        BsonDocument shardKey = new BsonDocument("_id", new BsonString("hashed"));
        MongoShardedCollectionMetadata metadata = new MongoShardedCollectionMetadata(shardKey, Optional.empty());

        List<BsonValue> sampleKeys = IntStream.range(0, 100)
                .mapToObj(BsonInt32::new)
                .collect(toImmutableList());

        TestingMongoSession mongoSession = new TestingMongoSession()
                .setShardedMetadata(metadata)
                .setIndexes(List.of(new MongoIndex(List.of(new MongodbIndexKey("_id", Optional.of(SortOrder.ASC_NULLS_LAST))))))
                .setEstimatedDocumentCount(10_000)
                .setCollectionSize(400 * 1024 * 1024)
                .setSampleKeys(sampleKeys);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_STRATEGY, "AUTOMATIC",
                        SPLIT_SIZE, "100MB",
                        MIN_RECORDS_PER_SPLIT, 1_000L))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        assertThat(mongoSession.isSampleCalled()).isTrue();
        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(4);
    }

    @Test
    public void testChunkSplitsLocalityPreferences()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        HostAddress shardHost1 = HostAddress.fromParts("shard-host-1", 27018);
        HostAddress shardHost2 = HostAddress.fromParts("shard-host-2", 27018);

        BsonDocument shardKey = new BsonDocument("_id", new BsonInt32(1));
        MongoShardedCollectionMetadata metadata = new MongoShardedCollectionMetadata(shardKey, Optional.empty());
        List<MongoChunk> chunks = List.of(
                new MongoChunk(new BsonDocument("_id", new BsonMinKey()), new BsonDocument("_id", new BsonInt32(50)), "shard0"),
                new MongoChunk(new BsonDocument("_id", new BsonInt32(50)), new BsonDocument("_id", new BsonMaxKey()), "shard1"));
        Map<String, List<HostAddress>> shardAddresses = ImmutableMap.of(
                "shard0", List.of(shardHost1),
                "shard1", List.of(shardHost2));

        TestingMongoSession mongoSession = new TestingMongoSession()
                .setShardedMetadata(metadata)
                .setChunks(chunks)
                .setShardAddresses(shardAddresses)
                .setEstimatedDocumentCount(10_000);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_STRATEGY, "CHUNK"))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(2);
        assertThat(splits.get(0).getAddresses()).containsExactly(shardHost1);
        assertThat(splits.get(1).getAddresses()).containsExactly(shardHost2);
    }

    @Test
    public void testChunkSplitsShardAddressFallback()
            throws Exception
    {
        HostAddress defaultAddress = HostAddress.fromParts("127.0.0.1", 27017);
        BsonDocument shardKey = new BsonDocument("_id", new BsonInt32(1));
        MongoShardedCollectionMetadata metadata = new MongoShardedCollectionMetadata(shardKey, Optional.empty());
        List<MongoChunk> chunks = List.of(
                new MongoChunk(new BsonDocument("_id", new BsonMinKey()), new BsonDocument("_id", new BsonInt32(50)), "shard_unknown"),
                new MongoChunk(new BsonDocument("_id", new BsonInt32(50)), new BsonDocument("_id", new BsonMaxKey()), "shard_unknown"));

        TestingMongoSession mongoSession = new TestingMongoSession()
                .setShardedMetadata(metadata)
                .setChunks(chunks)
                .setShardAddresses(ImmutableMap.of())
                .setEstimatedDocumentCount(10_000);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(defaultAddress), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_STRATEGY, "CHUNK"))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(2);
        assertThat(splits.get(0).getAddresses()).containsExactly(defaultAddress);
        assertThat(splits.get(1).getAddresses()).containsExactly(defaultAddress);
    }

    @Test
    public void testChunkSplitsFailFastWhenNotSharded()
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        TestingMongoSession mongoSession = new TestingMongoSession()
                .setShardedMetadata(null);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_STRATEGY, "CHUNK"))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        assertThatThrownBy(() -> splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("is not sharded")
                .matches(e -> ((TrinoException) e).getErrorCode().equals(NOT_SUPPORTED.toErrorCode()));
    }

    @Test
    public void testChunkSplitsFailFastOnPermissionDenied()
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        MongoCommandException unauthorizedException = new MongoCommandException(
                new BsonDocument("ok", new BsonInt32(0)).append("code", new BsonInt32(13)).append("errmsg", new BsonString("Unauthorized")),
                new ServerAddress("127.0.0.1", 27017));
        TestingMongoSession mongoSession = new TestingMongoSession()
                .setShardedMetadataException(unauthorizedException);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_STRATEGY, "CHUNK"))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        assertThatThrownBy(() -> splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Failed to access MongoDB sharded cluster metadata for table")
                .matches(e -> ((TrinoException) e).getErrorCode().equals(MONGODB_CLUSTER_ERROR.toErrorCode()));
    }

    @Test
    public void testChunkSplitsAutomaticFallbackOnPermissionDenied()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        MongoCommandException unauthorizedException = new MongoCommandException(
                new BsonDocument("ok", new BsonInt32(0)).append("code", new BsonInt32(13)).append("errmsg", new BsonString("Unauthorized")),
                new ServerAddress("127.0.0.1", 27017));

        List<BsonValue> sampleKeys = IntStream.range(0, 100)
                .mapToObj(BsonInt32::new)
                .collect(toImmutableList());

        TestingMongoSession mongoSession = new TestingMongoSession()
                .setShardedMetadataException(unauthorizedException)
                .setIndexes(List.of(new MongoIndex(List.of(new MongodbIndexKey("_id", Optional.of(SortOrder.ASC_NULLS_LAST))))))
                .setEstimatedDocumentCount(10_000)
                .setCollectionSize(400 * 1024 * 1024)
                .setSampleKeys(sampleKeys);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_STRATEGY, "AUTOMATIC",
                        SPLIT_SIZE, "100MB",
                        MIN_RECORDS_PER_SPLIT, 1_000L))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        assertThat(mongoSession.isSampleCalled()).isTrue();
        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(4);
    }

    @Test
    public void testChunkSplitsAutomaticFallbackWhenNotSharded()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        List<BsonValue> sampleKeys = IntStream.range(0, 100)
                .mapToObj(BsonInt32::new)
                .collect(toImmutableList());

        TestingMongoSession mongoSession = new TestingMongoSession()
                .setShardedMetadata(null)
                .setIndexes(List.of(new MongoIndex(List.of(new MongodbIndexKey("_id", Optional.of(SortOrder.ASC_NULLS_LAST))))))
                .setEstimatedDocumentCount(10_000)
                .setCollectionSize(400 * 1024 * 1024)
                .setSampleKeys(sampleKeys);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_STRATEGY, "AUTOMATIC",
                        SPLIT_SIZE, "100MB",
                        MIN_RECORDS_PER_SPLIT, 1_000L))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        assertThat(mongoSession.isSampleCalled()).isTrue();
        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(4);
    }

    @Test
    public void testChunkSplitsSingleChunkInAutomaticFallsBackToRange()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        BsonDocument shardKey = new BsonDocument("_id", new BsonInt32(1));
        MongoShardedCollectionMetadata metadata = new MongoShardedCollectionMetadata(shardKey, Optional.empty());
        List<MongoChunk> singleChunk = List.of(
                new MongoChunk(new BsonDocument("_id", new BsonMinKey()), new BsonDocument("_id", new BsonMaxKey()), "shard0"));

        List<BsonValue> sampleKeys = IntStream.range(0, 100)
                .mapToObj(BsonInt32::new)
                .collect(toImmutableList());

        TestingMongoSession mongoSession = new TestingMongoSession()
                .setShardedMetadata(metadata)
                .setChunks(singleChunk)
                .setIndexes(List.of(new MongoIndex(List.of(new MongodbIndexKey("_id", Optional.of(SortOrder.ASC_NULLS_LAST))))))
                .setEstimatedDocumentCount(10_000)
                .setCollectionSize(400 * 1024 * 1024)
                .setSampleKeys(sampleKeys);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_STRATEGY, "AUTOMATIC",
                        SPLIT_SIZE, "100MB",
                        MIN_RECORDS_PER_SPLIT, 1_000L))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        assertThat(mongoSession.isSampleCalled()).isTrue();
        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(4);
    }

    @Test
    public void testChunkSplitsBelowMinRecordsPerSplitFallsBackToSingleSplit()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        BsonDocument shardKey = new BsonDocument("_id", new BsonInt32(1));
        MongoShardedCollectionMetadata metadata = new MongoShardedCollectionMetadata(shardKey, Optional.empty());
        List<MongoChunk> chunks = List.of(
                new MongoChunk(new BsonDocument("_id", new BsonMinKey()), new BsonDocument("_id", new BsonInt32(50)), "shard0"),
                new MongoChunk(new BsonDocument("_id", new BsonInt32(50)), new BsonDocument("_id", new BsonMaxKey()), "shard1"));

        TestingMongoSession mongoSession = new TestingMongoSession()
                .setShardedMetadata(metadata)
                .setChunks(chunks)
                .setEstimatedDocumentCount(500);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_STRATEGY, "AUTOMATIC",
                        MIN_RECORDS_PER_SPLIT, 1_000L))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(1);
        assertThat(((MongoSplit) splits.get(0)).getPartitionBounds()).isEmpty();
    }

    @Test
    public void testChunkSplitsZeroEstimatedDocsReturnsSingleSplit()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        BsonDocument shardKey = new BsonDocument("_id", new BsonInt32(1));
        MongoShardedCollectionMetadata metadata = new MongoShardedCollectionMetadata(shardKey, Optional.empty());
        List<MongoChunk> chunks = List.of(
                new MongoChunk(new BsonDocument("_id", new BsonMinKey()), new BsonDocument("_id", new BsonInt32(50)), "shard0"),
                new MongoChunk(new BsonDocument("_id", new BsonInt32(50)), new BsonDocument("_id", new BsonMaxKey()), "shard1"));

        TestingMongoSession mongoSession = new TestingMongoSession()
                .setShardedMetadata(metadata)
                .setChunks(chunks)
                .setEstimatedDocumentCount(0);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_STRATEGY, "AUTOMATIC"))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(1);
        assertThat(((MongoSplit) splits.get(0)).getPartitionBounds()).isEmpty();
    }

    @Test
    public void testChunkSplitsEmptyConstraintSummaryReturnsZeroSplits()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        BsonDocument shardKey = new BsonDocument("_id", new BsonInt32(1));
        MongoShardedCollectionMetadata metadata = new MongoShardedCollectionMetadata(shardKey, Optional.empty());
        List<MongoChunk> chunks = List.of(
                new MongoChunk(new BsonDocument("_id", new BsonMinKey()), new BsonDocument("_id", new BsonInt32(50)), "shard0"),
                new MongoChunk(new BsonDocument("_id", new BsonInt32(50)), new BsonDocument("_id", new BsonMaxKey()), "shard1"));

        TestingMongoSession mongoSession = new TestingMongoSession()
                .setShardedMetadata(metadata)
                .setChunks(chunks)
                .setEstimatedDocumentCount(10_000);
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_STRATEGY, "CHUNK"))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                new Constraint(TupleDomain.none()));

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).isEmpty();
        assertThat(splitSource.isFinished()).isTrue();
    }

    @Test
    public void testChunkSplitsNoChunksFoundThrowsClusterError()
    {
        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        BsonDocument shardKey = new BsonDocument("_id", new BsonInt32(1));
        MongoShardedCollectionMetadata metadata = new MongoShardedCollectionMetadata(shardKey, Optional.empty());
        TestingMongoSession mongoSession = new TestingMongoSession()
                .setShardedMetadata(metadata)
                .setChunks(ImmutableList.of());
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address), mongoSession);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, true,
                        SPLIT_STRATEGY, "CHUNK"))
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        assertThatThrownBy(() -> splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("No chunks found for sharded table")
                .matches(e -> ((TrinoException) e).getErrorCode().equals(MONGODB_CLUSTER_ERROR.toErrorCode()));
    }

    @Test
    public void testParseHostAddresses()
    {
        List<HostAddress> addressesWithRs = MongoSession.parseHostAddresses("rs0/host1:27018,host2:27019");
        assertThat(addressesWithRs).containsExactly(
                HostAddress.fromParts("host1", 27018),
                HostAddress.fromParts("host2", 27019));

        List<HostAddress> addressesStandalone = MongoSession.parseHostAddresses("standalone.host:27017");
        assertThat(addressesStandalone).containsExactly(HostAddress.fromParts("standalone.host", 27017));
    }

    @Test
    public void testMongoShardedCollectionMetadata()
    {
        BsonDocument rangeKey = new BsonDocument("tenant", new BsonInt32(1)).append("created", new BsonInt32(-1));
        MongoShardedCollectionMetadata rangeMetadata = new MongoShardedCollectionMetadata(rangeKey, Optional.empty());
        assertThat(rangeMetadata.shardKeyFields()).containsExactly("tenant", "created");
        assertThat(rangeMetadata.hasHashedShardKey()).isFalse();

        BsonDocument hashedKey = new BsonDocument("_id", new BsonString("hashed"));
        MongoShardedCollectionMetadata hashedMetadata = new MongoShardedCollectionMetadata(hashedKey, Optional.empty());
        assertThat(hashedMetadata.shardKeyFields()).containsExactly("_id");
        assertThat(hashedMetadata.hasHashedShardKey()).isTrue();
    }

    private static class TestingMongoSession
            extends MongoSession
    {
        private boolean isView;
        private List<MongoIndex> indexes = ImmutableList.of();
        private long estimatedDocumentCount;
        private long collectionSize;
        private List<BsonValue> sampleKeys = ImmutableList.of();
        private RuntimeException sampleException;
        private boolean sampleCalled;

        private Optional<MongoShardedCollectionMetadata> shardedMetadata = Optional.empty();
        private List<MongoChunk> chunks = ImmutableList.of();
        private Map<String, List<HostAddress>> shardAddresses = ImmutableMap.of();
        private RuntimeException shardedMetadataException;
        private RuntimeException chunksException;
        private boolean shardedMetadataCalled;
        private boolean chunksCalled;

        public TestingMongoSession()
        {
            super(TESTING_TYPE_MANAGER, () -> null, new MongoClientConfig());
        }

        public TestingMongoSession setView(boolean isView)
        {
            this.isView = isView;
            return this;
        }

        public TestingMongoSession setIndexes(List<MongoIndex> indexes)
        {
            this.indexes = requireNonNull(indexes, "indexes is null");
            return this;
        }

        public TestingMongoSession setEstimatedDocumentCount(long estimatedDocumentCount)
        {
            this.estimatedDocumentCount = estimatedDocumentCount;
            return this;
        }

        public TestingMongoSession setCollectionSize(long collectionSize)
        {
            this.collectionSize = collectionSize;
            return this;
        }

        public TestingMongoSession setSampleKeys(List<BsonValue> sampleKeys)
        {
            this.sampleKeys = requireNonNull(sampleKeys, "sampleKeys is null");
            return this;
        }

        public TestingMongoSession setSampleException(RuntimeException sampleException)
        {
            this.sampleException = sampleException;
            return this;
        }

        public TestingMongoSession setShardedMetadata(MongoShardedCollectionMetadata metadata)
        {
            this.shardedMetadata = Optional.ofNullable(metadata);
            return this;
        }

        public TestingMongoSession setChunks(List<MongoChunk> chunks)
        {
            this.chunks = requireNonNull(chunks, "chunks is null");
            return this;
        }

        public TestingMongoSession setShardAddresses(Map<String, List<HostAddress>> shardAddresses)
        {
            this.shardAddresses = requireNonNull(shardAddresses, "shardAddresses is null");
            return this;
        }

        public TestingMongoSession setShardedMetadataException(RuntimeException exception)
        {
            this.shardedMetadataException = exception;
            return this;
        }

        public TestingMongoSession setChunksException(RuntimeException exception)
        {
            this.chunksException = exception;
            return this;
        }

        public boolean isShardedMetadataCalled()
        {
            return shardedMetadataCalled;
        }

        public boolean isChunksCalled()
        {
            return chunksCalled;
        }

        public boolean isSampleCalled()
        {
            return sampleCalled;
        }

        @Override
        public Optional<MongoShardedCollectionMetadata> getShardedCollectionMetadata(RemoteTableName remoteTableName)
        {
            this.shardedMetadataCalled = true;
            if (shardedMetadataException != null) {
                throw shardedMetadataException;
            }
            return shardedMetadata;
        }

        @Override
        public List<MongoChunk> getChunks(RemoteTableName remoteTableName, MongoShardedCollectionMetadata metadata)
        {
            this.chunksCalled = true;
            if (chunksException != null) {
                throw chunksException;
            }
            return chunks;
        }

        @Override
        public Map<String, List<HostAddress>> getShardAddresses()
        {
            return shardAddresses;
        }

        @Override
        public boolean isView(RemoteTableName remoteTableName)
        {
            return isView;
        }

        @Override
        public List<MongoIndex> getIndexes(String schemaName, String tableName)
        {
            return indexes;
        }

        @Override
        public long getEstimatedDocumentCount(RemoteTableName remoteTableName)
        {
            return estimatedDocumentCount;
        }

        @Override
        public long getCollectionSize(RemoteTableName remoteTableName)
        {
            return collectionSize;
        }

        @Override
        public List<BsonValue> samplePartitionKeys(RemoteTableName remoteTableName, String partitionKey, int sampleSize)
        {
            this.sampleCalled = true;
            if (sampleException != null) {
                throw sampleException;
            }
            return sampleKeys;
        }
    }
}
