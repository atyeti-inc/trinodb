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

import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoTimeoutException;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.plugin.mongodb.MongoErrorCode.MONGODB_CLUSTER_ERROR;
import static io.trino.plugin.mongodb.MongoSessionProperties.DYNAMIC_FILTERING_WAIT_TIMEOUT;
import static io.trino.plugin.mongodb.MongoSessionProperties.MIN_RECORDS_PER_SPLIT;
import static io.trino.plugin.mongodb.MongoSessionProperties.PARALLEL_SPLIT_ENABLED;
import static io.trino.plugin.mongodb.MongoSessionProperties.SPLIT_SIZE;
import static io.trino.plugin.mongodb.MongoSessionProperties.SPLIT_STRATEGY;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMongoSplitManager
{
    private static final HostAddress ADDRESS = HostAddress.fromParts("127.0.0.1", 27017);

    @Test
    public void testSingleSplitSource()
            throws Exception
    {
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(ADDRESS));
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
        assertThat(split.getAddresses()).containsExactly(ADDRESS);
        assertThat(splitSource.isFinished()).isTrue();
    }

    @Test
    public void testFallbackWhenParallelSplitDisabled()
            throws Exception
    {
        AtomicBoolean parallelSplitsCalled = new AtomicBoolean();
        TestingMongoSession mockSession = new TestingMongoSession();
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(ADDRESS), mockSession)
        {
            @Override
            protected ConnectorSplitSource getParallelSplits(
                    ConnectorTransactionHandle transaction,
                    ConnectorSession session,
                    ConnectorTableHandle table,
                    Set<ColumnHandle> dynamicFilterColumns,
                    Constraint constraint,
                    long dynamicFilteringWaitTimeoutMillis)
            {
                parallelSplitsCalled.set(true);
                return new FixedSplitSource(List.of(
                        new MongoSplit(List.of(ADDRESS)),
                        new MongoSplit(List.of(ADDRESS))));
            }
        };

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        // When parallel splitting is enabled (default), getParallelSplits is invoked returning multi-splits
        ConnectorSession enabledSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(PARALLEL_SPLIT_ENABLED, true))
                .build();

        ConnectorSplitSource enabledSplitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                enabledSession,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        assertThat(parallelSplitsCalled.get()).isTrue();
        List<ConnectorSplit> enabledSplits = enabledSplitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(enabledSplits).hasSize(2);

        // When parallel splitting is disabled, fallback routes directly to single split and bypasses getParallelSplits
        parallelSplitsCalled.set(false);
        ConnectorSession disabledSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(PARALLEL_SPLIT_ENABLED, false))
                .build();

        ConnectorSplitSource disabledSplitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                disabledSession,
                tableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        assertThat(parallelSplitsCalled.get()).isFalse();
        List<ConnectorSplit> disabledSplits = disabledSplitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(disabledSplits).hasSize(1);
        MongoSplit split = (MongoSplit) disabledSplits.get(0);
        assertThat(split.getAddresses()).containsExactly(ADDRESS);
        assertThat(disabledSplitSource.isFinished()).isTrue();
    }

    @Test
    public void testDynamicFilteringWaitTimeoutSessionPropertyInspection()
    {
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(ADDRESS));
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
    public void testZeroSplitsOnDomainNone()
            throws Exception
    {
        TestingMongoSession mockSession = new TestingMongoSession();
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(ADDRESS), mockSession);
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .build();

        // 1. TableHandle constraint is TupleDomain.none()
        MongoTableHandle noneTableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty(),
                TupleDomain.none(),
                Set.of(),
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
        assertThat(mockSession.sampled).isFalse();

        // 2. Constraint summary is TupleDomain.none()
        Constraint noneConstraint = new Constraint(TupleDomain.none());
        MongoTableHandle allTableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                allTableHandle,
                Set.of(),
                noneConstraint);

        splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).isEmpty();
        assertThat(splitSource.isFinished()).isTrue();
        assertThat(mockSession.sampled).isFalse();
    }

    @Test
    public void testFallbackOnPtfQuery()
            throws Exception
    {
        TestingMongoSession mockSession = new TestingMongoSession();
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(ADDRESS), mockSession);
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .build();

        MongoTableHandle ptfTableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.of("{ x: 1 }"));

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                ptfTableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(1);
        MongoSplit split = (MongoSplit) splits.get(0);
        assertThat(split.getPartitionBounds()).isEmpty();
        assertThat(mockSession.sampled).isFalse();
    }

    @Test
    public void testFallbackOnView()
            throws Exception
    {
        TestingMongoSession mockSession = new TestingMongoSession();
        mockSession.isView = true;
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(ADDRESS), mockSession);
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .build();

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "view_table"),
                new RemoteTableName("schema", "view_table"),
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
        assertThat(mockSession.sampled).isFalse();
    }

    @Test
    public void testChunkStrategyReturnsSingleSplit()
            throws Exception
    {
        TestingMongoSession mockSession = new TestingMongoSession();
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(ADDRESS), mockSession);
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(SPLIT_STRATEGY, MongoSplitStrategy.CHUNK.name()))
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
        assertThat(mockSession.sampled).isFalse();
    }

    @Test
    public void testMultiSplitGeneration()
            throws Exception
    {
        TestingMongoSession mockSession = new TestingMongoSession();
        mockSession.estimatedDocCount = 10_000L;
        mockSession.collectionSize = 400 * 1024 * 1024L; // 400MB
        mockSession.sampleKeys = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            mockSession.sampleKeys.add(new BsonInt32(i * 10));
        }

        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(ADDRESS), mockSession);
        // Default split_size is 64MB -> 400MB / 64MB = 7 splits (sizeSplits = (400 + 64 - 1)/64 = 7)
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        SPLIT_SIZE, "100MB",
                        MIN_RECORDS_PER_SPLIT, 100L))
                .build();
        // 400MB / 100MB = 4 splits

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

        // Check bounds completeness:
        // Split 0: MinKey -> b0
        MongoSplit split0 = (MongoSplit) splits.get(0);
        assertThat(split0.getPartitionBounds()).isPresent();
        assertThat(split0.getPartitionBounds().get().lowerBound()).hasValueSatisfying(s -> assertThat(s).contains("$minKey"));
        assertThat(split0.getPartitionBounds().get().lowerInclusive()).isTrue();
        assertThat(split0.getPartitionBounds().get().upperInclusive()).isFalse();

        // Split 3 (last): b2 -> MaxKey
        MongoSplit split3 = (MongoSplit) splits.get(3);
        assertThat(split3.getPartitionBounds()).isPresent();
        assertThat(split3.getPartitionBounds().get().upperBound()).hasValueSatisfying(s -> assertThat(s).contains("$maxKey"));
        assertThat(split3.getPartitionBounds().get().lowerInclusive()).isTrue();
        assertThat(split3.getPartitionBounds().get().upperInclusive()).isTrue();
    }

    @Test
    public void testCollectionSizeZeroTrapPrevention()
            throws Exception
    {
        // When collectionSize is 0 (deprecated collStats or permission restrictions),
        // split count must fall back to estimatedDocCount / minRecordsPerSplit rather than collapsing to 1 split.
        TestingMongoSession mockSession = new TestingMongoSession();
        mockSession.estimatedDocCount = 10_000L;
        mockSession.collectionSize = 0L;
        mockSession.sampleKeys = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            mockSession.sampleKeys.add(new BsonInt32(i * 10));
        }

        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(ADDRESS), mockSession);
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        MIN_RECORDS_PER_SPLIT, 2500L))
                .build();
        // 10,000 / 2,500 = 4 splits

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
    }

    @Test
    public void testRangeStrategyThrowsWhenNoRangeIndex()
    {
        TestingMongoSession mockSession = new TestingMongoSession();
        mockSession.indexes = List.of(new MongoIndex(List.of(new MongoIndex.MongodbIndexKey("_id", Optional.empty())))); // hashed index
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(ADDRESS), mockSession);
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(SPLIT_STRATEGY, MongoSplitStrategy.RANGE.name()))
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
                .hasMessageContaining("does not have a range-scannable index for range partitioning");
    }

    @Test
    public void testClusterErrorTranslation()
    {
        TestingMongoSession mockSession = new TestingMongoSession();
        mockSession.throwTimeoutOnSample = true;
        mockSession.estimatedDocCount = 10_000L;
        mockSession.collectionSize = 400 * 1024 * 1024L;

        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(ADDRESS), mockSession);
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
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
                .matches(e -> ((TrinoException) e).getErrorCode().equals(MONGODB_CLUSTER_ERROR.toErrorCode()));
    }

    private static class TestingMongoSession
            extends MongoSession
    {
        boolean isView;
        long estimatedDocCount = 10_000L;
        long collectionSize = 100 * 1024 * 1024L;
        List<MongoIndex> indexes = List.of(new MongoIndex(List.of(new MongoIndex.MongodbIndexKey("_id", Optional.of(SortOrder.ASC_NULLS_LAST)))));
        List<BsonValue> sampleKeys = List.of();
        boolean sampled;
        boolean throwTimeoutOnSample;

        TestingMongoSession()
        {
            super(TESTING_TYPE_MANAGER, () -> null, new MongoClientConfig());
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
            return estimatedDocCount;
        }

        @Override
        public long getCollectionSize(RemoteTableName remoteTableName)
        {
            return collectionSize;
        }

        @Override
        public List<BsonValue> samplePartitionKeys(RemoteTableName remoteTableName, String partitionKey, int sampleSize)
        {
            sampled = true;
            if (throwTimeoutOnSample) {
                throw new MongoTimeoutException("Timed out connecting to server");
            }
            return new ArrayList<>(sampleKeys);
        }
    }
}
