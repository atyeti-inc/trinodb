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
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

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
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMongoSplitManager
{
    private static final HostAddress DEFAULT_ADDRESS = HostAddress.fromParts("127.0.0.1", 27017);
    private static final MongoTableHandle DEFAULT_TABLE = new MongoTableHandle(
            new SchemaTableName("schema", "table"),
            new RemoteTableName("schema", "table"),
            Optional.empty());
    private static final MongoIndex ID_INDEX = new MongoIndex(List.of(
            new MongoIndex.MongodbIndexKey("_id", Optional.of(SortOrder.ASC_NULLS_LAST))));

    @Test
    public void testSingleSplitSource()
            throws Exception
    {
        TestingMongoSession session = new TestingMongoSession();
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(DEFAULT_ADDRESS), session);
        ConnectorSession connectorSession = createSession(ImmutableMap.of());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                connectorSession,
                DEFAULT_TABLE,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(1);
        MongoSplit split = (MongoSplit) splits.get(0);
        assertThat(split.getAddresses()).containsExactly(DEFAULT_ADDRESS);
        assertThat(split.getPartitionBounds()).isEmpty();
        assertThat(splitSource.isFinished()).isTrue();
    }

    @Test
    public void testFallbackWhenParallelSplitDisabled()
            throws Exception
    {
        AtomicBoolean parallelSplitsCalled = new AtomicBoolean();
        TestingMongoSession session = new TestingMongoSession();
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(DEFAULT_ADDRESS), session)
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
                        new MongoSplit(List.of(DEFAULT_ADDRESS)),
                        new MongoSplit(List.of(DEFAULT_ADDRESS))));
            }
        };

        // When parallel splitting is enabled (default), getParallelSplits is invoked returning multi-splits
        ConnectorSession enabledSession = createSession(ImmutableMap.of(PARALLEL_SPLIT_ENABLED, true));

        ConnectorSplitSource enabledSplitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                enabledSession,
                DEFAULT_TABLE,
                Set.of(),
                Constraint.alwaysTrue());

        assertThat(parallelSplitsCalled.get()).isTrue();
        List<ConnectorSplit> enabledSplits = enabledSplitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(enabledSplits).hasSize(2);

        // When parallel splitting is disabled, fallback routes directly to single split and bypasses getParallelSplits
        parallelSplitsCalled.set(false);
        ConnectorSession disabledSession = createSession(ImmutableMap.of(PARALLEL_SPLIT_ENABLED, false));

        ConnectorSplitSource disabledSplitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                disabledSession,
                DEFAULT_TABLE,
                Set.of(),
                Constraint.alwaysTrue());

        assertThat(parallelSplitsCalled.get()).isFalse();
        List<ConnectorSplit> disabledSplits = disabledSplitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(disabledSplits).hasSize(1);
        MongoSplit split = (MongoSplit) disabledSplits.get(0);
        assertThat(split.getAddresses()).containsExactly(DEFAULT_ADDRESS);
        assertThat(disabledSplitSource.isFinished()).isTrue();
    }

    @Test
    public void testDynamicFilteringWaitTimeoutSessionPropertyInspection()
    {
        TestingMongoSession session = new TestingMongoSession();
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(DEFAULT_ADDRESS), session);
        ConnectorSession connectorSession = createSession(ImmutableMap.of(DYNAMIC_FILTERING_WAIT_TIMEOUT, "10s"));

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                connectorSession,
                DEFAULT_TABLE,
                Set.of(),
                Constraint.alwaysTrue());

        assertThat(splitSource.getRequestedDynamicFilterWaitTimeoutMillis()).isEqualTo(10_000L);
    }

    @Test
    public void testZeroSplitsWhenConstraintIsNone()
            throws Exception
    {
        TestingMongoSession session = new TestingMongoSession()
                .setIndexes(List.of(ID_INDEX))
                .setEstimatedDocumentCount(100_000)
                .setCollectionSize(100_000_000)
                .setSamples(List.of(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3)));

        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(DEFAULT_ADDRESS), session);
        ConnectorSession connectorSession = createSession(ImmutableMap.of(PARALLEL_SPLIT_ENABLED, true));

        // Constraint summary is none
        ConnectorSplitSource splitSourceFromConstraint = splitManager.getSplits(
                new MongoTransactionHandle(),
                connectorSession,
                DEFAULT_TABLE,
                Set.of(),
                new Constraint(TupleDomain.none()));

        assertThat(splitSourceFromConstraint.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get()).isEmpty();
        assertThat(splitSourceFromConstraint.isFinished()).isTrue();

        // TableHandle constraint is none
        MongoTableHandle noneTableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty(),
                TupleDomain.none(),
                Set.of(),
                OptionalInt.empty());

        ConnectorSplitSource splitSourceFromTable = splitManager.getSplits(
                new MongoTransactionHandle(),
                connectorSession,
                noneTableHandle,
                Set.of(),
                Constraint.alwaysTrue());

        assertThat(splitSourceFromTable.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get()).isEmpty();
        assertThat(splitSourceFromTable.isFinished()).isTrue();
    }

    @Test
    public void testFallbackWhenView()
            throws Exception
    {
        TestingMongoSession session = new TestingMongoSession()
                .setView(true)
                .setIndexes(List.of(ID_INDEX))
                .setEstimatedDocumentCount(100_000)
                .setCollectionSize(100_000_000);

        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(DEFAULT_ADDRESS), session);
        ConnectorSession connectorSession = createSession(ImmutableMap.of(PARALLEL_SPLIT_ENABLED, true));

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                connectorSession,
                DEFAULT_TABLE,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(1);
        MongoSplit split = (MongoSplit) splits.get(0);
        assertThat(split.getPartitionBounds()).isEmpty();
    }

    @Test
    public void testFallbackWhenPtfQuery()
            throws Exception
    {
        TestingMongoSession session = new TestingMongoSession()
                .setIndexes(List.of(ID_INDEX))
                .setEstimatedDocumentCount(100_000)
                .setCollectionSize(100_000_000);

        MongoTableHandle ptfTable = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.of("{\"x\": 1}"));

        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(DEFAULT_ADDRESS), session);
        ConnectorSession connectorSession = createSession(ImmutableMap.of(PARALLEL_SPLIT_ENABLED, true));

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                connectorSession,
                ptfTable,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(1);
        MongoSplit split = (MongoSplit) splits.get(0);
        assertThat(split.getPartitionBounds()).isEmpty();
    }

    @Test
    public void testFallbackWhenChunkStrategyOnNonSharded()
            throws Exception
    {
        TestingMongoSession session = new TestingMongoSession()
                .setIndexes(List.of(ID_INDEX))
                .setEstimatedDocumentCount(100_000)
                .setCollectionSize(100_000_000);

        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(DEFAULT_ADDRESS), session);
        ConnectorSession connectorSession = createSession(ImmutableMap.of(
                PARALLEL_SPLIT_ENABLED, true,
                SPLIT_STRATEGY, MongoSplitStrategy.CHUNK.name()));

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                connectorSession,
                DEFAULT_TABLE,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(1);
        MongoSplit split = (MongoSplit) splits.get(0);
        assertThat(split.getPartitionBounds()).isEmpty();
    }

    @Test
    public void testFallbackWhenBelowMinRecordsPerSplit()
            throws Exception
    {
        TestingMongoSession session = new TestingMongoSession()
                .setIndexes(List.of(ID_INDEX))
                .setEstimatedDocumentCount(500)
                .setCollectionSize(100_000_000);

        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(DEFAULT_ADDRESS), session);
        ConnectorSession connectorSession = createSession(ImmutableMap.of(
                PARALLEL_SPLIT_ENABLED, true,
                MIN_RECORDS_PER_SPLIT, 1_000L));

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                connectorSession,
                DEFAULT_TABLE,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(1);
        MongoSplit split = (MongoSplit) splits.get(0);
        assertThat(split.getPartitionBounds()).isEmpty();
    }

    @Test
    public void testZeroMinRecordsPerSplitSafeDivision()
            throws Exception
    {
        TestingMongoSession session = new TestingMongoSession()
                .setIndexes(List.of(ID_INDEX))
                .setEstimatedDocumentCount(50_000)
                .setCollectionSize(64 * 1024 * 1024) // 64MB
                .setSamples(List.of(
                        new BsonInt32(10),
                        new BsonInt32(20),
                        new BsonInt32(30),
                        new BsonInt32(40)));

        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(DEFAULT_ADDRESS), session);
        ConnectorSession connectorSession = createSession(ImmutableMap.of(
                PARALLEL_SPLIT_ENABLED, true,
                MIN_RECORDS_PER_SPLIT, 0L,
                SPLIT_SIZE, "32MB"));

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                connectorSession,
                DEFAULT_TABLE,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits.size()).isGreaterThanOrEqualTo(2);
    }

    @Test
    public void testFallbackWhenSamplesLessThanTwo()
            throws Exception
    {
        TestingMongoSession session = new TestingMongoSession()
                .setIndexes(List.of(ID_INDEX))
                .setEstimatedDocumentCount(50_000)
                .setCollectionSize(100_000_000)
                .setSamples(List.of(new BsonInt32(1))); // only 1 sample

        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(DEFAULT_ADDRESS), session);
        ConnectorSession connectorSession = createSession(ImmutableMap.of(
                PARALLEL_SPLIT_ENABLED, true,
                SPLIT_SIZE, "1MB"));

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                connectorSession,
                DEFAULT_TABLE,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(1);
        MongoSplit split = (MongoSplit) splits.get(0);
        assertThat(split.getPartitionBounds()).isEmpty();
    }

    @Test
    public void testMultiSplitGenerationDisjointAndExhaustive()
            throws Exception
    {
        ImmutableList.Builder<BsonValue> samples = ImmutableList.builder();
        for (int i = 0; i < 100; i++) {
            samples.add(new BsonInt64(i * 10L));
        }

        TestingMongoSession session = new TestingMongoSession()
                .setIndexes(List.of(ID_INDEX))
                .setEstimatedDocumentCount(100_000)
                .setCollectionSize(100 * 1024 * 1024)
                .setSamples(samples.build());

        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(DEFAULT_ADDRESS), session);
        ConnectorSession connectorSession = createSession(ImmutableMap.of(
                PARALLEL_SPLIT_ENABLED, true,
                SPLIT_SIZE, "20MB", // 100MB / 20MB = 5 splits
                MIN_RECORDS_PER_SPLIT, 1_000L));

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                connectorSession,
                DEFAULT_TABLE,
                Set.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(20, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).hasSize(5);

        // Verify split boundaries form disjoint, collectively exhaustive range
        for (int i = 0; i < splits.size(); i++) {
            MongoSplit split = (MongoSplit) splits.get(i);
            assertThat(split.getPartitionBounds()).isPresent();
            MongoPartitionBounds bounds = split.getPartitionBounds().get();
            assertThat(bounds.keyNames()).containsExactly("_id");

            if (i == 0) {
                // Split 0: [-inf, k_1)
                assertThat(bounds.lowerBound()).isEmpty();
                assertThat(bounds.lowerInclusive()).isFalse();
                assertThat(bounds.upperBound()).isPresent();
                assertThat(bounds.upperInclusive()).isFalse();
            }
            else if (i == splits.size() - 1) {
                // Split P: [k_P, +inf)
                assertThat(bounds.lowerBound()).isPresent();
                assertThat(bounds.lowerInclusive()).isTrue();
                assertThat(bounds.upperBound()).isEmpty();
                assertThat(bounds.upperInclusive()).isFalse();

                MongoSplit previousSplit = (MongoSplit) splits.get(i - 1);
                assertThat(bounds.lowerBound()).isEqualTo(previousSplit.getPartitionBounds().get().upperBound());
            }
            else {
                // Intermediate split: [k_i, k_{i+1})
                assertThat(bounds.lowerBound()).isPresent();
                assertThat(bounds.lowerInclusive()).isTrue();
                assertThat(bounds.upperBound()).isPresent();
                assertThat(bounds.upperInclusive()).isFalse();

                MongoSplit previousSplit = (MongoSplit) splits.get(i - 1);
                assertThat(bounds.lowerBound()).isEqualTo(previousSplit.getPartitionBounds().get().upperBound());
            }
        }
    }

    @Test
    public void testCandidatePartitionKeyResolution()
            throws Exception
    {
        // 1. When _id is present, prioritize _id
        MongoIndex compoundIndexStartingWithId = new MongoIndex(List.of(
                new MongoIndex.MongodbIndexKey("_id", Optional.of(SortOrder.ASC_NULLS_LAST)),
                new MongoIndex.MongodbIndexKey("user_id", Optional.of(SortOrder.ASC_NULLS_LAST))));
        MongoIndex otherIndex = new MongoIndex(List.of(
                new MongoIndex.MongodbIndexKey("user_id", Optional.of(SortOrder.ASC_NULLS_LAST))));

        TestingMongoSession session = new TestingMongoSession()
                .setIndexes(List.of(otherIndex, compoundIndexStartingWithId))
                .setEstimatedDocumentCount(50_000)
                .setCollectionSize(100 * 1024 * 1024)
                .setSamples(List.of(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3)));

        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(DEFAULT_ADDRESS), session);
        ConnectorSession connectorSession = createSession(ImmutableMap.of(
                PARALLEL_SPLIT_ENABLED, true,
                SPLIT_SIZE, "32MB"));

        List<ConnectorSplit> splits = splitManager.getSplits(
                new MongoTransactionHandle(),
                connectorSession,
                DEFAULT_TABLE,
                Set.of(),
                Constraint.alwaysTrue()).getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();

        assertThat(splits.get(0)).isInstanceOf(MongoSplit.class);
        MongoSplit firstSplit = (MongoSplit) splits.get(0);
        assertThat(firstSplit.getPartitionBounds().orElseThrow().keyNames()).containsExactly("_id");

        // 2. When _id is missing, use top-level single-key index
        TestingMongoSession noIdSession = new TestingMongoSession()
                .setIndexes(List.of(otherIndex))
                .setEstimatedDocumentCount(50_000)
                .setCollectionSize(100 * 1024 * 1024)
                .setSamples(List.of(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3)));

        MongoSplitManager splitManagerNoId = new MongoSplitManager(() -> List.of(DEFAULT_ADDRESS), noIdSession);
        List<ConnectorSplit> splitsNoId = splitManagerNoId.getSplits(
                new MongoTransactionHandle(),
                connectorSession,
                DEFAULT_TABLE,
                Set.of(),
                Constraint.alwaysTrue()).getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();

        MongoSplit firstSplitNoId = (MongoSplit) splitsNoId.get(0);
        assertThat(firstSplitNoId.getPartitionBounds().orElseThrow().keyNames()).containsExactly("user_id");

        // 3. When no suitable index exists
        TestingMongoSession unindexableSession = new TestingMongoSession()
                .setIndexes(List.of(new MongoIndex(List.of(
                        new MongoIndex.MongodbIndexKey("nested.field", Optional.of(SortOrder.ASC_NULLS_LAST))))))
                .setEstimatedDocumentCount(50_000)
                .setCollectionSize(100 * 1024 * 1024);

        MongoSplitManager splitManagerUnindexable = new MongoSplitManager(() -> List.of(DEFAULT_ADDRESS), unindexableSession);

        // Under AUTOMATIC, falls back to 1 split
        ConnectorSession autoSession = createSession(ImmutableMap.of(
                PARALLEL_SPLIT_ENABLED, true,
                SPLIT_STRATEGY, MongoSplitStrategy.AUTOMATIC.name()));
        List<ConnectorSplit> autoSplits = splitManagerUnindexable.getSplits(
                new MongoTransactionHandle(),
                autoSession,
                DEFAULT_TABLE,
                Set.of(),
                Constraint.alwaysTrue()).getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();
        assertThat(autoSplits).hasSize(1);
        assertThat(((MongoSplit) autoSplits.get(0)).getPartitionBounds()).isEmpty();

        // Under RANGE, throws NOT_SUPPORTED
        ConnectorSession rangeSession = createSession(ImmutableMap.of(
                PARALLEL_SPLIT_ENABLED, true,
                SPLIT_STRATEGY, MongoSplitStrategy.RANGE.name()));
        assertThatThrownBy(() -> splitManagerUnindexable.getSplits(
                new MongoTransactionHandle(),
                rangeSession,
                DEFAULT_TABLE,
                Set.of(),
                Constraint.alwaysTrue()))
                .isInstanceOf(TrinoException.class)
                .hasMessageStartingWith("No suitable range partition key found for collection")
                .matches(e -> ((TrinoException) e).getErrorCode() == NOT_SUPPORTED.toErrorCode());
    }

    @Test
    public void testSamplingFailureFallbackBehavior()
            throws Exception
    {
        TestingMongoSession failureSession = new TestingMongoSession()
                .setIndexes(List.of(ID_INDEX))
                .setEstimatedDocumentCount(50_000)
                .setCollectionSize(100 * 1024 * 1024)
                .setSamplingException(new MongoTimeoutException("Cluster connection timed out"));

        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(DEFAULT_ADDRESS), failureSession);

        // Under AUTOMATIC, falls back to single unpartitioned split
        ConnectorSession autoSession = createSession(ImmutableMap.of(
                PARALLEL_SPLIT_ENABLED, true,
                SPLIT_STRATEGY, MongoSplitStrategy.AUTOMATIC.name(),
                SPLIT_SIZE, "32MB"));

        List<ConnectorSplit> autoSplits = splitManager.getSplits(
                new MongoTransactionHandle(),
                autoSession,
                DEFAULT_TABLE,
                Set.of(),
                Constraint.alwaysTrue()).getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();

        assertThat(autoSplits).hasSize(1);
        assertThat(((MongoSplit) autoSplits.get(0)).getPartitionBounds()).isEmpty();

        // Under RANGE, throws MONGODB_CLUSTER_ERROR
        ConnectorSession rangeSession = createSession(ImmutableMap.of(
                PARALLEL_SPLIT_ENABLED, true,
                SPLIT_STRATEGY, MongoSplitStrategy.RANGE.name(),
                SPLIT_SIZE, "32MB"));

        assertThatThrownBy(() -> splitManager.getSplits(
                new MongoTransactionHandle(),
                rangeSession,
                DEFAULT_TABLE,
                Set.of(),
                Constraint.alwaysTrue()))
                .isInstanceOf(TrinoException.class)
                .matches(e -> ((TrinoException) e).getErrorCode() == MONGODB_CLUSTER_ERROR.toErrorCode());
    }

    @Test
    public void testRangeSplitWithObjectId()
            throws Exception
    {
        ObjectId oid1 = new ObjectId("000000000000000000000001");
        ObjectId oid2 = new ObjectId("000000000000000000000002");
        ObjectId oid3 = new ObjectId("000000000000000000000003");

        TestingMongoSession session = new TestingMongoSession()
                .setIndexes(List.of(ID_INDEX))
                .setEstimatedDocumentCount(100_000)
                .setCollectionSize(100 * 1024 * 1024)
                .setSamples(List.of(new BsonObjectId(oid1), new BsonObjectId(oid2), new BsonObjectId(oid3)));

        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(DEFAULT_ADDRESS), session);
        ConnectorSession connectorSession = createSession(ImmutableMap.of(
                PARALLEL_SPLIT_ENABLED, true,
                SPLIT_SIZE, "32MB"));

        List<ConnectorSplit> splits = splitManager.getSplits(
                new MongoTransactionHandle(),
                connectorSession,
                DEFAULT_TABLE,
                Set.of(),
                Constraint.alwaysTrue()).getNextBatch(10, DynamicFilterSnapshot.EMPTY).get();

        assertThat(splits.size()).isGreaterThanOrEqualTo(2);
        for (ConnectorSplit split : splits) {
            MongoSplit mongoSplit = (MongoSplit) split;
            assertThat(mongoSplit.getPartitionBounds()).isPresent();
            MongoPartitionBounds bounds = mongoSplit.getPartitionBounds().get();
            bounds.parseLowerBoundDocument();
            bounds.parseUpperBoundDocument();
        }
    }

    private static ConnectorSession createSession(ImmutableMap<String, Object> properties)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(properties)
                .build();
    }

    private static class TestingMongoSession
            extends MongoSession
    {
        private boolean isView;
        private List<MongoIndex> indexes = List.of();
        private long estimatedDocumentCount;
        private long collectionSize;
        private List<BsonValue> samples = List.of();
        private RuntimeException samplingException;

        public TestingMongoSession()
        {
            super(TESTING_TYPE_MANAGER, () -> { throw new UnsupportedOperationException(); }, new MongoClientConfig());
        }

        public TestingMongoSession setView(boolean isView)
        {
            this.isView = isView;
            return this;
        }

        public TestingMongoSession setIndexes(List<MongoIndex> indexes)
        {
            this.indexes = indexes;
            return this;
        }

        public TestingMongoSession setEstimatedDocumentCount(long count)
        {
            this.estimatedDocumentCount = count;
            return this;
        }

        public TestingMongoSession setCollectionSize(long size)
        {
            this.collectionSize = size;
            return this;
        }

        public TestingMongoSession setSamples(List<BsonValue> samples)
        {
            this.samples = samples;
            return this;
        }

        public TestingMongoSession setSamplingException(RuntimeException exception)
        {
            this.samplingException = exception;
            return this;
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
        public List<BsonValue> sampleCandidateKeys(MongoTableHandle tableHandle, String candidateKey, int sampleSize)
        {
            if (samplingException != null) {
                throw samplingException;
            }
            return samples;
        }
    }
}
