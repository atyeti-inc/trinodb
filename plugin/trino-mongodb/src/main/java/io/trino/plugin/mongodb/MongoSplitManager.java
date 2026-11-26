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
import com.google.inject.Inject;
import com.mongodb.MongoException;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.FixedSplitSource;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.mongodb.BsonValueComparator.BSON_VALUE_COMPARATOR;
import static io.trino.plugin.mongodb.MongoErrorCode.MONGODB_CLUSTER_ERROR;
import static io.trino.plugin.mongodb.MongoSessionProperties.getDynamicFilteringWaitTimeout;
import static io.trino.plugin.mongodb.MongoSessionProperties.getMinRecordsPerSplit;
import static io.trino.plugin.mongodb.MongoSessionProperties.getSplitSize;
import static io.trino.plugin.mongodb.MongoSessionProperties.getSplitStrategy;
import static io.trino.plugin.mongodb.MongoSessionProperties.isParallelSplitEnabled;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class MongoSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(MongoSplitManager.class);

    private final MongoServerDetailsProvider serverDetailsProvider;
    private final MongoSession mongoSession;

    @Inject
    public MongoSplitManager(MongoServerDetailsProvider serverDetailsProvider, MongoSession mongoSession)
    {
        this.serverDetailsProvider = requireNonNull(serverDetailsProvider, "serverDetailsProvider is null");
        this.mongoSession = requireNonNull(mongoSession, "mongoSession is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint)
    {
        MongoTableHandle tableHandle = (MongoTableHandle) table;
        if (constraint.getSummary().isNone() || tableHandle.constraint().isNone()) {
            return new FixedSplitSource(ImmutableList.of());
        }

        long dynamicFilteringWaitTimeoutMillis = getDynamicFilteringWaitTimeout(session).toMillis();
        if (!isParallelSplitEnabled(session)) {
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        return getParallelSplits(transaction, session, table, dynamicFilterColumns, constraint, dynamicFilteringWaitTimeoutMillis);
    }

    protected ConnectorSplitSource getParallelSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint,
            long dynamicFilteringWaitTimeoutMillis)
    {
        MongoTableHandle tableHandle = (MongoTableHandle) table;
        RemoteTableName remoteTableName = tableHandle.remoteTableName();

        // Check if table is a PTF query
        if (tableHandle.filter().isPresent()) {
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        // Check if collection is a view
        try {
            if (mongoSession.isView(remoteTableName)) {
                return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
            }
        }
        catch (TrinoException e) {
            MongoSplitStrategy splitStrategy = getSplitStrategy(session);
            if (splitStrategy == MongoSplitStrategy.RANGE) {
                throw e;
            }
            log.debug(e, "Failed to check if collection is a view for %s; falling back to single split", tableHandle.schemaTableName());
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }
        catch (MongoException e) {
            MongoSplitStrategy splitStrategy = getSplitStrategy(session);
            if (splitStrategy == MongoSplitStrategy.RANGE) {
                throw new TrinoException(MONGODB_CLUSTER_ERROR, "Failed to check if collection is a view: " + e.getMessage(), e);
            }
            log.debug(e, "Failed to check if collection is a view for %s; falling back to single split", tableHandle.schemaTableName());
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        // Check split strategy: CHUNK strategy is non-applicable to non-sharded collections
        MongoSplitStrategy splitStrategy = getSplitStrategy(session);
        if (splitStrategy == MongoSplitStrategy.CHUNK) {
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        List<MongoIndex> indexes;
        try {
            indexes = mongoSession.getIndexes(remoteTableName.databaseName(), remoteTableName.collectionName());
        }
        catch (TrinoException e) {
            if (splitStrategy == MongoSplitStrategy.RANGE) {
                throw e;
            }
            log.debug(e, "Failed to retrieve indexes for %s; falling back to single split", tableHandle.schemaTableName());
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }
        catch (MongoException e) {
            if (splitStrategy == MongoSplitStrategy.RANGE) {
                throw new TrinoException(MONGODB_CLUSTER_ERROR, "Failed to retrieve indexes: " + e.getMessage(), e);
            }
            log.debug(e, "Failed to retrieve indexes for %s; falling back to single split", tableHandle.schemaTableName());
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        Optional<String> candidatePartitionKey = chooseCandidatePartitionKey(indexes);
        if (candidatePartitionKey.isEmpty()) {
            if (splitStrategy == MongoSplitStrategy.RANGE) {
                throw new TrinoException(NOT_SUPPORTED, "No suitable range partition key found for collection: " + tableHandle.schemaTableName());
            }
            log.debug("No suitable range partition key found for collection %s; falling back to single split", tableHandle.schemaTableName());
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        String candidateKey = candidatePartitionKey.get();

        long minRecordsPerSplit = getMinRecordsPerSplit(session);
        long splitSizeInBytes = getSplitSize(session).toBytes();

        long estimatedCount;
        long collectionSize;
        try {
            estimatedCount = mongoSession.getEstimatedDocumentCount(remoteTableName);
            collectionSize = mongoSession.getCollectionSize(remoteTableName);
        }
        catch (TrinoException e) {
            if (splitStrategy == MongoSplitStrategy.RANGE) {
                throw e;
            }
            log.debug(e, "Failed to retrieve collection metadata for %s; falling back to single split", tableHandle.schemaTableName());
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }
        catch (MongoException e) {
            if (splitStrategy == MongoSplitStrategy.RANGE) {
                throw new TrinoException(MONGODB_CLUSTER_ERROR, "Failed to retrieve collection metadata: " + e.getMessage(), e);
            }
            log.debug(e, "Failed to retrieve collection metadata for %s; falling back to single split", tableHandle.schemaTableName());
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        if (minRecordsPerSplit > 0 && estimatedCount < minRecordsPerSplit) {
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        int sizeSplits = collectionSize > 0 ? Math.max(1, (int) Math.ceil((double) collectionSize / splitSizeInBytes)) : 1;
        int targetSplitCount;
        if (minRecordsPerSplit > 0) {
            int maxRecordSplits = Math.max(1, (int) (estimatedCount / minRecordsPerSplit));
            targetSplitCount = Math.min(sizeSplits, maxRecordSplits);
        }
        else {
            targetSplitCount = sizeSplits;
        }

        if (targetSplitCount <= 1) {
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        int sampleSize = Math.min(Math.max(100, targetSplitCount * 20), 10_000);
        List<BsonValue> samples;
        try {
            samples = mongoSession.sampleCandidateKeys(tableHandle, candidateKey, sampleSize);
        }
        catch (TrinoException e) {
            if (splitStrategy == MongoSplitStrategy.RANGE) {
                throw e;
            }
            log.debug(e, "Failed to sample candidate keys for %s; falling back to single split", tableHandle.schemaTableName());
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }
        catch (MongoException e) {
            if (splitStrategy == MongoSplitStrategy.RANGE) {
                throw new TrinoException(MONGODB_CLUSTER_ERROR, "Failed to sample candidate keys: " + e.getMessage(), e);
            }
            log.debug(e, "Failed to sample candidate keys for %s; falling back to single split", tableHandle.schemaTableName());
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        if (samples.size() < 2) {
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        List<BsonValue> sortedSamples = new ArrayList<>(samples);
        sortedSamples.sort(BSON_VALUE_COMPARATOR);

        List<BsonValue> distinctBoundaries = new ArrayList<>();
        BsonValue lastBoundary = null;
        for (int i = 1; i < targetSplitCount; i++) {
            int index = (int) (((long) i * sortedSamples.size()) / targetSplitCount);
            BsonValue boundary = sortedSamples.get(index);
            if (lastBoundary == null || BSON_VALUE_COMPARATOR.compare(lastBoundary, boundary) != 0) {
                distinctBoundaries.add(boundary);
                lastBoundary = boundary;
            }
        }

        if (distinctBoundaries.isEmpty()) {
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        List<BsonDocument> boundaryDocs = distinctBoundaries.stream()
                .map(val -> new BsonDocument(candidateKey, val))
                .collect(toImmutableList());

        int boundaryCount = boundaryDocs.size();
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

        // Split 0: [-inf, k_0)
        splits.add(new MongoSplit(
                serverDetailsProvider.getServerAddress(),
                Optional.of(MongoPartitionBounds.fromBson(
                        ImmutableList.of(candidateKey),
                        Optional.empty(),
                        false,
                        Optional.of(boundaryDocs.get(0)),
                        false))));

        // Splits 1 to boundaryCount - 1: [k_{i-1}, k_i)
        for (int i = 1; i < boundaryCount; i++) {
            splits.add(new MongoSplit(
                    serverDetailsProvider.getServerAddress(),
                    Optional.of(MongoPartitionBounds.fromBson(
                            ImmutableList.of(candidateKey),
                            Optional.of(boundaryDocs.get(i - 1)),
                            true,
                            Optional.of(boundaryDocs.get(i)),
                            false))));
        }

        // Split boundaryCount: [k_{boundaryCount - 1}, +inf)
        splits.add(new MongoSplit(
                serverDetailsProvider.getServerAddress(),
                Optional.of(MongoPartitionBounds.fromBson(
                        ImmutableList.of(candidateKey),
                        Optional.of(boundaryDocs.get(boundaryCount - 1)),
                        true,
                        Optional.empty(),
                        false))));

        return new MongoSplitSource(dynamicFilteringWaitTimeoutMillis, new FixedSplitSource(splits.build()));
    }

    private static Optional<String> chooseCandidatePartitionKey(List<MongoIndex> indexes)
    {
        // Check for an index where the first key is _id
        for (MongoIndex index : indexes) {
            if (!index.keys().isEmpty() && index.keys().get(0).name().equals("_id")) {
                return Optional.of("_id");
            }
        }

        // Search for any single-column ascending or descending index on a top-level field
        for (MongoIndex index : indexes) {
            if (index.keys().size() == 1) {
                MongoIndex.MongodbIndexKey key = index.keys().get(0);
                if (key.sortOrder().isPresent() && !key.name().contains(".")) {
                    return Optional.of(key.name());
                }
            }
        }

        return Optional.empty();
    }

    private MongoSplitSource createSingleSplitSource(long dynamicFilteringWaitTimeoutMillis)
    {
        MongoSplit split = new MongoSplit(serverDetailsProvider.getServerAddress());
        return new MongoSplitSource(dynamicFilteringWaitTimeoutMillis, new FixedSplitSource(split));
    }

    private static class MongoSplitSource
            implements ConnectorSplitSource
    {
        private final long dynamicFilteringWaitTimeoutMillis;
        private final ConnectorSplitSource delegateSplitSource;

        public MongoSplitSource(long dynamicFilteringWaitTimeoutMillis, ConnectorSplitSource delegateSplitSource)
        {
            this.dynamicFilteringWaitTimeoutMillis = dynamicFilteringWaitTimeoutMillis;
            this.delegateSplitSource = requireNonNull(delegateSplitSource, "delegateSplitSource is null");
        }

        @Override
        public long getRequestedDynamicFilterWaitTimeoutMillis()
        {
            return dynamicFilteringWaitTimeoutMillis;
        }

        @Override
        public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
        {
            return delegateSplitSource.getNextBatch(maxSize, dynamicFilterSnapshot);
        }

        @Override
        public void close()
        {
            delegateSplitSource.close();
        }

        @Override
        public boolean isFinished()
        {
            return delegateSplitSource.isFinished();
        }
    }
}
