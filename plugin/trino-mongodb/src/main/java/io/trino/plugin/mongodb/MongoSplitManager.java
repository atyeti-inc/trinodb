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
import com.mongodb.MongoServerUnavailableException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import io.airlift.log.Logger;
import io.trino.spi.HostAddress;
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
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.mongodb.MongoErrorCode.MONGODB_CLUSTER_ERROR;
import static io.trino.plugin.mongodb.MongoSessionProperties.getDynamicFilteringWaitTimeout;
import static io.trino.plugin.mongodb.MongoSessionProperties.getMinRecordsPerSplit;
import static io.trino.plugin.mongodb.MongoSessionProperties.getSplitSize;
import static io.trino.plugin.mongodb.MongoSessionProperties.getSplitStrategy;
import static io.trino.plugin.mongodb.MongoSessionProperties.isParallelSplitEnabled;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MongoSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(MongoSplitManager.class);

    private final MongoServerDetailsProvider serverDetailsProvider;
    private final MongoSession mongoSession;

    public MongoSplitManager(MongoServerDetailsProvider serverDetailsProvider)
    {
        this(serverDetailsProvider, null);
    }

    @Inject
    public MongoSplitManager(MongoServerDetailsProvider serverDetailsProvider, MongoSession mongoSession)
    {
        this.serverDetailsProvider = requireNonNull(serverDetailsProvider, "serverDetailsProvider is null");
        this.mongoSession = mongoSession;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint)
    {
        long dynamicFilteringWaitTimeoutMillis = getDynamicFilteringWaitTimeout(session).toMillis();

        MongoTableHandle tableHandle = (MongoTableHandle) table;
        if (tableHandle.constraint().isNone() || constraint.getSummary().isNone()) {
            return createEmptySplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        if (tableHandle.filter().isPresent()) {
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        if (mongoSession == null || !isParallelSplitEnabled(session)) {
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
        MongoSplitStrategy splitStrategy = getSplitStrategy(session);

        if (splitStrategy == MongoSplitStrategy.CHUNK) {
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        RemoteTableName remoteTableName = tableHandle.remoteTableName();
        try {
            if (mongoSession.isView(remoteTableName)) {
                return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
            }

            List<MongoIndex> indexes = mongoSession.getIndexes(remoteTableName.databaseName(), remoteTableName.collectionName());
            Optional<String> candidateKeyOptional = MongoCandidatePartitionKeyResolver.resolveKey(indexes);
            if (candidateKeyOptional.isEmpty()) {
                if (splitStrategy == MongoSplitStrategy.RANGE) {
                    throw new TrinoException(
                            NOT_SUPPORTED,
                            format("Table '%s' does not have a range-scannable index for range partitioning", tableHandle.schemaTableName()));
                }
                return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
            }

            String candidateKey = candidateKeyOptional.get();
            long estimatedDocCount = mongoSession.getEstimatedDocumentCount(remoteTableName);
            if (estimatedDocCount == 0) {
                return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
            }

            long minRecordsPerSplit = getMinRecordsPerSplit(session);
            if (minRecordsPerSplit > 0 && estimatedDocCount < minRecordsPerSplit) {
                return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
            }

            long collectionSize = mongoSession.getCollectionSize(remoteTableName);
            long targetSplitSize = getSplitSize(session).toBytes();

            long splitCount;
            if (collectionSize > 0) {
                long sizeSplits = (collectionSize + targetSplitSize - 1) / targetSplitSize;
                long maxRecordSplits = minRecordsPerSplit > 0 ? estimatedDocCount / minRecordsPerSplit : Long.MAX_VALUE;
                splitCount = Math.min(sizeSplits, maxRecordSplits);
            }
            else {
                if (minRecordsPerSplit > 0) {
                    splitCount = estimatedDocCount / minRecordsPerSplit;
                }
                else {
                    splitCount = 1;
                }
            }

            if (splitCount <= 1) {
                return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
            }

            int sampleTarget = (int) Math.min(Math.max(splitCount * 10, 100), 10_000);
            int sampleSize = (int) Math.min(sampleTarget, estimatedDocCount);

            List<BsonValue> sampleKeys = mongoSession.samplePartitionKeys(remoteTableName, candidateKey, sampleSize);
            if (sampleKeys.size() < 2) {
                return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
            }

            sampleKeys.sort(BsonValueComparator.INSTANCE);

            List<BsonValue> boundaries = new ArrayList<>();
            BsonValue lastBoundary = null;
            for (int k = 1; k < splitCount; k++) {
                int index = (int) ((k * (long) sampleKeys.size()) / splitCount);
                BsonValue boundary = sampleKeys.get(index);
                if (lastBoundary == null || BsonValueComparator.INSTANCE.compare(lastBoundary, boundary) != 0) {
                    boundaries.add(boundary);
                    lastBoundary = boundary;
                }
            }

            if (boundaries.isEmpty()) {
                return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
            }

            List<HostAddress> addresses = serverDetailsProvider.getServerAddress();
            ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

            int k = boundaries.size();
            for (int i = 0; i <= k; i++) {
                BsonDocument lowerDoc = new BsonDocument();
                BsonDocument upperDoc = new BsonDocument();

                if (i == 0) {
                    lowerDoc.append(candidateKey, new BsonMinKey());
                    upperDoc.append(candidateKey, boundaries.get(0));
                    MongoPartitionBounds bounds = MongoPartitionBounds.fromBson(
                            List.of(candidateKey),
                            lowerDoc,
                            true,
                            upperDoc,
                            false);
                    splits.add(new MongoSplit(addresses, Optional.of(bounds)));
                }
                else if (i == k) {
                    lowerDoc.append(candidateKey, boundaries.get(k - 1));
                    upperDoc.append(candidateKey, new BsonMaxKey());
                    MongoPartitionBounds bounds = MongoPartitionBounds.fromBson(
                            List.of(candidateKey),
                            lowerDoc,
                            true,
                            upperDoc,
                            true);
                    splits.add(new MongoSplit(addresses, Optional.of(bounds)));
                }
                else {
                    lowerDoc.append(candidateKey, boundaries.get(i - 1));
                    upperDoc.append(candidateKey, boundaries.get(i));
                    MongoPartitionBounds bounds = MongoPartitionBounds.fromBson(
                            List.of(candidateKey),
                            lowerDoc,
                            true,
                            upperDoc,
                            false);
                    splits.add(new MongoSplit(addresses, Optional.of(bounds)));
                }
            }

            return new MongoSplitSource(dynamicFilteringWaitTimeoutMillis, new FixedSplitSource(splits.build()));
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (MongoTimeoutException | MongoSocketException | MongoServerUnavailableException e) {
            throw new TrinoException(MONGODB_CLUSTER_ERROR, "Failed to communicate with MongoDB during split generation", e);
        }
        catch (Exception e) {
            if (splitStrategy == MongoSplitStrategy.RANGE) {
                throw new TrinoException(MONGODB_CLUSTER_ERROR, "Failed to compute range splits for collection: " + remoteTableName, e);
            }
            log.warn(e, "Falling back to single split generation for table %s due to split generation failure", tableHandle.schemaTableName());
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }
    }

    private MongoSplitSource createEmptySplitSource(long dynamicFilteringWaitTimeoutMillis)
    {
        return new MongoSplitSource(dynamicFilteringWaitTimeoutMillis, new FixedSplitSource(ImmutableList.of()));
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
