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
import java.util.Map;
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
        this.mongoSession = mongoSession;
    }

    public MongoSplitManager(MongoServerDetailsProvider serverDetailsProvider)
    {
        this(serverDetailsProvider, null);
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
        long dynamicFilteringWaitTimeoutMillis = getDynamicFilteringWaitTimeout(session).toMillis();

        if (tableHandle.constraint().isNone() || constraint.getSummary().isNone()) {
            return createEmptySplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        if (tableHandle.filter().isPresent()) {
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        if (!isParallelSplitEnabled(session)) {
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        if (mongoSession == null) {
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        RemoteTableName remoteTableName = tableHandle.remoteTableName();
        if (mongoSession.isView(remoteTableName)) {
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        MongoSplitStrategy splitStrategy = getSplitStrategy(session);
        return switch (splitStrategy) {
            case CHUNK -> generateChunkSplits(tableHandle, dynamicFilteringWaitTimeoutMillis);
            case RANGE -> generateRangeSplits(session, tableHandle, dynamicFilteringWaitTimeoutMillis);
            case AUTOMATIC -> generateAutomaticSplits(session, tableHandle, dynamicFilteringWaitTimeoutMillis);
        };
    }

    private MongoSplitSource generateChunkSplits(MongoTableHandle tableHandle, long dynamicFilteringWaitTimeoutMillis)
    {
        RemoteTableName remoteTableName = tableHandle.remoteTableName();
        try {
            Optional<MongoShardedCollectionMetadata> metadataOptional = mongoSession.getShardedCollectionMetadata(remoteTableName);
            if (metadataOptional.isEmpty()) {
                throw new TrinoException(NOT_SUPPORTED, "Table '%s' is not sharded".formatted(tableHandle.schemaTableName()));
            }

            MongoShardedCollectionMetadata metadata = metadataOptional.get();
            if (metadata.hasHashedShardKey()) {
                throw new TrinoException(NOT_SUPPORTED, "Table '%s' has a hashed shard key which does not support chunk-based split generation".formatted(tableHandle.schemaTableName()));
            }

            List<MongoChunk> chunks = mongoSession.getChunks(remoteTableName, metadata);
            if (chunks.isEmpty()) {
                throw new TrinoException(MONGODB_CLUSTER_ERROR, "No chunks found for sharded table: " + tableHandle.schemaTableName());
            }

            Map<String, List<HostAddress>> shardAddresses;
            try {
                shardAddresses = mongoSession.getShardAddresses();
            }
            catch (Exception e) {
                log.warn(e, "Failed to retrieve shard addresses; falling back to coordinator server address");
                shardAddresses = ImmutableMap.of();
            }

            List<ConnectorSplit> splits = buildChunkSplits(metadata.shardKeyFields(), chunks, shardAddresses);
            return new MongoSplitSource(dynamicFilteringWaitTimeoutMillis, new FixedSplitSource(splits));
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (MongoTimeoutException | MongoSocketException | MongoServerUnavailableException e) {
            throw new TrinoException(MONGODB_CLUSTER_ERROR, "Failed to communicate with MongoDB during split generation", e);
        }
        catch (Exception e) {
            throw new TrinoException(MONGODB_CLUSTER_ERROR, "Failed to generate chunk splits for table: " + tableHandle.schemaTableName(), e);
        }
    }

    private MongoSplitSource generateAutomaticSplits(
            ConnectorSession session,
            MongoTableHandle tableHandle,
            long dynamicFilteringWaitTimeoutMillis)
    {
        RemoteTableName remoteTableName = tableHandle.remoteTableName();
        Optional<MongoShardedCollectionMetadata> metadataOptional = Optional.empty();
        try {
            metadataOptional = mongoSession.getShardedCollectionMetadata(remoteTableName);
        }
        catch (Exception e) {
            log.debug(e, "Failed to retrieve sharded collection metadata for %s in automatic split mode; falling back to range splitting", remoteTableName);
        }

        if (metadataOptional.isPresent() && !metadataOptional.get().hasHashedShardKey()) {
            try {
                MongoShardedCollectionMetadata metadata = metadataOptional.get();
                List<MongoChunk> chunks = mongoSession.getChunks(remoteTableName, metadata);
                if (chunks.size() > 1) {
                    long estimatedDocCount = mongoSession.getEstimatedDocumentCount(remoteTableName);
                    long minRecordsPerSplit = getMinRecordsPerSplit(session);
                    if (estimatedDocCount == 0 || (minRecordsPerSplit > 0 && estimatedDocCount < minRecordsPerSplit)) {
                        return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
                    }

                    Map<String, List<HostAddress>> shardAddresses;
                    try {
                        shardAddresses = mongoSession.getShardAddresses();
                    }
                    catch (Exception e) {
                        log.debug(e, "Failed to retrieve shard addresses in automatic split mode; falling back to coordinator server address");
                        shardAddresses = ImmutableMap.of();
                    }

                    List<ConnectorSplit> splits = buildChunkSplits(metadata.shardKeyFields(), chunks, shardAddresses);
                    return new MongoSplitSource(dynamicFilteringWaitTimeoutMillis, new FixedSplitSource(splits));
                }
            }
            catch (Exception e) {
                log.debug(e, "Failed to generate chunk splits for %s in automatic split mode; falling back to range splitting", remoteTableName);
            }
        }

        return tryGenerateRangeSplits(session, tableHandle, dynamicFilteringWaitTimeoutMillis);
    }

    private List<ConnectorSplit> buildChunkSplits(
            List<String> shardKeyFields,
            List<MongoChunk> chunks,
            Map<String, List<HostAddress>> shardAddresses)
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        List<HostAddress> fallbackAddresses = serverDetailsProvider.getServerAddress();

        for (MongoChunk chunk : chunks) {
            List<HostAddress> addresses = shardAddresses.getOrDefault(chunk.shardId(), fallbackAddresses);
            if (addresses.isEmpty()) {
                addresses = fallbackAddresses;
            }

            MongoPartitionBounds bounds = MongoPartitionBounds.fromBson(
                    shardKeyFields,
                    chunk.min(),
                    true,
                    chunk.max(),
                    false);

            splits.add(new MongoSplit(addresses, Optional.of(bounds)));
        }
        return splits.build();
    }

    private MongoSplitSource generateRangeSplits(ConnectorSession session, MongoTableHandle tableHandle, long dynamicFilteringWaitTimeoutMillis)
    {
        return generateRangeSplitsInternal(session, tableHandle, dynamicFilteringWaitTimeoutMillis, true);
    }

    private MongoSplitSource tryGenerateRangeSplits(ConnectorSession session, MongoTableHandle tableHandle, long dynamicFilteringWaitTimeoutMillis)
    {
        return generateRangeSplitsInternal(session, tableHandle, dynamicFilteringWaitTimeoutMillis, false);
    }

    private MongoSplitSource generateRangeSplitsInternal(
            ConnectorSession session,
            MongoTableHandle tableHandle,
            long dynamicFilteringWaitTimeoutMillis,
            boolean failFast)
    {
        RemoteTableName remoteTableName = tableHandle.remoteTableName();
        try {
            List<MongoIndex> indexes = mongoSession.getIndexes(remoteTableName.databaseName(), remoteTableName.collectionName());
            Optional<String> candidateKeyOptional = MongoCandidatePartitionKeyResolver.resolveKey(indexes);
            if (candidateKeyOptional.isEmpty()) {
                if (failFast) {
                    throw new TrinoException(NOT_SUPPORTED, "Table '%s' does not have a range-scannable index for range partitioning".formatted(tableHandle.schemaTableName()));
                }
                return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
            }
            String candidateKey = candidateKeyOptional.get();

            long estimatedDocCount = mongoSession.getEstimatedDocumentCount(remoteTableName);
            long minRecordsPerSplit = getMinRecordsPerSplit(session);
            if (estimatedDocCount == 0 || (minRecordsPerSplit > 0 && estimatedDocCount < minRecordsPerSplit)) {
                return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
            }

            long targetSplitSize = getSplitSize(session).toBytes();
            long collectionSize = mongoSession.getCollectionSize(remoteTableName);
            long splitCount;
            if (collectionSize > 0) {
                long sizeSplits = (collectionSize + targetSplitSize - 1) / targetSplitSize;
                long maxRecordSplits = minRecordsPerSplit > 0 ? (estimatedDocCount / minRecordsPerSplit) : Long.MAX_VALUE;
                splitCount = Math.min(sizeSplits, maxRecordSplits);
            }
            else {
                splitCount = minRecordsPerSplit > 0 ? (estimatedDocCount / minRecordsPerSplit) : 1;
            }

            if (splitCount <= 1) {
                return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
            }

            // Sample size S = min(max(splitCount * 10, 100), 10_000) <= estimatedDocCount
            int targetSampleSize = (int) Math.min(Math.max(splitCount * 10, 100), 10_000);
            int sampleSize = (int) Math.min((long) targetSampleSize, estimatedDocCount);

            List<BsonValue> sampleKeys = mongoSession.samplePartitionKeys(remoteTableName, candidateKey, sampleSize);
            if (sampleKeys.size() < 2) {
                return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
            }

            List<BsonValue> sortedKeys = new ArrayList<>(sampleKeys);
            sortedKeys.sort(BsonValueComparator.INSTANCE);

            List<BsonValue> boundaries = new ArrayList<>();
            for (int k = 1; k < splitCount; k++) {
                int index = (int) ((k * (long) sortedKeys.size()) / splitCount);
                BsonValue boundary = sortedKeys.get(index);
                if (boundaries.isEmpty() || BsonValueComparator.INSTANCE.compare(boundaries.get(boundaries.size() - 1), boundary) < 0) {
                    boundaries.add(boundary);
                }
            }

            if (boundaries.isEmpty()) {
                return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
            }

            List<ConnectorSplit> splits = new ArrayList<>();
            List<HostAddress> addresses = serverDetailsProvider.getServerAddress();
            List<String> keyNames = List.of(candidateKey);

            // Split 0: [MinKey, b_0)
            MongoPartitionBounds firstBounds = MongoPartitionBounds.fromBson(
                    keyNames,
                    new BsonDocument(candidateKey, new BsonMinKey()),
                    true,
                    new BsonDocument(candidateKey, boundaries.get(0)),
                    false);
            splits.add(new MongoSplit(addresses, Optional.of(firstBounds)));

            // Intermediate splits: [b_{i-1}, b_i)
            for (int i = 1; i < boundaries.size(); i++) {
                MongoPartitionBounds bounds = MongoPartitionBounds.fromBson(
                        keyNames,
                        new BsonDocument(candidateKey, boundaries.get(i - 1)),
                        true,
                        new BsonDocument(candidateKey, boundaries.get(i)),
                        false);
                splits.add(new MongoSplit(addresses, Optional.of(bounds)));
            }

            // Split last: [b_{last}, MaxKey]
            MongoPartitionBounds lastBounds = MongoPartitionBounds.fromBson(
                    keyNames,
                    new BsonDocument(candidateKey, boundaries.get(boundaries.size() - 1)),
                    true,
                    new BsonDocument(candidateKey, new BsonMaxKey()),
                    true);
            splits.add(new MongoSplit(addresses, Optional.of(lastBounds)));

            return new MongoSplitSource(dynamicFilteringWaitTimeoutMillis, new FixedSplitSource(splits));
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (MongoTimeoutException | MongoSocketException | MongoServerUnavailableException e) {
            throw new TrinoException(MONGODB_CLUSTER_ERROR, "Failed to communicate with MongoDB during split generation", e);
        }
        catch (Exception e) {
            if (failFast) {
                throw new TrinoException(MONGODB_CLUSTER_ERROR, "Failed to generate range splits for table: " + tableHandle.schemaTableName(), e);
            }
            log.warn(e, "Failed to generate range splits for table %s; falling back to single split", tableHandle.schemaTableName());
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
