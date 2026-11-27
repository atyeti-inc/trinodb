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
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.mongodb.BsonValueComparator.BSON_VALUE_COMPARATOR;
import static java.util.Objects.requireNonNull;

public final class MongoPartitionBoundsResolver
{
    private static final Logger log = Logger.get(MongoPartitionBoundsResolver.class);
    private static final long ESTIMATED_AVERAGE_DOC_SIZE_BYTES = 1024L;

    private MongoPartitionBoundsResolver() {}

    public static Optional<String> choosePartitionKey(List<MongoIndex> indexes)
    {
        requireNonNull(indexes, "indexes is null");

        // Prioritize range index on _id
        for (MongoIndex index : indexes) {
            if (index.keys().size() == 1) {
                MongoIndex.MongodbIndexKey key = index.keys().get(0);
                if (key.name().equals("_id") && key.sortOrder().isPresent()) {
                    return Optional.of("_id");
                }
            }
        }

        // Fall back to single-field range indexes where sortOrder is present
        for (MongoIndex index : indexes) {
            if (index.keys().size() == 1) {
                MongoIndex.MongodbIndexKey key = index.keys().get(0);
                if (!key.name().equals("_id") && key.sortOrder().isPresent()) {
                    return Optional.of(key.name());
                }
            }
        }

        return Optional.empty();
    }

    public static int calculateSplitCount(long docCount, long collectionSizeBytes, long splitSizeBytes, long minRecordsPerSplit)
    {
        if (docCount <= 0) {
            return 1;
        }
        if (minRecordsPerSplit > 0 && docCount < minRecordsPerSplit) {
            return 1;
        }

        long targetSplits;
        if (collectionSizeBytes > 0 && splitSizeBytes > 0) {
            long sizeSplits = (long) Math.ceil((double) collectionSizeBytes / splitSizeBytes);
            if (minRecordsPerSplit > 0) {
                long maxRecordSplits = (long) Math.ceil((double) docCount / minRecordsPerSplit);
                targetSplits = Math.min(sizeSplits, maxRecordSplits);
            }
            else {
                targetSplits = sizeSplits;
            }
        }
        else if (minRecordsPerSplit > 0) {
            targetSplits = (long) Math.ceil((double) docCount / minRecordsPerSplit);
        }
        else if (splitSizeBytes > 0) {
            long estimatedSizeBytes = docCount * ESTIMATED_AVERAGE_DOC_SIZE_BYTES;
            targetSplits = (long) Math.ceil((double) estimatedSizeBytes / splitSizeBytes);
        }
        else {
            return 1;
        }

        if (targetSplits <= 1) {
            return 1;
        }

        return (int) Math.min(targetSplits, Integer.MAX_VALUE);
    }

    public static List<MongoPartitionBounds> buildPartitionBounds(String partitionKey, List<BsonValue> sampledKeys, int targetSplits)
    {
        requireNonNull(partitionKey, "partitionKey is null");
        requireNonNull(sampledKeys, "sampledKeys is null");

        if (targetSplits <= 1) {
            return ImmutableList.of();
        }

        List<BsonValue> sortedKeys = sampledKeys.stream()
                .filter(Objects::nonNull)
                .sorted(BSON_VALUE_COMPARATOR)
                .collect(toImmutableList());

        ImmutableList.Builder<BsonValue> distinctBuilder = ImmutableList.builder();
        BsonValue last = null;
        for (BsonValue key : sortedKeys) {
            if (last == null || BSON_VALUE_COMPARATOR.compare(last, key) != 0) {
                distinctBuilder.add(key);
                last = key;
            }
        }
        List<BsonValue> distinctKeys = distinctBuilder.build();

        if (distinctKeys.size() < 2) {
            return ImmutableList.of();
        }

        int k = Math.min(targetSplits, distinctKeys.size());
        if (k <= 1) {
            return ImmutableList.of();
        }

        List<BsonValue> cutPoints = new ArrayList<>(k - 1);
        for (int i = 1; i < k; i++) {
            int index = (int) ((long) i * distinctKeys.size() / k);
            cutPoints.add(distinctKeys.get(index));
        }

        ImmutableList.Builder<MongoPartitionBounds> boundsBuilder = ImmutableList.builder();
        List<String> keyNames = List.of(partitionKey);

        // Split 0: (-inf, b1)
        boundsBuilder.add(MongoPartitionBounds.fromBson(
                keyNames,
                Optional.empty(),
                true,
                Optional.of(new BsonDocument(partitionKey, cutPoints.get(0))),
                false));

        // Splits 1 to K-2: [bi, bi+1)
        for (int i = 1; i < k - 1; i++) {
            boundsBuilder.add(MongoPartitionBounds.fromBson(
                    keyNames,
                    Optional.of(new BsonDocument(partitionKey, cutPoints.get(i - 1))),
                    true,
                    Optional.of(new BsonDocument(partitionKey, cutPoints.get(i))),
                    false));
        }

        // Split K-1: [bK-1, +inf)
        boundsBuilder.add(MongoPartitionBounds.fromBson(
                keyNames,
                Optional.of(new BsonDocument(partitionKey, cutPoints.get(k - 2))),
                true,
                Optional.empty(),
                true));

        return boundsBuilder.build();
    }

    public static List<MongoPartitionBounds> resolvePartitionBounds(
            MongoSession mongoSession,
            RemoteTableName remoteTableName,
            long splitSizeBytes,
            long minRecordsPerSplit)
    {
        requireNonNull(mongoSession, "mongoSession is null");
        requireNonNull(remoteTableName, "remoteTableName is null");

        List<MongoIndex> indexes;
        try {
            indexes = mongoSession.getIndexes(remoteTableName);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            log.warn(e, "Failed to retrieve indexes for %s, falling back to single split", remoteTableName);
            return ImmutableList.of();
        }

        Optional<String> partitionKey = choosePartitionKey(indexes);
        if (partitionKey.isEmpty()) {
            return ImmutableList.of();
        }

        long docCount;
        try {
            docCount = mongoSession.getEstimatedDocumentCount(remoteTableName);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            log.warn(e, "Failed to get estimated document count for %s, falling back to single split", remoteTableName);
            return ImmutableList.of();
        }

        if (docCount <= 0 || (minRecordsPerSplit > 0 && docCount < minRecordsPerSplit)) {
            return ImmutableList.of();
        }

        long collectionSizeBytes = mongoSession.getCollectionSizeBytes(remoteTableName);
        int targetSplits = calculateSplitCount(docCount, collectionSizeBytes, splitSizeBytes, minRecordsPerSplit);
        if (targetSplits <= 1) {
            return ImmutableList.of();
        }

        int sampleSize = (int) Math.min(docCount, Math.min(10_000, Math.max(100, (long) targetSplits * 20)));
        List<BsonValue> sampledKeys;
        try {
            sampledKeys = mongoSession.samplePartitionKeys(remoteTableName, partitionKey.get(), sampleSize);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            log.warn(e, "Failed to sample partition keys for %s, falling back to single split", remoteTableName);
            return ImmutableList.of();
        }

        return buildPartitionBounds(partitionKey.get(), sampledKeys, targetSplits);
    }
}
