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
import static java.util.Objects.requireNonNull;

public class MongoSplitManager
        implements ConnectorSplitManager
{
    private final MongoServerDetailsProvider serverDetailsProvider;
    private final Optional<MongoSession> mongoSession;

    @Inject
    public MongoSplitManager(MongoServerDetailsProvider serverDetailsProvider, MongoSession mongoSession)
    {
        this.serverDetailsProvider = requireNonNull(serverDetailsProvider, "serverDetailsProvider is null");
        this.mongoSession = Optional.of(requireNonNull(mongoSession, "mongoSession is null"));
    }

    public MongoSplitManager(MongoServerDetailsProvider serverDetailsProvider)
    {
        this.serverDetailsProvider = requireNonNull(serverDetailsProvider, "serverDetailsProvider is null");
        this.mongoSession = Optional.empty();
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
            return new MongoSplitSource(dynamicFilteringWaitTimeoutMillis, new FixedSplitSource(ImmutableList.of()));
        }

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

        if (tableHandle.filter().isPresent()) {
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        if (mongoSession.isEmpty()) {
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        MongoSession sessionClient = mongoSession.get();
        if (sessionClient.isView(tableHandle.remoteTableName())) {
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        MongoSplitStrategy splitStrategy = getSplitStrategy(session);
        if (splitStrategy != MongoSplitStrategy.AUTOMATIC && splitStrategy != MongoSplitStrategy.RANGE) {
            return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
        }

        try {
            long splitSizeBytes = getSplitSize(session).toBytes();
            long minRecordsPerSplit = getMinRecordsPerSplit(session);

            List<MongoPartitionBounds> bounds = MongoPartitionBoundsResolver.resolvePartitionBounds(
                    sessionClient,
                    tableHandle.remoteTableName(),
                    splitSizeBytes,
                    minRecordsPerSplit);

            if (bounds.size() < 2) {
                return createSingleSplitSource(dynamicFilteringWaitTimeoutMillis);
            }

            List<HostAddress> addresses = serverDetailsProvider.getServerAddress();
            ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
            for (MongoPartitionBounds bound : bounds) {
                splits.add(new MongoSplit(addresses, Optional.of(bound)));
            }

            return new MongoSplitSource(dynamicFilteringWaitTimeoutMillis, new FixedSplitSource(splits.build()));
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            throw new TrinoException(MONGODB_CLUSTER_ERROR, "Failed to generate splits for " + tableHandle.schemaTableName() + ": " + e.getMessage(), e);
        }
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

        MongoSplitSource(long dynamicFilteringWaitTimeoutMillis, ConnectorSplitSource delegateSplitSource)
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
