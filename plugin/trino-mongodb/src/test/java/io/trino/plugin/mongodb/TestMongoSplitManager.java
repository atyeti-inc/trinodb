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
import io.trino.spi.HostAddress;
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
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.plugin.mongodb.MongoSessionProperties.DYNAMIC_FILTERING_WAIT_TIMEOUT;
import static io.trino.plugin.mongodb.MongoSessionProperties.PARALLEL_SPLIT_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

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
        AtomicBoolean parallelSplitsCalled = new AtomicBoolean();
        MongoSplitManager splitManager = new MongoSplitManager(() -> List.of(address))
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
                        new MongoSplit(List.of(address)),
                        new MongoSplit(List.of(address))));
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
}
