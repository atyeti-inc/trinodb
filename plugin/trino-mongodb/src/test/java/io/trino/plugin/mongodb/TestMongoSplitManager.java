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
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.mongodb.MongoSessionProperties.DYNAMIC_FILTERING_WAIT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoSplitManager
{
    private static final HostAddress SERVER_ADDRESS = HostAddress.fromParts("localhost", 27017);
    private static final MongoTableHandle TABLE_HANDLE = new MongoTableHandle(
            new SchemaTableName("schema", "table"),
            new RemoteTableName("schema", "table"),
            Optional.empty());

    @Test
    public void testSingleSplitSource()
    {
        MongoSplitManager splitManager = new MongoSplitManager(() -> ImmutableList.of(SERVER_ADDRESS));
        MongoSessionProperties sessionProperties = new MongoSessionProperties(new MongoClientConfig());
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .build();

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                session,
                TABLE_HANDLE,
                ImmutableSet.of(),
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(100, DynamicFilterSnapshot.EMPTY).join();
        assertThat(splits).hasSize(1);
        assertThat(splitSource.isFinished()).isTrue();
        MongoSplit split = (MongoSplit) splits.get(0);
        assertThat(split.getAddresses()).containsExactly(SERVER_ADDRESS);
    }

    @Test
    public void testDynamicFilteringWaitTimeoutSessionPropertyInspection()
    {
        MongoSplitManager splitManager = new MongoSplitManager(() -> ImmutableList.of(SERVER_ADDRESS));
        MongoSessionProperties sessionProperties = new MongoSessionProperties(new MongoClientConfig());

        ConnectorSession defaultSession = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .build();
        ConnectorSplitSource defaultSplitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                defaultSession,
                TABLE_HANDLE,
                ImmutableSet.of(),
                Constraint.alwaysTrue());
        assertThat(defaultSplitSource.getRequestedDynamicFilterWaitTimeoutMillis()).isEqualTo(5_000L);

        ConnectorSession customSession = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .setPropertyValues(ImmutableMap.of(DYNAMIC_FILTERING_WAIT_TIMEOUT, "10s"))
                .build();
        ConnectorSplitSource customSplitSource = splitManager.getSplits(
                new MongoTransactionHandle(),
                customSession,
                TABLE_HANDLE,
                ImmutableSet.of(),
                Constraint.alwaysTrue());
        assertThat(customSplitSource.getRequestedDynamicFilterWaitTimeoutMillis()).isEqualTo(10_000L);
    }
}
