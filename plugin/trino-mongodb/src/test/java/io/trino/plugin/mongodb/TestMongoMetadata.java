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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalInt;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoMetadata
{
    private final MongoMetadata metadata = new MongoMetadata(
            new MongoSession(TESTING_TYPE_MANAGER, () -> null, new MongoClientConfig()));

    @Test
    public void testApplyLimitPreservesAggregation()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");

        MongoColumnHandle groupCol = new MongoColumnHandle("col_group", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle countResult = new MongoColumnHandle("_count", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator countAll = new MongoAccumulator(countResult, "count", ImmutableList.of(), false);
        MongoAggregation aggregation = new MongoAggregation(ImmutableList.of(groupCol), ImmutableList.of(countAll));

        MongoTableHandle handleWithAggregation = new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(),
                OptionalInt.empty(),
                Optional.of(aggregation));

        Optional<LimitApplicationResult<ConnectorTableHandle>> result = metadata.applyLimit(SESSION, handleWithAggregation, 10);
        assertThat(result).isPresent();
        MongoTableHandle limitedHandle = (MongoTableHandle) result.get().getHandle();
        assertThat(limitedHandle.limit()).isEqualTo(OptionalInt.of(10));
        assertThat(limitedHandle.aggregation()).isEqualTo(Optional.of(aggregation));

        // When aggregation is absent, applyLimit preserves Optional.empty()
        MongoTableHandle handleWithoutAggregation = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty());
        Optional<LimitApplicationResult<ConnectorTableHandle>> resultNoAgg = metadata.applyLimit(SESSION, handleWithoutAggregation, 10);
        assertThat(resultNoAgg).isPresent();
        MongoTableHandle limitedHandleNoAgg = (MongoTableHandle) resultNoAgg.get().getHandle();
        assertThat(limitedHandleNoAgg.limit()).isEqualTo(OptionalInt.of(10));
        assertThat(limitedHandleNoAgg.aggregation()).isEmpty();
    }

    @Test
    public void testApplyFilterPreservesAggregation()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");

        MongoColumnHandle groupCol = new MongoColumnHandle("col_group", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle countResult = new MongoColumnHandle("_count", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator countAll = new MongoAccumulator(countResult, "count", ImmutableList.of(), false);
        MongoAggregation aggregation = new MongoAggregation(ImmutableList.of(groupCol), ImmutableList.of(countAll));

        MongoTableHandle handleWithAggregation = new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(),
                OptionalInt.empty(),
                Optional.of(aggregation));

        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(groupCol, Domain.singleValue(VARCHAR, utf8Slice("test-group"))));
        Constraint constraint = new Constraint(tupleDomain);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(SESSION, handleWithAggregation, constraint);
        assertThat(result).isPresent();
        MongoTableHandle filteredHandle = (MongoTableHandle) result.get().getHandle();
        assertThat(filteredHandle.constraint()).isEqualTo(tupleDomain);
        assertThat(filteredHandle.aggregation()).isEqualTo(Optional.of(aggregation));

        // When aggregation is absent, applyFilter preserves Optional.empty()
        MongoTableHandle handleWithoutAggregation = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty());
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> resultNoAgg = metadata.applyFilter(SESSION, handleWithoutAggregation, constraint);
        assertThat(resultNoAgg).isPresent();
        MongoTableHandle filteredHandleNoAgg = (MongoTableHandle) resultNoAgg.get().getHandle();
        assertThat(filteredHandleNoAgg.constraint()).isEqualTo(tupleDomain);
        assertThat(filteredHandleNoAgg.aggregation()).isEmpty();
    }
}
