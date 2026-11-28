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
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Optional;
import java.util.OptionalInt;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestMongoMetadata
{
    private MongoSession mongoSession;
    private MongoMetadata metadata;

    @BeforeAll
    public void setUp()
    {
        mongoSession = new MongoSession(TESTING_TYPE_MANAGER, () -> {
            throw new UnsupportedOperationException();
        }, new MongoClientConfig());
        metadata = new MongoMetadata(mongoSession);
    }

    @Test
    public void testApplyFilterRejectedWhenAggregationPresent()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoColumnHandle groupCol = new MongoColumnHandle("region", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle countCol = new MongoColumnHandle("cnt", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator accumulator = new MongoAccumulator(countCol, "count", ImmutableList.of(), false);
        MongoAggregation aggregation = MongoAggregation.singleGroupingSet(ImmutableList.of(groupCol), ImmutableList.of(accumulator));

        MongoTableHandle aggregatedHandle = new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(groupCol, countCol),
                OptionalInt.empty(),
                Optional.of(aggregation));

        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(
                ImmutableMap.of(groupCol, Domain.singleValue(VARCHAR, utf8Slice("APAC")))));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(TestingConnectorSession.SESSION, aggregatedHandle, constraint);

        assertThat(result).isEmpty();
    }

    @Test
    public void testApplyFilterAllowedWithoutAggregation()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoColumnHandle column = new MongoColumnHandle("region", ImmutableList.of(), VARCHAR, false, false, Optional.empty());

        MongoTableHandle handle = new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(column),
                OptionalInt.empty(),
                Optional.empty());

        Domain domain = Domain.singleValue(VARCHAR, utf8Slice("APAC"));
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(TestingConnectorSession.SESSION, handle, constraint);

        assertThat(result).isPresent();
        MongoTableHandle filteredHandle = (MongoTableHandle) result.get().getHandle();
        assertThat(filteredHandle.constraint()).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of((ColumnHandle) column, domain)));
        assertThat(filteredHandle.aggregation()).isEmpty();
    }

    @Test
    public void testApplyLimitPreservesAggregation()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoColumnHandle groupCol = new MongoColumnHandle("region", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle countCol = new MongoColumnHandle("cnt", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator accumulator = new MongoAccumulator(countCol, "count", ImmutableList.of(), false);
        MongoAggregation aggregation = MongoAggregation.singleGroupingSet(ImmutableList.of(groupCol), ImmutableList.of(accumulator));

        MongoTableHandle aggregatedHandle = new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(groupCol, countCol),
                OptionalInt.empty(),
                Optional.of(aggregation));

        Optional<LimitApplicationResult<ConnectorTableHandle>> result =
                metadata.applyLimit(TestingConnectorSession.SESSION, aggregatedHandle, 25);

        assertThat(result).isPresent();
        MongoTableHandle limitedHandle = (MongoTableHandle) result.get().getHandle();
        assertThat(limitedHandle.limit()).hasValue(25);
        assertThat(limitedHandle.aggregation()).isEqualTo(Optional.of(aggregation));
    }
}
