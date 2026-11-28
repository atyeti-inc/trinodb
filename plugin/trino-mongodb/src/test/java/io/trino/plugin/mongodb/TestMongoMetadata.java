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
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;
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

    @Test
    public void testApplyAggregationDisabledBySession()
    {
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of("aggregation_pushdown_enabled", false))
                .build();

        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoTableHandle handle = new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(),
                OptionalInt.empty(),
                Optional.empty());

        AggregateFunction countStar = new AggregateFunction("count", BIGINT, ImmutableList.of(), ImmutableList.of(), false, Optional.empty());
        Optional<AggregationApplicationResult<ConnectorTableHandle>> result = metadata.applyAggregation(
                session,
                handle,
                ImmutableList.of(countStar),
                ImmutableMap.of(),
                ImmutableList.of(ImmutableList.of()));

        assertThat(result).isEmpty();
    }

    @Test
    public void testApplyAggregationEligibility()
    {
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of("aggregation_pushdown_enabled", true))
                .build();

        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoColumnHandle groupCol = new MongoColumnHandle("region", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle countCol = new MongoColumnHandle("cnt", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator accumulator = new MongoAccumulator(countCol, "count", ImmutableList.of(), false);
        MongoAggregation aggregation = MongoAggregation.singleGroupingSet(ImmutableList.of(groupCol), ImmutableList.of(accumulator));

        AggregateFunction countStar = new AggregateFunction("count", BIGINT, ImmutableList.of(), ImmutableList.of(), false, Optional.empty());

        // Reject if aggregation is already present (idempotency)
        MongoTableHandle aggregatedHandle = new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(groupCol, countCol),
                OptionalInt.empty(),
                Optional.of(aggregation));
        assertThat(metadata.applyAggregation(session, aggregatedHandle, ImmutableList.of(countStar), ImmutableMap.of(), ImmutableList.of(ImmutableList.of(groupCol))))
                .isEmpty();

        // Reject if limit is present
        MongoTableHandle limitedHandle = new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(groupCol),
                OptionalInt.of(10),
                Optional.empty());
        assertThat(metadata.applyAggregation(session, limitedHandle, ImmutableList.of(countStar), ImmutableMap.of(), ImmutableList.of(ImmutableList.of(groupCol))))
                .isEmpty();

        // Reject if filter is present (passthrough)
        MongoTableHandle filterHandle = new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                Optional.of("query"),
                TupleDomain.all(),
                ImmutableSet.of(groupCol),
                OptionalInt.empty(),
                Optional.empty());
        assertThat(metadata.applyAggregation(session, filterHandle, ImmutableList.of(countStar), ImmutableMap.of(), ImmutableList.of(ImmutableList.of(groupCol))))
                .isEmpty();

        // Reject multi-grouping sets
        MongoTableHandle baseHandle = new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(),
                OptionalInt.empty(),
                Optional.empty());
        assertThat(metadata.applyAggregation(session, baseHandle, ImmutableList.of(countStar), ImmutableMap.of(), ImmutableList.of(ImmutableList.of(groupCol), ImmutableList.of())))
                .isEmpty();

        // Reject empty aggregation without grouping keys
        assertThat(metadata.applyAggregation(session, baseHandle, ImmutableList.of(), ImmutableMap.of(), ImmutableList.of(ImmutableList.of())))
                .isEmpty();

        // Reject hidden grouping column
        MongoColumnHandle hiddenCol = new MongoColumnHandle("hidden", ImmutableList.of(), VARCHAR, true, false, Optional.empty());
        assertThat(metadata.applyAggregation(session, baseHandle, ImmutableList.of(countStar), ImmutableMap.of(), ImmutableList.of(ImmutableList.of(hiddenCol))))
                .isEmpty();
    }

    @Test
    public void testApplyAggregationSuccess()
    {
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new MongoSessionProperties(new MongoClientConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of("aggregation_pushdown_enabled", true))
                .build();

        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoTableHandle baseHandle = new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(),
                OptionalInt.empty(),
                Optional.empty());

        MongoColumnHandle regionCol = new MongoColumnHandle("region", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle amountCol = new MongoColumnHandle("amount", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        Map<String, ColumnHandle> assignments = ImmutableMap.of("amount_var", amountCol);

        AggregateFunction countStar = new AggregateFunction("count", BIGINT, ImmutableList.of(), ImmutableList.of(), false, Optional.empty());
        AggregateFunction sumAmount = new AggregateFunction(
                "sum",
                BIGINT,
                ImmutableList.of(new Variable("amount_var", BIGINT)),
                ImmutableList.of(),
                false,
                Optional.empty());

        Optional<AggregationApplicationResult<ConnectorTableHandle>> result = metadata.applyAggregation(
                session,
                baseHandle,
                ImmutableList.of(countStar, sumAmount),
                assignments,
                ImmutableList.of(ImmutableList.of(regionCol)));

        assertThat(result).isPresent();
        MongoTableHandle newHandle = (MongoTableHandle) result.get().getHandle();
        assertThat(newHandle.aggregation()).isPresent();

        MongoAggregation aggregation = newHandle.aggregation().get();
        assertThat(aggregation.groupingSets()).isEqualTo(ImmutableList.of(ImmutableList.of(regionCol)));
        assertThat(aggregation.accumulators()).hasSize(2);
        assertThat(aggregation.accumulators().get(0).functionName()).isEqualTo("count");
        assertThat(aggregation.accumulators().get(1).functionName()).isEqualTo("sum");
        assertThat(aggregation.accumulators().get(1).arguments()).containsExactly(amountCol);

        // Group by column + 2 synthetic columns in projectedColumns
        assertThat(newHandle.projectedColumns()).hasSize(3);
        assertThat(result.get().getProjections()).hasSize(2);
        assertThat(result.get().getAssignments()).hasSize(2);
    }

    @Test
    public void testAggregatedHandleColumnHandlesAndTableMetadata()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoColumnHandle groupCol = new MongoColumnHandle("region", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle countCol = new MongoColumnHandle("_mfgnrtd_0", ImmutableList.of(), BIGINT, false, false, Optional.of("synthetic"));
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

        // getColumnHandles should return columns from projectedColumns without calling mongoSession.getTable()
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(TestingConnectorSession.SESSION, aggregatedHandle);
        assertThat(columnHandles).containsOnlyKeys("region", "_mfgnrtd_0");
        assertThat(columnHandles.get("region")).isEqualTo(groupCol);
        assertThat(columnHandles.get("_mfgnrtd_0")).isEqualTo(countCol);

        // getTableMetadata should return metadata from projectedColumns without calling mongoSession.getTable()
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(TestingConnectorSession.SESSION, aggregatedHandle);
        assertThat(tableMetadata.getTable()).isEqualTo(schemaTableName);
        assertThat(tableMetadata.getColumns()).containsExactly(groupCol.toColumnMetadata(), countCol.toColumnMetadata());
    }
}
