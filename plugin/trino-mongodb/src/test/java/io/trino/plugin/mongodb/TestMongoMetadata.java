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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoMetadata
{
    private static final SchemaTableName SCHEMA_TABLE = new SchemaTableName("schema", "table");
    private static final RemoteTableName REMOTE_TABLE = new RemoteTableName("schema", "table");
    private static final MongoColumnHandle DEPT_COL = new MongoColumnHandle("dept_id", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
    private static final MongoColumnHandle TOTAL_COL = new MongoColumnHandle("total", ImmutableList.of(), BIGINT, false, false, Optional.empty());
    private static final MongoColumnHandle SALARY_COL = new MongoColumnHandle("salary", ImmutableList.of(), BIGINT, false, false, Optional.empty());

    private final MongoMetadata metadata;
    private final ConnectorSession session;

    public TestMongoMetadata()
    {
        MongoClientConfig config = new MongoClientConfig();
        MongoSession mongoSession = new MongoSession(
                TESTING_TYPE_MANAGER,
                () -> { throw new UnsupportedOperationException(); },
                config);
        this.metadata = new MongoMetadata(mongoSession);
        MongoSessionProperties sessionProperties = new MongoSessionProperties(config);
        this.session = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .build();
    }

    @Test
    public void testApplyFilterWithAggregation()
    {
        MongoAggregation aggregation = new MongoAggregation(
                ImmutableList.of(DEPT_COL),
                ImmutableList.of(new MongoAccumulator(TOTAL_COL, "sum", ImmutableList.of(SALARY_COL))));

        MongoTableHandle aggregatedHandle = new MongoTableHandle(
                SCHEMA_TABLE,
                REMOTE_TABLE,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(),
                OptionalInt.empty(),
                Optional.of(aggregation));

        assertThat(metadata.applyFilter(session, aggregatedHandle, new Constraint(TupleDomain.all())))
                .isEmpty();
    }

    @Test
    public void testApplyLimitWithAggregation()
    {
        MongoAggregation aggregation = new MongoAggregation(
                ImmutableList.of(DEPT_COL),
                ImmutableList.of(new MongoAccumulator(TOTAL_COL, "sum", ImmutableList.of(SALARY_COL))));

        MongoTableHandle aggregatedHandle = new MongoTableHandle(
                SCHEMA_TABLE,
                REMOTE_TABLE,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(),
                OptionalInt.empty(),
                Optional.of(aggregation));

        Optional<LimitApplicationResult<ConnectorTableHandle>> result = metadata.applyLimit(session, aggregatedHandle, 10);
        assertThat(result).isPresent();
        MongoTableHandle resultHandle = (MongoTableHandle) result.get().getHandle();
        assertThat(resultHandle.limit()).isEqualTo(OptionalInt.of(10));
        assertThat(resultHandle.aggregation()).isEqualTo(Optional.of(aggregation));
    }

    @Test
    public void testApplyProjectionWithAggregation()
    {
        MongoAggregation aggregation = new MongoAggregation(
                ImmutableList.of(DEPT_COL),
                ImmutableList.of(new MongoAccumulator(TOTAL_COL, "sum", ImmutableList.of(SALARY_COL))));

        MongoTableHandle aggregatedHandle = new MongoTableHandle(
                SCHEMA_TABLE,
                REMOTE_TABLE,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(),
                OptionalInt.empty(),
                Optional.of(aggregation));

        List<ConnectorExpression> projections = ImmutableList.of(new Variable("dept_id", VARCHAR));
        Map<String, ColumnHandle> assignments = ImmutableMap.of("dept_id", DEPT_COL);

        assertThat(metadata.applyProjection(session, aggregatedHandle, projections, assignments))
                .isEmpty();
    }
}
