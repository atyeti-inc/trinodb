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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoMetadata
{
    private static final ConnectorSession SESSION = TestingConnectorSession.builder().build();

    @Test
    public void testApplyFilterRejectsWhenAggregationPresent()
    {
        MongoSession mongoSession = new MongoSession(TESTING_TYPE_MANAGER, () -> null, new MongoClientConfig());
        MongoMetadata metadata = new MongoMetadata(mongoSession);

        MongoColumnHandle custkey = new MongoColumnHandle("custkey", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoColumnHandle agg0 = new MongoColumnHandle("_agg_0", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAggregation aggregation = new MongoAggregation(
                ImmutableList.of(custkey),
                ImmutableList.of(new MongoAccumulator(agg0, "count", ImmutableList.of())));

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty())
                .withAggregation(aggregation);

        MongoColumnHandle filterColumn = new MongoColumnHandle("id", ImmutableList.of(), INTEGER, false, false, Optional.empty());
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(ImmutableMap.of(filterColumn, Domain.singleValue(INTEGER, 1L))));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(SESSION, tableHandle, constraint);
        assertThat(result).isEmpty();
    }

    @Test
    public void testApplyFilterPushesConstraintWhenAggregationAbsent()
    {
        MongoSession mongoSession = new MongoSession(TESTING_TYPE_MANAGER, () -> null, new MongoClientConfig());
        MongoMetadata metadata = new MongoMetadata(mongoSession);

        MongoTableHandle tableHandle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty());

        MongoColumnHandle filterColumn = new MongoColumnHandle("id", ImmutableList.of(), INTEGER, false, false, Optional.empty());
        TupleDomain<ColumnHandle> domain = TupleDomain.withColumnDomains(ImmutableMap.of(filterColumn, Domain.singleValue(INTEGER, 1L)));
        Constraint constraint = new Constraint(domain);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(SESSION, tableHandle, constraint);
        assertThat(result).isPresent();

        MongoTableHandle resultHandle = (MongoTableHandle) result.get().getHandle();
        assertThat(resultHandle.constraint()).isEqualTo(domain);
        assertThat(resultHandle.schemaTableName()).isEqualTo(tableHandle.schemaTableName());
        assertThat(resultHandle.remoteTableName()).isEqualTo(tableHandle.remoteTableName());
        assertThat(resultHandle.filter()).isEqualTo(tableHandle.filter());
        assertThat(resultHandle.projectedColumns()).isEqualTo(tableHandle.projectedColumns());
        assertThat(resultHandle.limit()).isEqualTo(tableHandle.limit());
        assertThat(resultHandle.aggregation()).isEmpty();
    }
}
