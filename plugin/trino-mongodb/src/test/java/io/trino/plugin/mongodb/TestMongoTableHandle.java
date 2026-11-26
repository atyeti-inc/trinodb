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
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.type.TypeDeserializer;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoTableHandle
{
    private final JsonCodec<MongoTableHandle> codec;

    public TestMongoTableHandle()
    {
        JsonMapper jsonMapper = new JsonMapperProvider()
                .withJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)))
                .get();
        codec = new JsonCodecFactory(jsonMapper).jsonCodec(MongoTableHandle.class);
    }

    @Test
    public void testRoundTripWithoutQuery()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoTableHandle expected = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty());

        String json = codec.toJson(expected);
        MongoTableHandle actual = codec.fromJson(json);

        assertThat(actual.schemaTableName())
                .isEqualTo(expected.schemaTableName());
    }

    @Test
    public void testRoundTripNonLowercaseWithoutQuery()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("Schema", "Table");
        MongoTableHandle expected = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty());

        String json = codec.toJson(expected);
        MongoTableHandle actual = codec.fromJson(json);

        assertThat(actual.schemaTableName())
                .isEqualTo(expected.schemaTableName());
    }

    @Test
    public void testRoundTripWithQuery()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoTableHandle expected = new MongoTableHandle(schemaTableName, remoteTableName, Optional.of("{\"key\": \"value\"}"));

        String json = codec.toJson(expected);
        MongoTableHandle actual = codec.fromJson(json);

        assertThat(actual.schemaTableName())
                .isEqualTo(expected.schemaTableName());
    }

    @Test
    public void testRoundTripWithQueryHavingHelperFunction()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoTableHandle expected = new MongoTableHandle(schemaTableName, remoteTableName, Optional.of("{timestamp: ISODate(\"2023-03-20T01:02:03.000Z\")}"));

        String json = codec.toJson(expected);
        MongoTableHandle actual = codec.fromJson(json);

        assertThat(actual.schemaTableName())
                .isEqualTo(expected.schemaTableName());
    }

    @Test
    public void testRoundTripWithProjectedColumns()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("Schema", "Table");
        Set<MongoColumnHandle> projectedColumns = ImmutableSet.of(
                new MongoColumnHandle("id", ImmutableList.of(), INTEGER, false, false, Optional.empty()),
                new MongoColumnHandle("address", ImmutableList.of("street"), VARCHAR, false, false, Optional.empty()),
                new MongoColumnHandle(
                        "user",
                        ImmutableList.of(),
                        RowType.from(ImmutableList.of(new RowType.Field(Optional.of("first"), VARCHAR), new RowType.Field(Optional.of("last"), VARCHAR))),
                        false,
                        false,
                        Optional.empty()),
                new MongoColumnHandle("creator", ImmutableList.of("databasename"), VARCHAR, false, true, Optional.empty()));

        MongoTableHandle expected = new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                Optional.empty(),
                TupleDomain.all(),
                projectedColumns,
                OptionalInt.empty());

        String json = codec.toJson(expected);
        MongoTableHandle actual = codec.fromJson(json);

        assertThat(actual)
                .isEqualTo(expected);
    }

    @Test
    public void testRoundTripWithAggregation()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoColumnHandle deptCol = new MongoColumnHandle("dept_id", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle salaryCol = new MongoColumnHandle("salary", ImmutableList.of(), DOUBLE, false, false, Optional.empty());
        MongoColumnHandle totalCol = new MongoColumnHandle("total", ImmutableList.of(), DOUBLE, false, false, Optional.empty());
        MongoColumnHandle countCol = new MongoColumnHandle("cnt", ImmutableList.of(), BIGINT, false, false, Optional.empty());

        MongoAccumulator sumSalary = new MongoAccumulator("sum", ImmutableList.of(salaryCol), totalCol);
        MongoAccumulator countAll = new MongoAccumulator("count", ImmutableList.of(), countCol);
        MongoAggregation aggregation = new MongoAggregation(ImmutableList.of(deptCol), ImmutableList.of(sumSalary, countAll));

        MongoTableHandle expected = new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(deptCol, totalCol, countCol),
                OptionalInt.of(10),
                Optional.of(aggregation));

        String json = codec.toJson(expected);
        MongoTableHandle actual = codec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.hashCode()).isEqualTo(expected.hashCode());
        assertThat(actual.aggregation()).isEqualTo(Optional.of(aggregation));
    }

    @Test
    public void testBackwardCompatibleDeserialization()
    {
        // Legacy JSON without aggregation field
        String legacyJson =
                """
                {
                    "schemaTableName": {"schema": "schema", "table": "table"},
                    "remoteTableName": {"databaseName": "schema", "collectionName": "table"},
                    "filter": null,
                    "constraint": {},
                    "projectedColumns": [],
                    "limit": null
                }
                """;
        MongoTableHandle actual = codec.fromJson(legacyJson);
        assertThat(actual.aggregation()).isEmpty();

        // JSON with explicit null aggregation
        String jsonWithNullAggregation =
                """
                {
                    "schemaTableName": {"schema": "schema", "table": "table"},
                    "remoteTableName": {"databaseName": "schema", "collectionName": "table"},
                    "filter": null,
                    "constraint": {},
                    "projectedColumns": [],
                    "limit": null,
                    "aggregation": null
                }
                """;
        MongoTableHandle actualWithNull = codec.fromJson(jsonWithNullAggregation);
        assertThat(actualWithNull.aggregation()).isEmpty();
    }

    @Test
    public void testToStringWithAggregation()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoColumnHandle deptCol = new MongoColumnHandle("dept_id", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle salaryCol = new MongoColumnHandle("salary", ImmutableList.of(), DOUBLE, false, false, Optional.empty());
        MongoColumnHandle totalCol = new MongoColumnHandle("total", ImmutableList.of(), DOUBLE, false, false, Optional.empty());
        MongoColumnHandle countCol = new MongoColumnHandle("cnt", ImmutableList.of(), BIGINT, false, false, Optional.empty());

        MongoAccumulator sumSalary = new MongoAccumulator("sum", ImmutableList.of(salaryCol), totalCol);
        MongoAccumulator countAll = new MongoAccumulator("count", ImmutableList.of(), countCol);
        MongoAggregation aggregation = new MongoAggregation(ImmutableList.of(deptCol), ImmutableList.of(sumSalary, countAll));

        MongoTableHandle handle = new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(),
                OptionalInt.empty(),
                Optional.of(aggregation));

        assertThat(handle.toString()).contains("aggregation=[groupBy=[dept_id], aggregations=[total := sum(salary), cnt := count(*)]]");

        MongoAggregation globalAgg = new MongoAggregation(ImmutableList.of(), ImmutableList.of(countAll));
        MongoTableHandle globalHandle = handle.withAggregation(globalAgg);
        assertThat(globalHandle.toString()).contains("aggregation=[global, aggregations=[cnt := count(*)]]");
    }

    @Test
    public void testWitherMethodsPreserveAggregation()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoColumnHandle countCol = new MongoColumnHandle("cnt", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator countAll = new MongoAccumulator("count", ImmutableList.of(), countCol);
        MongoAggregation aggregation = new MongoAggregation(ImmutableList.of(), ImmutableList.of(countAll));

        MongoTableHandle handle = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty())
                .withAggregation(aggregation);

        assertThat(handle.aggregation()).isEqualTo(Optional.of(aggregation));

        MongoTableHandle withLimit = handle.withLimit(OptionalInt.of(100));
        assertThat(withLimit.aggregation()).isEqualTo(Optional.of(aggregation));
        assertThat(withLimit.limit()).isEqualTo(OptionalInt.of(100));

        MongoTableHandle withConstraint = handle.withConstraint(TupleDomain.none());
        assertThat(withConstraint.aggregation()).isEqualTo(Optional.of(aggregation));
        assertThat(withConstraint.constraint().isNone()).isTrue();

        MongoTableHandle withProjectedColumns = handle.withProjectedColumns(ImmutableSet.of(countCol));
        assertThat(withProjectedColumns.aggregation()).isEqualTo(Optional.of(aggregation));
        assertThat(withProjectedColumns.projectedColumns()).containsExactly(countCol);
    }
}
