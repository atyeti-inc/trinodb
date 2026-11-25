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
import io.airlift.testing.EquivalenceTester;
import io.trino.spi.connector.Constraint;
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
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
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
        MongoColumnHandle dept = new MongoColumnHandle("dept", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle res0 = new MongoColumnHandle("_col0", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator countStar = new MongoAccumulator(res0, "count", ImmutableList.of());
        MongoAggregation aggregation = new MongoAggregation(ImmutableList.of(dept), ImmutableList.of(countStar));

        MongoTableHandle expected = new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(dept),
                OptionalInt.empty(),
                Optional.of(aggregation));

        String json = codec.toJson(expected);
        MongoTableHandle actual = codec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.aggregation()).isEqualTo(expected.aggregation());
    }

    @Test
    public void testBackwardCompatibilityDeserialization()
    {
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
        assertThat(actual.schemaTableName()).isEqualTo(new SchemaTableName("schema", "table"));
    }

    @Test
    public void testEquivalence()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoColumnHandle dept = new MongoColumnHandle("dept", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle salary = new MongoColumnHandle("salary", ImmutableList.of(), INTEGER, false, false, Optional.empty());
        MongoColumnHandle res0 = new MongoColumnHandle("_col0", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator countStar = new MongoAccumulator(res0, "count", ImmutableList.of(), false);
        MongoAccumulator sumSalary = new MongoAccumulator(res0, "sum", ImmutableList.of(salary), false);
        MongoAccumulator sumSalaryDistinct = new MongoAccumulator(res0, "sum", ImmutableList.of(salary), true);

        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty()),
                        new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty()))
                .addEquivalentGroup(
                        new MongoTableHandle(new SchemaTableName("other", "table"), new RemoteTableName("other", "table"), Optional.empty()),
                        new MongoTableHandle(new SchemaTableName("other", "table"), new RemoteTableName("other", "table"), Optional.empty()))
                .addEquivalentGroup(
                        new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty(), TupleDomain.all(), ImmutableSet.of(), OptionalInt.empty(), Optional.of(new MongoAggregation(ImmutableList.of(dept), ImmutableList.of(countStar)))),
                        new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty(), TupleDomain.all(), ImmutableSet.of(), OptionalInt.empty(), Optional.of(new MongoAggregation(ImmutableList.of(dept), ImmutableList.of(countStar)))))
                .addEquivalentGroup(
                        new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty(), TupleDomain.all(), ImmutableSet.of(), OptionalInt.empty(), Optional.of(new MongoAggregation(ImmutableList.of(salary), ImmutableList.of(countStar)))),
                        new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty(), TupleDomain.all(), ImmutableSet.of(), OptionalInt.empty(), Optional.of(new MongoAggregation(ImmutableList.of(salary), ImmutableList.of(countStar)))))
                .addEquivalentGroup(
                        new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty(), TupleDomain.all(), ImmutableSet.of(), OptionalInt.empty(), Optional.of(new MongoAggregation(ImmutableList.of(dept), ImmutableList.of(sumSalary)))),
                        new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty(), TupleDomain.all(), ImmutableSet.of(), OptionalInt.empty(), Optional.of(new MongoAggregation(ImmutableList.of(dept), ImmutableList.of(sumSalary)))))
                .addEquivalentGroup(
                        new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty(), TupleDomain.all(), ImmutableSet.of(), OptionalInt.empty(), Optional.of(new MongoAggregation(ImmutableList.of(dept), ImmutableList.of(sumSalaryDistinct)))),
                        new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty(), TupleDomain.all(), ImmutableSet.of(), OptionalInt.empty(), Optional.of(new MongoAggregation(ImmutableList.of(dept), ImmutableList.of(sumSalaryDistinct)))))
                .check();
    }

    @Test
    public void testToStringWithAggregation()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoColumnHandle dept = new MongoColumnHandle("dept", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle salary = new MongoColumnHandle("salary", ImmutableList.of(), INTEGER, false, false, Optional.empty());
        MongoColumnHandle res0 = new MongoColumnHandle("_col0", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoColumnHandle res1 = new MongoColumnHandle("_col1", ImmutableList.of(), BIGINT, false, false, Optional.empty());

        MongoAccumulator countStar = new MongoAccumulator(res0, "count", ImmutableList.of());
        MongoAccumulator sumSalary = new MongoAccumulator(res1, "sum", ImmutableList.of(salary));

        MongoAggregation aggregation = new MongoAggregation(ImmutableList.of(dept), ImmutableList.of(countStar, sumSalary));
        MongoTableHandle handle = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty())
                .withAggregation(aggregation);

        assertThat(handle.toString()).contains("aggregation=[groupBy=[dept], aggregates=[count(*) AS _col0, sum(salary) AS _col1]]");
    }

    @Test
    public void testWithAggregation()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoTableHandle base = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty());
        assertThat(base.aggregation()).isEmpty();

        MongoColumnHandle dept = new MongoColumnHandle("dept", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoAggregation aggregation = new MongoAggregation(ImmutableList.of(dept), ImmutableList.of());

        MongoTableHandle withAgg = base.withAggregation(aggregation);
        assertThat(withAgg.aggregation()).contains(aggregation);

        MongoTableHandle withOptionalAgg = base.withAggregation(Optional.of(aggregation));
        assertThat(withOptionalAgg.aggregation()).contains(aggregation);

        MongoTableHandle cleared = withAgg.withAggregation(Optional.empty());
        assertThat(cleared.aggregation()).isEmpty();
    }

    @Test
    public void testAggregationPreservationOnMutations()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoColumnHandle dept = new MongoColumnHandle("dept", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoAggregation aggregation = new MongoAggregation(ImmutableList.of(dept), ImmutableList.of());

        MongoTableHandle handle = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty())
                .withAggregation(aggregation);

        MongoTableHandle withProjected = handle.withProjectedColumns(ImmutableSet.of(dept));
        assertThat(withProjected.aggregation()).contains(aggregation);

        MongoTableHandle withConstraint = handle.withConstraint(TupleDomain.none());
        assertThat(withConstraint.aggregation()).contains(aggregation);

        MongoSession mongoSession = new MongoSession(TESTING_TYPE_MANAGER, () -> null, new MongoClientConfig());
        MongoMetadata metadata = new MongoMetadata(mongoSession);

        var limitResult = metadata.applyLimit(SESSION, handle, 10);
        assertThat(limitResult).isPresent();
        MongoTableHandle limitedHandle = (MongoTableHandle) limitResult.get().getHandle();
        assertThat(limitedHandle.aggregation()).contains(aggregation);

        var filterResult = metadata.applyFilter(SESSION, handle, new Constraint(TupleDomain.none()));
        assertThat(filterResult).isPresent();
        MongoTableHandle filteredHandle = (MongoTableHandle) filterResult.get().getHandle();
        assertThat(filteredHandle.aggregation()).contains(aggregation);
    }
}
