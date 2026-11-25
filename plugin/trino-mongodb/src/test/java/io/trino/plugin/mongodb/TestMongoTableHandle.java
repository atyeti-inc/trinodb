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
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

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
        MongoColumnHandle custkey = new MongoColumnHandle("custkey", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoColumnHandle price = new MongoColumnHandle("totalprice", ImmutableList.of(), DOUBLE, false, false, Optional.empty());
        MongoColumnHandle countCol = new MongoColumnHandle("cnt", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoColumnHandle sumCol = new MongoColumnHandle("sum_price", ImmutableList.of(), DOUBLE, false, false, Optional.empty());

        MongoAccumulator countAccumulator = new MongoAccumulator(countCol, "count", ImmutableList.of(), false);
        MongoAccumulator sumAccumulator = new MongoAccumulator(sumCol, "sum", ImmutableList.of(price), false);

        MongoAggregation aggregation = new MongoAggregation(
                ImmutableList.of(custkey),
                ImmutableList.of(countAccumulator, sumAccumulator));

        MongoTableHandle expected = new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(custkey),
                OptionalInt.empty(),
                Optional.of(aggregation));

        String json = codec.toJson(expected);
        MongoTableHandle actual = codec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.aggregation()).isEqualTo(Optional.of(aggregation));
    }

    @Test
    public void testRoundTripLegacyJsonWithoutAggregation()
            throws Exception
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoTableHandle handle = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty());

        JsonMapper mapper = new JsonMapper();
        JsonNode tree = mapper.readTree(codec.toJson(handle));
        ((ObjectNode) tree).remove("aggregation");
        String legacyJson = mapper.writeValueAsString(tree);

        assertThat(legacyJson).doesNotContain("\"aggregation\"");
        MongoTableHandle actual = codec.fromJson(legacyJson);
        assertThat(actual.aggregation()).isEmpty();
        assertThat(actual).isEqualTo(handle);
    }

    @Test
    public void testEqualsAndHashCodeWithAggregation()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoColumnHandle col1 = new MongoColumnHandle("c1", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle col2 = new MongoColumnHandle("c2", ImmutableList.of(), VARCHAR, false, false, Optional.empty());

        MongoAccumulator acc1 = new MongoAccumulator(col1, "count", ImmutableList.of(), false);
        MongoAccumulator acc1Distinct = new MongoAccumulator(col1, "count", ImmutableList.of(), true);
        MongoAccumulator acc2 = new MongoAccumulator(col2, "sum", ImmutableList.of(col1), false);

        MongoAggregation agg1 = new MongoAggregation(ImmutableList.of(col1), ImmutableList.of(acc1));
        MongoAggregation agg1Same = new MongoAggregation(ImmutableList.of(col1), ImmutableList.of(acc1));
        MongoAggregation aggDistinct = new MongoAggregation(ImmutableList.of(col1), ImmutableList.of(acc1Distinct));
        MongoAggregation agg2 = new MongoAggregation(ImmutableList.of(col2), ImmutableList.of(acc1));
        MongoAggregation agg3 = new MongoAggregation(ImmutableList.of(col1), ImmutableList.of(acc2));

        MongoTableHandle handle1 = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty(), TupleDomain.all(), ImmutableSet.of(), OptionalInt.empty(), Optional.of(agg1));
        MongoTableHandle handle1Same = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty(), TupleDomain.all(), ImmutableSet.of(), OptionalInt.empty(), Optional.of(agg1Same));
        MongoTableHandle handleDistinct = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty(), TupleDomain.all(), ImmutableSet.of(), OptionalInt.empty(), Optional.of(aggDistinct));
        MongoTableHandle handleDiffGroupBy = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty(), TupleDomain.all(), ImmutableSet.of(), OptionalInt.empty(), Optional.of(agg2));
        MongoTableHandle handleDiffAcc = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty(), TupleDomain.all(), ImmutableSet.of(), OptionalInt.empty(), Optional.of(agg3));
        MongoTableHandle handleNoAgg = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty(), TupleDomain.all(), ImmutableSet.of(), OptionalInt.empty(), Optional.empty());

        assertThat(handle1).isEqualTo(handle1Same);
        assertThat(handle1.hashCode()).isEqualTo(handle1Same.hashCode());

        assertThat(handle1).isNotEqualTo(handleDistinct);
        assertThat(handle1).isNotEqualTo(handleDiffGroupBy);
        assertThat(handle1).isNotEqualTo(handleDiffAcc);
        assertThat(handle1).isNotEqualTo(handleNoAgg);
    }

    @Test
    public void testToStringFormatting()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoTableHandle handleWithoutAggregation = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty());
        assertThat(handleWithoutAggregation.toString()).isEqualTo("schema.table");

        MongoColumnHandle cityCol = new MongoColumnHandle("city", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle cntCol = new MongoColumnHandle("cnt", ImmutableList.of(), INTEGER, false, false, Optional.empty());
        MongoAccumulator countAccumulator = new MongoAccumulator(cntCol, "count", ImmutableList.of(), false);
        MongoAggregation aggregation = new MongoAggregation(ImmutableList.of(cityCol), ImmutableList.of(countAccumulator));
        MongoTableHandle handleWithAggregation = handleWithoutAggregation.withAggregation(aggregation);

        assertThat(handleWithAggregation.toString())
                .isEqualTo("schema.table aggregation=[groupBy=[city], accumulators=[cnt := count(*)]]");
    }
}
