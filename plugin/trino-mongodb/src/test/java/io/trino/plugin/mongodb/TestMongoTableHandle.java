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
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoTableHandle
{
    private final JsonCodec<MongoTableHandle> codec;
    private final JsonCodec<MongoAccumulator> accumulatorCodec;

    public TestMongoTableHandle()
    {
        JsonMapper jsonMapper = new JsonMapperProvider()
                .withJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)))
                .get();
        JsonCodecFactory codecFactory = new JsonCodecFactory(jsonMapper);
        codec = codecFactory.jsonCodec(MongoTableHandle.class);
        accumulatorCodec = codecFactory.jsonCodec(MongoAccumulator.class);
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
        MongoColumnHandle groupCol = new MongoColumnHandle("col_group", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle countResult = new MongoColumnHandle("_count", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator countAll = new MongoAccumulator(countResult, "count", ImmutableList.of(), false);

        MongoColumnHandle totalCol = new MongoColumnHandle("total", ImmutableList.of(), INTEGER, false, false, Optional.empty());
        MongoColumnHandle sumResult = new MongoColumnHandle("_sum", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator sumTotal = new MongoAccumulator(sumResult, "sum", ImmutableList.of(totalCol), false);

        MongoColumnHandle custCol = new MongoColumnHandle("customer", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle distinctCustResult = new MongoColumnHandle("_distinct_cust", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator distinctCust = new MongoAccumulator(distinctCustResult, "count", ImmutableList.of(custCol), true);

        MongoAggregation aggregation = new MongoAggregation(
                ImmutableList.of(groupCol),
                ImmutableList.of(countAll, sumTotal, distinctCust));

        MongoTableHandle expected = new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(),
                OptionalInt.empty(),
                Optional.of(aggregation));

        String json = codec.toJson(expected);
        MongoTableHandle actual = codec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.aggregation()).isEqualTo(Optional.of(aggregation));
    }

    @Test
    public void testDeserializationWithoutAggregationField()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoTableHandle handle = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty());
        String json = codec.toJson(handle);
        // Remove aggregation field to simulate serialized data from earlier versions
        json = json.replace(",\"aggregation\":null", "").replace("\"aggregation\":null,", "");
        MongoTableHandle actual = codec.fromJson(json);

        assertThat(actual.aggregation()).isEmpty();
        assertThat(actual).isEqualTo(handle);
    }

    @Test
    public void testAccumulatorSerializationJsonIgnore()
    {
        MongoColumnHandle countResult = new MongoColumnHandle("_count", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator accumulator = new MongoAccumulator(countResult, "count", ImmutableList.of(), false);

        String json = accumulatorCodec.toJson(accumulator);
        assertThat(json).doesNotContain("countAll");
        assertThat(json).doesNotContain("outputType");

        MongoAccumulator roundTripped = accumulatorCodec.fromJson(json);
        assertThat(roundTripped).isEqualTo(accumulator);
        assertThat(roundTripped.isCountAll()).isTrue();
        assertThat(roundTripped.outputType()).isEqualTo(BIGINT);
    }

    @Test
    public void testEqualsAndHashCode()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");

        MongoColumnHandle col1 = new MongoColumnHandle("col1", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle col2 = new MongoColumnHandle("col2", ImmutableList.of(), INTEGER, false, false, Optional.empty());
        MongoColumnHandle countResult = new MongoColumnHandle("_count", ImmutableList.of(), BIGINT, false, false, Optional.empty());

        MongoAccumulator acc1 = new MongoAccumulator(countResult, "count", ImmutableList.of(), false);
        MongoAccumulator acc2 = new MongoAccumulator(countResult, "count", ImmutableList.of(col1), false);
        MongoAccumulator acc3 = new MongoAccumulator(countResult, "count", ImmutableList.of(col1), true);

        MongoAggregation agg1 = new MongoAggregation(ImmutableList.of(col1), ImmutableList.of(acc1));
        MongoAggregation agg1Same = new MongoAggregation(ImmutableList.of(col1), ImmutableList.of(acc1));
        MongoAggregation aggDifferentGroup = new MongoAggregation(ImmutableList.of(col2), ImmutableList.of(acc1));
        MongoAggregation aggDifferentAcc = new MongoAggregation(ImmutableList.of(col1), ImmutableList.of(acc2));
        MongoAggregation aggDifferentDistinct = new MongoAggregation(ImmutableList.of(col1), ImmutableList.of(acc3));

        MongoTableHandle handleNoAgg1 = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty());
        MongoTableHandle handleNoAgg2 = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty());
        MongoTableHandle handleAgg1 = handleNoAgg1.withAggregation(agg1);
        MongoTableHandle handleAgg1Same = handleNoAgg2.withAggregation(agg1Same);
        MongoTableHandle handleAggDiffGroup = handleNoAgg1.withAggregation(aggDifferentGroup);
        MongoTableHandle handleAggDiffAcc = handleNoAgg1.withAggregation(aggDifferentAcc);
        MongoTableHandle handleAggDiffDistinct = handleNoAgg1.withAggregation(aggDifferentDistinct);

        assertThat(handleAgg1).isEqualTo(handleAgg1Same);
        assertThat(handleAgg1.hashCode()).isEqualTo(handleAgg1Same.hashCode());

        assertThat(handleAgg1).isNotEqualTo(handleNoAgg1);
        assertThat(handleAgg1).isNotEqualTo(handleAggDiffGroup);
        assertThat(handleAgg1).isNotEqualTo(handleAggDiffAcc);
        assertThat(handleAgg1).isNotEqualTo(handleAggDiffDistinct);
    }

    @Test
    public void testToStringRepresentation()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");

        MongoColumnHandle cityCol = new MongoColumnHandle("city", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle countResult = new MongoColumnHandle("_count", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator countAll = new MongoAccumulator(countResult, "count", ImmutableList.of(), false);

        MongoColumnHandle totalPriceCol = new MongoColumnHandle("totalprice", ImmutableList.of(), INTEGER, false, false, Optional.empty());
        MongoColumnHandle sumResult = new MongoColumnHandle("_sum", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator sumTotalPrice = new MongoAccumulator(sumResult, "sum", ImmutableList.of(totalPriceCol), false);

        // Global aggregation (empty groupBy)
        MongoAggregation globalAggregation = new MongoAggregation(ImmutableList.of(), ImmutableList.of(countAll));
        MongoTableHandle globalHandle = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty())
                .withAggregation(globalAggregation);
        assertThat(globalHandle.toString()).contains("groupBy=[] aggregate=[_count := count(*)]");

        // Grouped aggregation
        MongoAggregation groupedAggregation = new MongoAggregation(ImmutableList.of(cityCol), ImmutableList.of(countAll, sumTotalPrice));
        MongoTableHandle groupedHandle = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty())
                .withAggregation(groupedAggregation);
        assertThat(groupedHandle.toString()).contains("groupBy=[city] aggregate=[_count := count(*), _sum := sum(totalprice)]");

        // Copy methods preserve aggregation
        MongoTableHandle handleWithProj = groupedHandle.withProjectedColumns(ImmutableSet.of(cityCol));
        assertThat(handleWithProj.aggregation()).isEqualTo(Optional.of(groupedAggregation));

        MongoTableHandle handleWithConstraint = groupedHandle.withConstraint(TupleDomain.none());
        assertThat(handleWithConstraint.aggregation()).isEqualTo(Optional.of(groupedAggregation));
    }
}
