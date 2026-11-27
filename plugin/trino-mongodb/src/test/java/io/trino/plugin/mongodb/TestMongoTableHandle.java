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
    private static final MongoColumnHandle CUSTKEY = new MongoColumnHandle("custkey", ImmutableList.of(), BIGINT, false, false, Optional.empty());
    private static final MongoColumnHandle TOTALPRICE = new MongoColumnHandle("totalprice", ImmutableList.of(), DOUBLE, false, false, Optional.empty());
    private static final MongoColumnHandle AGG0 = new MongoColumnHandle("_agg_0", ImmutableList.of(), BIGINT, false, false, Optional.empty());
    private static final MongoColumnHandle AGG1 = new MongoColumnHandle("_agg_1", ImmutableList.of(), DOUBLE, false, false, Optional.empty());

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

        MongoAccumulator countAccumulator = new MongoAccumulator(AGG0, "count", ImmutableList.of());
        MongoAccumulator sumAccumulator = new MongoAccumulator(AGG1, "sum", ImmutableList.of(TOTALPRICE));

        // GroupBy and accumulators
        MongoAggregation aggregationWithBoth = new MongoAggregation(
                ImmutableList.of(CUSTKEY),
                ImmutableList.of(countAccumulator, sumAccumulator));
        MongoTableHandle expectedWithBoth = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty())
                .withAggregation(aggregationWithBoth);
        assertThat(codec.fromJson(codec.toJson(expectedWithBoth))).isEqualTo(expectedWithBoth);

        // Global aggregation (groupBy empty, accumulators populated)
        MongoAggregation globalAggregation = new MongoAggregation(
                ImmutableList.of(),
                ImmutableList.of(countAccumulator));
        MongoTableHandle expectedGlobal = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty())
                .withAggregation(globalAggregation);
        assertThat(codec.fromJson(codec.toJson(expectedGlobal))).isEqualTo(expectedGlobal);

        // Pure grouping (groupBy populated, accumulators empty)
        MongoAggregation pureGrouping = new MongoAggregation(
                ImmutableList.of(CUSTKEY),
                ImmutableList.of());
        MongoTableHandle expectedPureGrouping = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty())
                .withAggregation(pureGrouping);
        assertThat(codec.fromJson(codec.toJson(expectedPureGrouping))).isEqualTo(expectedPureGrouping);
    }

    @Test
    public void testBackwardCompatibilityWithoutAggregation()
    {
        String legacyJson =
                """
                {
                  "schemaTableName": {"schema": "schema", "table": "table"},
                  "remoteTableName": {"databaseName": "schema", "collectionName": "table"},
                  "filter": null,
                  "constraint": {"columnDomains": null},
                  "projectedColumns": [],
                  "limit": null
                }
                """;
        MongoTableHandle actual = codec.fromJson(legacyJson);
        assertThat(actual.aggregation()).isEmpty();
    }

    @Test
    public void testEqualsAndHashCodeWithAggregation()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoTableHandle baseHandle = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty());

        MongoAccumulator count0 = new MongoAccumulator(AGG0, "count", ImmutableList.of());
        MongoAccumulator sum0 = new MongoAccumulator(AGG1, "sum", ImmutableList.of(TOTALPRICE));

        MongoAggregation agg1 = new MongoAggregation(ImmutableList.of(CUSTKEY), ImmutableList.of(count0));
        MongoAggregation agg1Same = new MongoAggregation(ImmutableList.of(CUSTKEY), ImmutableList.of(count0));
        MongoAggregation aggDiffKey = new MongoAggregation(ImmutableList.of(TOTALPRICE), ImmutableList.of(count0));
        MongoAggregation aggDiffAcc = new MongoAggregation(ImmutableList.of(CUSTKEY), ImmutableList.of(sum0));

        MongoTableHandle handle1 = baseHandle.withAggregation(agg1);
        MongoTableHandle handle1Same = baseHandle.withAggregation(agg1Same);
        MongoTableHandle handleDiffKey = baseHandle.withAggregation(aggDiffKey);
        MongoTableHandle handleDiffAcc = baseHandle.withAggregation(aggDiffAcc);

        assertThat(handle1).isEqualTo(handle1Same);
        assertThat(handle1.hashCode()).isEqualTo(handle1Same.hashCode());

        assertThat(handle1).isNotEqualTo(baseHandle);
        assertThat(handle1).isNotEqualTo(handleDiffKey);
        assertThat(handle1.hashCode()).isNotEqualTo(handleDiffKey.hashCode());

        assertThat(handle1).isNotEqualTo(handleDiffAcc);
        assertThat(handle1.hashCode()).isNotEqualTo(handleDiffAcc.hashCode());
    }

    @Test
    public void testWitherMethodsPreserveAggregation()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoAccumulator countAccumulator = new MongoAccumulator(AGG0, "count", ImmutableList.of());
        MongoAggregation aggregation = new MongoAggregation(ImmutableList.of(CUSTKEY), ImmutableList.of(countAccumulator));

        MongoTableHandle handle = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty())
                .withAggregation(aggregation);

        // withConstraint
        MongoTableHandle withConstraint = handle.withConstraint(TupleDomain.none());
        assertThat(withConstraint.aggregation()).isEqualTo(Optional.of(aggregation));
        assertThat(withConstraint.constraint()).isEqualTo(TupleDomain.none());
        assertThat(withConstraint.schemaTableName()).isEqualTo(schemaTableName);

        // withProjectedColumns
        Set<MongoColumnHandle> cols = ImmutableSet.of(CUSTKEY);
        MongoTableHandle withColumns = handle.withProjectedColumns(cols);
        assertThat(withColumns.aggregation()).isEqualTo(Optional.of(aggregation));
        assertThat(withColumns.projectedColumns()).isEqualTo(cols);

        // withLimit
        MongoTableHandle withLimit = handle.withLimit(OptionalInt.of(10));
        assertThat(withLimit.aggregation()).isEqualTo(Optional.of(aggregation));
        assertThat(withLimit.limit()).isEqualTo(OptionalInt.of(10));

        // withAggregation optional / non-optional
        MongoTableHandle clearedAggregation = handle.withAggregation(Optional.empty());
        assertThat(clearedAggregation.aggregation()).isEmpty();

        MongoAggregation newAgg = new MongoAggregation(ImmutableList.of(), ImmutableList.of(countAccumulator));
        MongoTableHandle withNewAgg = clearedAggregation.withAggregation(newAgg);
        assertThat(withNewAgg.aggregation()).isEqualTo(Optional.of(newAgg));
    }

    @Test
    public void testToStringWithAggregation()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoAccumulator countAgg = new MongoAccumulator(AGG0, "count", ImmutableList.of());
        MongoAccumulator sumAgg = new MongoAccumulator(AGG1, "sum", ImmutableList.of(TOTALPRICE));
        MongoAggregation aggregation = new MongoAggregation(
                ImmutableList.of(CUSTKEY),
                ImmutableList.of(countAgg, sumAgg));

        MongoTableHandle handle = new MongoTableHandle(schemaTableName, remoteTableName, Optional.empty())
                .withAggregation(aggregation);

        assertThat(handle.toString()).contains("aggregation=[keys=[custkey], accumulators=[_agg_0=count(), _agg_1=sum(totalprice)]]");
    }

    @Test
    public void testMongoAccumulatorIsCountAll()
    {
        assertThat(new MongoAccumulator(AGG0, "count", ImmutableList.of()).isCountAll()).isTrue();
        assertThat(new MongoAccumulator(AGG0, "COUNT", ImmutableList.of()).isCountAll()).isTrue();
        assertThat(new MongoAccumulator(AGG0, "Count", ImmutableList.of()).isCountAll()).isTrue();

        // Non-empty arguments
        assertThat(new MongoAccumulator(AGG0, "count", ImmutableList.of(CUSTKEY)).isCountAll()).isFalse();

        // Non-count functions
        assertThat(new MongoAccumulator(AGG0, "sum", ImmutableList.of()).isCountAll()).isFalse();
        assertThat(new MongoAccumulator(AGG0, "avg", ImmutableList.of()).isCountAll()).isFalse();
        assertThat(new MongoAccumulator(AGG0, "min", ImmutableList.of(CUSTKEY)).isCountAll()).isFalse();
        assertThat(new MongoAccumulator(AGG0, "max", ImmutableList.of(CUSTKEY)).isCountAll()).isFalse();
    }

    @Test
    public void testMongoAccumulatorOutputType()
    {
        MongoAccumulator countAccumulator = new MongoAccumulator(AGG0, "count", ImmutableList.of());
        assertThat(countAccumulator.outputType()).isEqualTo(BIGINT);

        MongoAccumulator sumAccumulator = new MongoAccumulator(AGG1, "sum", ImmutableList.of(TOTALPRICE));
        assertThat(sumAccumulator.outputType()).isEqualTo(DOUBLE);
    }
}
