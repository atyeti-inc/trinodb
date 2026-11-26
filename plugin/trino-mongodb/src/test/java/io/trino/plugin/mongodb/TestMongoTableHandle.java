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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorSession;
import io.trino.type.TypeDeserializer;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoTableHandle
{
    private static final MongoColumnHandle COL_A = new MongoColumnHandle("colA", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
    private static final MongoColumnHandle COL_B = new MongoColumnHandle("colB", ImmutableList.of(), INTEGER, false, false, Optional.empty());
    private static final MongoColumnHandle COL_C = new MongoColumnHandle("colC", ImmutableList.of(), BIGINT, false, false, Optional.empty());
    private static final MongoColumnHandle COL_PRICE = new MongoColumnHandle("price", ImmutableList.of(), DOUBLE, false, false, Optional.empty());
    private static final MongoColumnHandle COL_TS = new MongoColumnHandle("ts", ImmutableList.of(), TIMESTAMP_MILLIS, false, false, Optional.empty());
    private static final MongoColumnHandle COL_CUSTOMER = new MongoColumnHandle("customer", ImmutableList.of(), VARCHAR, false, false, Optional.empty());

    private static final MongoColumnHandle RESULT_CNT = new MongoColumnHandle("cnt", ImmutableList.of(), BIGINT, false, false, Optional.empty());
    private static final MongoColumnHandle RESULT_TOTAL = new MongoColumnHandle("total", ImmutableList.of(), BIGINT, false, false, Optional.empty());
    private static final MongoColumnHandle RESULT_AVG = new MongoColumnHandle("avg_price", ImmutableList.of(), DOUBLE, false, false, Optional.empty());
    private static final MongoColumnHandle RESULT_MIN = new MongoColumnHandle("min_ts", ImmutableList.of(), TIMESTAMP_MILLIS, false, false, Optional.empty());
    private static final MongoColumnHandle RESULT_MAX = new MongoColumnHandle("max_ts", ImmutableList.of(), TIMESTAMP_MILLIS, false, false, Optional.empty());

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
    public void testRoundTripWithAggregationOptionalEmpty()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");
        MongoTableHandle expected = new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(),
                OptionalInt.empty(),
                Optional.empty());

        String json = codec.toJson(expected);
        MongoTableHandle actual = codec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.aggregation()).isEmpty();
    }

    @Test
    public void testRoundTripWithAggregationSingleAndMultiColumnGrouping()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");

        // Single grouping key
        MongoAggregationHandle singleGroup = new MongoAggregationHandle(
                ImmutableList.of(COL_A),
                ImmutableList.of());
        MongoTableHandle expectedSingle = new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(),
                OptionalInt.empty(),
                Optional.of(singleGroup));

        String jsonSingle = codec.toJson(expectedSingle);
        MongoTableHandle actualSingle = codec.fromJson(jsonSingle);
        assertThat(actualSingle).isEqualTo(expectedSingle);

        // Multi grouping keys
        MongoAggregationHandle multiGroup = new MongoAggregationHandle(
                ImmutableList.of(COL_A, COL_B),
                ImmutableList.of());
        MongoTableHandle expectedMulti = new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(),
                OptionalInt.empty(),
                Optional.of(multiGroup));

        String jsonMulti = codec.toJson(expectedMulti);
        MongoTableHandle actualMulti = codec.fromJson(jsonMulti);
        assertThat(actualMulti).isEqualTo(expectedMulti);
    }

    @Test
    public void testRoundTripWithAggregationStandardAccumulators()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");

        ImmutableList<MongoAccumulator> accumulators = ImmutableList.of(
                new MongoAccumulator(RESULT_CNT, "count", ImmutableList.of(), false),
                new MongoAccumulator(RESULT_TOTAL, "sum", ImmutableList.of(COL_C), false),
                new MongoAccumulator(RESULT_AVG, "avg", ImmutableList.of(COL_PRICE), false),
                new MongoAccumulator(RESULT_MIN, "min", ImmutableList.of(COL_TS), false),
                new MongoAccumulator(RESULT_MAX, "max", ImmutableList.of(COL_TS), false));

        MongoAggregationHandle aggregation = new MongoAggregationHandle(
                ImmutableList.of(COL_A, COL_B),
                accumulators);

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
        assertThat(actual.aggregation()).contains(aggregation);
    }

    @Test
    public void testRoundTripWithAggregationDistinctAccumulator()
    {
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RemoteTableName remoteTableName = new RemoteTableName("schema", "table");

        MongoAggregationHandle aggregation = new MongoAggregationHandle(
                ImmutableList.of(COL_A),
                ImmutableList.of(new MongoAccumulator(RESULT_CNT, "count", ImmutableList.of(COL_CUSTOMER), true)));

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
        assertThat(actual.aggregation().orElseThrow().accumulators().getFirst().distinct()).isTrue();
    }

    @Test
    public void testLegacyDeserializationWithoutAggregation()
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
        assertThat(actual.schemaTableName()).isEqualTo(new SchemaTableName("schema", "table"));
        assertThat(actual.remoteTableName()).isEqualTo(new RemoteTableName("schema", "table"));
        assertThat(actual.aggregation()).isEmpty();
    }

    @Test
    public void testEqualsAndHashCode()
    {
        MongoAggregationHandle agg1 = new MongoAggregationHandle(
                ImmutableList.of(COL_A),
                ImmutableList.of(new MongoAccumulator(RESULT_CNT, "count", ImmutableList.of(), false)));
        MongoAggregationHandle agg2 = new MongoAggregationHandle(
                ImmutableList.of(COL_A),
                ImmutableList.of(new MongoAccumulator(RESULT_CNT, "count", ImmutableList.of(), false)));

        MongoTableHandle handle1 = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(),
                OptionalInt.empty(),
                Optional.of(agg1));

        MongoTableHandle handle2 = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(),
                OptionalInt.empty(),
                Optional.of(agg2));

        assertThat(handle1)
                .isEqualTo(handle2)
                .hasSameHashCodeAs(handle2);

        // Differing grouping keys
        MongoTableHandle diffGrouping = handle1.withAggregation(Optional.of(new MongoAggregationHandle(
                ImmutableList.of(COL_B),
                agg1.accumulators())));
        assertThat(handle1).isNotEqualTo(diffGrouping);

        // Differing accumulators
        MongoTableHandle diffAccumulators = handle1.withAggregation(Optional.of(new MongoAggregationHandle(
                agg1.groupingKeys(),
                ImmutableList.of(new MongoAccumulator(RESULT_TOTAL, "sum", ImmutableList.of(COL_C), false)))));
        assertThat(handle1).isNotEqualTo(diffAccumulators);

        // Differing distinctness
        MongoTableHandle diffDistinct = handle1.withAggregation(Optional.of(new MongoAggregationHandle(
                agg1.groupingKeys(),
                ImmutableList.of(new MongoAccumulator(RESULT_CNT, "count", ImmutableList.of(), true)))));
        assertThat(handle1).isNotEqualTo(diffDistinct);

        // Differing function names
        MongoTableHandle diffFunction = handle1.withAggregation(Optional.of(new MongoAggregationHandle(
                agg1.groupingKeys(),
                ImmutableList.of(new MongoAccumulator(RESULT_CNT, "approx_count", ImmutableList.of(), false)))));
        assertThat(handle1).isNotEqualTo(diffFunction);

        // Empty aggregation vs present aggregation
        MongoTableHandle noAggregation = handle1.withAggregation(Optional.empty());
        assertThat(handle1).isNotEqualTo(noAggregation);
    }

    @Test
    public void testToStringWithAggregation()
    {
        MongoAggregationHandle aggregation = new MongoAggregationHandle(
                ImmutableList.of(COL_A, COL_B),
                ImmutableList.of(
                        new MongoAccumulator(RESULT_CNT, "count", ImmutableList.of(), false),
                        new MongoAccumulator(RESULT_TOTAL, "sum", ImmutableList.of(COL_C), false)));

        MongoTableHandle handle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(),
                OptionalInt.empty(),
                Optional.of(aggregation));

        assertThat(handle.toString()).contains("aggregation=[groupBy=[colA, colB], accumulators=[cnt=count(*), total=sum(colC)]]");
    }

    @Test
    public void testMetadataPreservesAggregation()
    {
        MongoSession mongoSession = new MongoSession(TESTING_TYPE_MANAGER, () -> null, new MongoClientConfig());
        MongoMetadata metadata = new MongoMetadata(mongoSession);
        ConnectorSession session = TestingConnectorSession.builder().build();

        MongoAggregationHandle aggregation = new MongoAggregationHandle(
                ImmutableList.of(COL_A),
                ImmutableList.of(new MongoAccumulator(RESULT_CNT, "count", ImmutableList.of(), false)));

        MongoTableHandle handle = new MongoTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName("schema", "table"),
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(),
                OptionalInt.empty(),
                Optional.of(aggregation));

        // applyLimit preserves aggregation
        Optional<LimitApplicationResult<ConnectorTableHandle>> limitResult = metadata.applyLimit(session, handle, 10);
        assertThat(limitResult).isPresent();
        MongoTableHandle limitedHandle = (MongoTableHandle) limitResult.get().getHandle();
        assertThat(limitedHandle.aggregation()).contains(aggregation);
        assertThat(limitedHandle.limit()).isEqualTo(OptionalInt.of(10));

        // applyFilter preserves aggregation
        TupleDomain<ColumnHandle> filterDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(COL_A, Domain.singleValue(VARCHAR, utf8Slice("test"))));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> filterResult = metadata.applyFilter(
                session,
                handle,
                new Constraint(filterDomain));
        assertThat(filterResult).isPresent();
        MongoTableHandle filteredHandle = (MongoTableHandle) filterResult.get().getHandle();
        assertThat(filteredHandle.aggregation()).contains(aggregation);
        assertThat(filteredHandle.constraint()).isEqualTo(filterDomain);
    }
}
