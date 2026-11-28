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
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.plugin.mongodb.ObjectIdType.OBJECT_ID;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoAggregationTranslator
{
    private static final MongoTableHandle BASE_TABLE_HANDLE = new MongoTableHandle(
            new SchemaTableName("schema", "table"),
            new RemoteTableName("schema", "table"),
            Optional.empty(),
            TupleDomain.all(),
            ImmutableSet.of(),
            OptionalInt.empty(),
            Optional.empty());

    private static MongoColumnHandle createColumn(String name, Type type)
    {
        return new MongoColumnHandle(name, ImmutableList.of(), type, false, false, Optional.empty());
    }

    private static MongoColumnHandle createHiddenColumn(String name, Type type)
    {
        return new MongoColumnHandle(name, ImmutableList.of(), type, true, false, Optional.empty());
    }

    @Test
    public void testTranslateCountStar()
    {
        AggregateFunction countStar = new AggregateFunction("count", BIGINT, ImmutableList.of(), ImmutableList.of(), false, Optional.empty());
        Optional<AggregationApplicationResult<ConnectorTableHandle>> result =
                MongoAggregationTranslator.translate(BASE_TABLE_HANDLE, ImmutableList.of(countStar), ImmutableMap.of(), ImmutableList.of());

        assertThat(result).isPresent();
        MongoTableHandle tableHandle = (MongoTableHandle) result.get().getHandle();
        assertThat(tableHandle.aggregation()).isPresent();

        MongoAggregation aggregation = tableHandle.aggregation().get();
        assertThat(aggregation.groupingSets()).isEqualTo(ImmutableList.of(ImmutableList.of()));
        assertThat(aggregation.accumulators()).hasSize(1);

        MongoAccumulator accumulator = aggregation.accumulators().get(0);
        assertThat(accumulator.functionName()).isEqualTo("count");
        assertThat(accumulator.arguments()).isEmpty();
        assertThat(accumulator.distinct()).isFalse();
        assertThat(accumulator.projectedColumn().baseName()).isEqualTo("_mfgnrtd_0");
        assertThat(accumulator.projectedColumn().type()).isEqualTo(BIGINT);
    }

    @Test
    public void testTranslateCountColumn()
    {
        List<Type> testTypes = ImmutableList.of(BIGINT, VARCHAR, TIMESTAMP_MILLIS, BOOLEAN, OBJECT_ID);

        for (Type type : testTypes) {
            MongoColumnHandle column = createColumn("col", type);
            Map<String, ColumnHandle> assignments = ImmutableMap.of("col_var", column);
            AggregateFunction countCol = new AggregateFunction(
                    "count",
                    BIGINT,
                    ImmutableList.of(new Variable("col_var", type)),
                    ImmutableList.of(),
                    false,
                    Optional.empty());

            Optional<AggregationApplicationResult<ConnectorTableHandle>> result =
                    MongoAggregationTranslator.translate(BASE_TABLE_HANDLE, ImmutableList.of(countCol), assignments, ImmutableList.of());

            assertThat(result).as("count(" + type + ")").isPresent();
            MongoTableHandle tableHandle = (MongoTableHandle) result.get().getHandle();
            MongoAggregation aggregation = tableHandle.aggregation().orElseThrow();
            assertThat(aggregation.accumulators()).hasSize(1);
            MongoAccumulator accumulator = aggregation.accumulators().get(0);
            assertThat(accumulator.functionName()).isEqualTo("count");
            assertThat(accumulator.arguments()).containsExactly(column);
        }
    }

    @Test
    public void testTranslateSumAndAvg()
    {
        List<Type> numericTypes = ImmutableList.of(
                BIGINT,
                INTEGER,
                SMALLINT,
                TINYINT,
                REAL,
                DOUBLE,
                DecimalType.createDecimalType(10, 2),
                DecimalType.createDecimalType(38, 10));

        for (String functionName : ImmutableList.of("sum", "avg")) {
            for (Type type : numericTypes) {
                MongoColumnHandle column = createColumn("num_col", type);
                Map<String, ColumnHandle> assignments = ImmutableMap.of("num_var", column);
                Type outputType = functionName.equals("avg") ? DOUBLE : type;
                AggregateFunction aggregate = new AggregateFunction(
                        functionName,
                        outputType,
                        ImmutableList.of(new Variable("num_var", type)),
                        ImmutableList.of(),
                        false,
                        Optional.empty());

                Optional<AggregationApplicationResult<ConnectorTableHandle>> result =
                        MongoAggregationTranslator.translate(BASE_TABLE_HANDLE, ImmutableList.of(aggregate), assignments, ImmutableList.of());

                assertThat(result).as(functionName + "(" + type + ")").isPresent();
                MongoTableHandle tableHandle = (MongoTableHandle) result.get().getHandle();
                MongoAggregation aggregation = tableHandle.aggregation().orElseThrow();
                assertThat(aggregation.accumulators()).hasSize(1);
                MongoAccumulator accumulator = aggregation.accumulators().get(0);
                assertThat(accumulator.functionName()).isEqualTo(functionName);
                assertThat(accumulator.arguments()).containsExactly(column);
            }
        }
    }

    @Test
    public void testTranslateMinAndMax()
    {
        List<Type> orderableTypes = ImmutableList.of(
                BIGINT,
                INTEGER,
                SMALLINT,
                TINYINT,
                REAL,
                DOUBLE,
                DecimalType.createDecimalType(10, 2),
                VARCHAR,
                CharType.createCharType(10),
                DATE,
                TIME_MILLIS,
                TIMESTAMP_MILLIS,
                TIMESTAMP_TZ_MILLIS,
                BOOLEAN,
                OBJECT_ID);

        for (String functionName : ImmutableList.of("min", "max")) {
            for (Type type : orderableTypes) {
                MongoColumnHandle column = createColumn("ord_col", type);
                Map<String, ColumnHandle> assignments = ImmutableMap.of("ord_var", column);
                AggregateFunction aggregate = new AggregateFunction(
                        functionName,
                        type,
                        ImmutableList.of(new Variable("ord_var", type)),
                        ImmutableList.of(),
                        false,
                        Optional.empty());

                Optional<AggregationApplicationResult<ConnectorTableHandle>> result =
                        MongoAggregationTranslator.translate(BASE_TABLE_HANDLE, ImmutableList.of(aggregate), assignments, ImmutableList.of());

                assertThat(result).as(functionName + "(" + type + ")").isPresent();
                MongoTableHandle tableHandle = (MongoTableHandle) result.get().getHandle();
                MongoAggregation aggregation = tableHandle.aggregation().orElseThrow();
                assertThat(aggregation.accumulators()).hasSize(1);
                MongoAccumulator accumulator = aggregation.accumulators().get(0);
                assertThat(accumulator.functionName()).isEqualTo(functionName);
                assertThat(accumulator.arguments()).containsExactly(column);
            }
        }
    }

    @Test
    public void testRejectClauses()
    {
        MongoColumnHandle column = createColumn("val", BIGINT);
        Map<String, ColumnHandle> assignments = ImmutableMap.of("val_var", column);

        // DISTINCT rejected
        AggregateFunction distinctAgg = new AggregateFunction(
                "count",
                BIGINT,
                ImmutableList.of(new Variable("val_var", BIGINT)),
                ImmutableList.of(),
                true,
                Optional.empty());
        assertThat(MongoAggregationTranslator.translate(BASE_TABLE_HANDLE, ImmutableList.of(distinctAgg), assignments, ImmutableList.of()))
                .isEmpty();

        // FILTER rejected
        AggregateFunction filterAgg = new AggregateFunction(
                "count",
                BIGINT,
                ImmutableList.of(new Variable("val_var", BIGINT)),
                ImmutableList.of(),
                false,
                Optional.of(new Variable("filter_var", BOOLEAN)));
        assertThat(MongoAggregationTranslator.translate(BASE_TABLE_HANDLE, ImmutableList.of(filterAgg), assignments, ImmutableList.of()))
                .isEmpty();

        // ORDER BY / sortItems rejected
        AggregateFunction sortAgg = new AggregateFunction(
                "count",
                BIGINT,
                ImmutableList.of(new Variable("val_var", BIGINT)),
                ImmutableList.of(new SortItem("val_var", SortOrder.ASC_NULLS_LAST)),
                false,
                Optional.empty());
        assertThat(MongoAggregationTranslator.translate(BASE_TABLE_HANDLE, ImmutableList.of(sortAgg), assignments, ImmutableList.of()))
                .isEmpty();
    }

    @Test
    public void testRejectNonNumericSumAvg()
    {
        List<Type> nonNumericTypes = ImmutableList.of(
                VARCHAR,
                BOOLEAN,
                RowType.anonymousRow(BIGINT, VARCHAR),
                new ArrayType(BIGINT));

        for (String functionName : ImmutableList.of("sum", "avg")) {
            for (Type type : nonNumericTypes) {
                MongoColumnHandle column = createColumn("col", type);
                Map<String, ColumnHandle> assignments = ImmutableMap.of("col_var", column);
                AggregateFunction aggregate = new AggregateFunction(
                        functionName,
                        DOUBLE,
                        ImmutableList.of(new Variable("col_var", type)),
                        ImmutableList.of(),
                        false,
                        Optional.empty());

                assertThat(MongoAggregationTranslator.translate(BASE_TABLE_HANDLE, ImmutableList.of(aggregate), assignments, ImmutableList.of()))
                        .as(functionName + "(" + type + ") should be rejected")
                        .isEmpty();
            }
        }
    }

    @Test
    public void testRejectNonOrderableOrComplexMinMax()
    {
        Type mapType = new MapType(BIGINT, BIGINT, TESTING_TYPE_MANAGER.getTypeOperators());
        Type jsonType = TESTING_TYPE_MANAGER.getType(TypeId.of(JSON));
        List<Type> complexOrUnsupportedTypes = ImmutableList.of(
                RowType.anonymousRow(BIGINT, VARCHAR),
                new ArrayType(BIGINT),
                mapType,
                jsonType);

        for (String functionName : ImmutableList.of("min", "max")) {
            for (Type type : complexOrUnsupportedTypes) {
                MongoColumnHandle column = createColumn("col", type);
                Map<String, ColumnHandle> assignments = ImmutableMap.of("col_var", column);
                AggregateFunction aggregate = new AggregateFunction(
                        functionName,
                        type,
                        ImmutableList.of(new Variable("col_var", type)),
                        ImmutableList.of(),
                        false,
                        Optional.empty());

                assertThat(MongoAggregationTranslator.translate(BASE_TABLE_HANDLE, ImmutableList.of(aggregate), assignments, ImmutableList.of()))
                        .as(functionName + "(" + type + ") should be rejected")
                        .isEmpty();
            }
        }
    }

    @Test
    public void testRejectNonVariableArgument()
    {
        // Call expression as argument
        Call callExpr = new Call(BIGINT, new FunctionName("abs"), ImmutableList.of(new Variable("v", BIGINT)));
        AggregateFunction callAgg = new AggregateFunction(
                "sum",
                BIGINT,
                ImmutableList.of(callExpr),
                ImmutableList.of(),
                false,
                Optional.empty());
        assertThat(MongoAggregationTranslator.translate(BASE_TABLE_HANDLE, ImmutableList.of(callAgg), ImmutableMap.of(), ImmutableList.of()))
                .isEmpty();

        // Constant expression as argument
        Constant constExpr = new Constant(10L, BIGINT);
        AggregateFunction constAgg = new AggregateFunction(
                "sum",
                BIGINT,
                ImmutableList.of(constExpr),
                ImmutableList.of(),
                false,
                Optional.empty());
        assertThat(MongoAggregationTranslator.translate(BASE_TABLE_HANDLE, ImmutableList.of(constAgg), ImmutableMap.of(), ImmutableList.of()))
                .isEmpty();
    }

    @Test
    public void testRejectHiddenColumn()
    {
        MongoColumnHandle hiddenCol = createHiddenColumn("hidden_col", BIGINT);
        Map<String, ColumnHandle> assignments = ImmutableMap.of("h_var", hiddenCol);

        AggregateFunction agg = new AggregateFunction(
                "sum",
                BIGINT,
                ImmutableList.of(new Variable("h_var", BIGINT)),
                ImmutableList.of(),
                false,
                Optional.empty());

        assertThat(MongoAggregationTranslator.translate(BASE_TABLE_HANDLE, ImmutableList.of(agg), assignments, ImmutableList.of()))
                .isEmpty();
    }

    @Test
    public void testRejectUnsupportedFunctionName()
    {
        MongoColumnHandle column = createColumn("val", BIGINT);
        Map<String, ColumnHandle> assignments = ImmutableMap.of("v", column);

        for (String unsupported : ImmutableList.of("approx_distinct", "stddev", "covar_pop", "arbitrary")) {
            AggregateFunction agg = new AggregateFunction(
                    unsupported,
                    BIGINT,
                    ImmutableList.of(new Variable("v", BIGINT)),
                    ImmutableList.of(),
                    false,
                    Optional.empty());
            assertThat(MongoAggregationTranslator.translate(BASE_TABLE_HANDLE, ImmutableList.of(agg), assignments, ImmutableList.of()))
                    .as(unsupported + " should be rejected")
                    .isEmpty();
        }
    }

    @Test
    public void testSyntheticColumnNamingAndCollisionResolution()
    {
        MongoColumnHandle collidingCol = createColumn("_mfgnrtd_0", BIGINT);
        Map<String, ColumnHandle> assignments = ImmutableMap.of("_mfgnrtd_0", collidingCol);

        AggregateFunction countStar = new AggregateFunction("count", BIGINT, ImmutableList.of(), ImmutableList.of(), false, Optional.empty());

        Optional<AggregationApplicationResult<ConnectorTableHandle>> result =
                MongoAggregationTranslator.translate(BASE_TABLE_HANDLE, ImmutableList.of(countStar), assignments, ImmutableList.of());

        assertThat(result).isPresent();
        MongoTableHandle tableHandle = (MongoTableHandle) result.get().getHandle();
        MongoAccumulator accumulator = tableHandle.aggregation().orElseThrow().accumulators().get(0);

        // Prepend underscore to resolve collision
        assertThat(accumulator.projectedColumn().baseName()).isEqualTo("__mfgnrtd_0");
    }
}
