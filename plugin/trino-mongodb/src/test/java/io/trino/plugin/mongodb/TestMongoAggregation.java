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
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.spi.type.Type;
import io.trino.type.TypeDeserializer;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMongoAggregation
{
    private final JsonCodec<MongoAccumulator> accumulatorCodec;
    private final JsonCodec<MongoAggregation> aggregationCodec;

    public TestMongoAggregation()
    {
        JsonMapper jsonMapper = new JsonMapperProvider()
                .withJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)))
                .get();
        JsonCodecFactory codecFactory = new JsonCodecFactory(jsonMapper);
        accumulatorCodec = codecFactory.jsonCodec(MongoAccumulator.class);
        aggregationCodec = codecFactory.jsonCodec(MongoAggregation.class);
    }

    @Test
    public void testImmutability()
    {
        MongoColumnHandle column = new MongoColumnHandle("col", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle resultColumn = new MongoColumnHandle("res", ImmutableList.of(), BIGINT, false, false, Optional.empty());

        List<MongoColumnHandle> arguments = new ArrayList<>();
        arguments.add(column);
        MongoAccumulator accumulator = new MongoAccumulator(resultColumn, "count", arguments);

        arguments.clear();
        assertThat(accumulator.arguments()).containsExactly(column);
        assertThatThrownBy(() -> accumulator.arguments().add(column))
                .isInstanceOf(UnsupportedOperationException.class);

        List<MongoColumnHandle> groupByColumns = new ArrayList<>();
        groupByColumns.add(column);
        List<MongoAccumulator> accumulators = new ArrayList<>();
        accumulators.add(accumulator);

        MongoAggregation aggregation = new MongoAggregation(groupByColumns, accumulators);
        groupByColumns.clear();
        accumulators.clear();
        assertThat(aggregation.groupByColumns()).containsExactly(column);
        assertThat(aggregation.accumulators()).containsExactly(accumulator);

        assertThatThrownBy(() -> aggregation.groupByColumns().add(column))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> aggregation.accumulators().add(accumulator))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testConstructorValidation()
    {
        assertThatThrownBy(() -> new MongoAggregation(ImmutableList.of(), ImmutableList.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("both groupByColumns and accumulators are empty");

        assertThatThrownBy(() -> new MongoAggregation(null, ImmutableList.of()))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> new MongoAggregation(ImmutableList.of(), null))
                .isInstanceOf(NullPointerException.class);

        MongoColumnHandle resultColumn = new MongoColumnHandle("res", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        assertThatThrownBy(() -> new MongoAccumulator(null, "count", ImmutableList.of()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new MongoAccumulator(resultColumn, null, ImmutableList.of()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new MongoAccumulator(resultColumn, "count", null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testIsGlobalAggregation()
    {
        MongoColumnHandle column = new MongoColumnHandle("col", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle resultColumn = new MongoColumnHandle("res", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator accumulator = new MongoAccumulator(resultColumn, "count", ImmutableList.of());

        MongoAggregation globalAggregation = new MongoAggregation(ImmutableList.of(), ImmutableList.of(accumulator));
        assertThat(globalAggregation.isGlobalAggregation()).isTrue();

        MongoAggregation groupByAndAgg = new MongoAggregation(ImmutableList.of(column), ImmutableList.of(accumulator));
        assertThat(groupByAndAgg.isGlobalAggregation()).isFalse();

        MongoAggregation groupByOnly = new MongoAggregation(ImmutableList.of(column), ImmutableList.of());
        assertThat(groupByOnly.isGlobalAggregation()).isFalse();
    }

    @Test
    public void testToString()
    {
        MongoColumnHandle colK1 = new MongoColumnHandle("k1", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle colK2 = new MongoColumnHandle("k2", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle res0 = new MongoColumnHandle("_col0", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoColumnHandle res1 = new MongoColumnHandle("_col1", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoColumnHandle colV = new MongoColumnHandle("v", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoColumnHandle team = new MongoColumnHandle("team", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle res2 = new MongoColumnHandle("_col2", ImmutableList.of(), BIGINT, false, false, Optional.empty());

        MongoAccumulator countStar = new MongoAccumulator(res0, "count", ImmutableList.of());
        assertThat(countStar.toString()).isEqualTo("count(*) AS _col0");

        MongoAccumulator sumV = new MongoAccumulator(res1, "sum", ImmutableList.of(colV));
        assertThat(sumV.toString()).isEqualTo("sum(v) AS _col1");

        MongoAccumulator countDistinctTeam = new MongoAccumulator(res2, "count", ImmutableList.of(team), true);
        assertThat(countDistinctTeam.toString()).isEqualTo("count(DISTINCT team) AS _col2");

        MongoAggregation groupByOnly = new MongoAggregation(ImmutableList.of(colK1, colK2), ImmutableList.of());
        assertThat(groupByOnly.toString()).isEqualTo("groupBy=[k1, k2]");

        MongoAggregation aggregatesOnly = new MongoAggregation(ImmutableList.of(), ImmutableList.of(countStar));
        assertThat(aggregatesOnly.toString()).isEqualTo("aggregates=[count(*) AS _col0]");

        MongoAggregation both = new MongoAggregation(ImmutableList.of(colK1), ImmutableList.of(countStar, sumV));
        assertThat(both.toString()).isEqualTo("groupBy=[k1], aggregates=[count(*) AS _col0, sum(v) AS _col1]");
    }

    @Test
    public void testRoundTripSerialization()
    {
        MongoColumnHandle col = new MongoColumnHandle("dept", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle res = new MongoColumnHandle("_col0", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator accumulator = new MongoAccumulator(res, "count", ImmutableList.of(), false);

        String accumulatorJson = accumulatorCodec.toJson(accumulator);
        MongoAccumulator deserializedAccumulator = accumulatorCodec.fromJson(accumulatorJson);
        assertThat(deserializedAccumulator).isEqualTo(accumulator);

        MongoAggregation aggregation = new MongoAggregation(ImmutableList.of(col), ImmutableList.of(accumulator));
        String aggregationJson = aggregationCodec.toJson(aggregation);
        MongoAggregation deserializedAggregation = aggregationCodec.fromJson(aggregationJson);
        assertThat(deserializedAggregation).isEqualTo(aggregation);
    }
}
