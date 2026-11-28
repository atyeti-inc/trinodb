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
    public void testAccumulatorRoundTrip()
    {
        MongoColumnHandle projected = new MongoColumnHandle("count_val", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoColumnHandle arg = new MongoColumnHandle("col1", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoAccumulator expected = new MongoAccumulator(projected, "count", ImmutableList.of(arg), true);

        String json = accumulatorCodec.toJson(expected);
        MongoAccumulator actual = accumulatorCodec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.hashCode()).isEqualTo(expected.hashCode());
    }

    @Test
    public void testAccumulatorToString()
    {
        MongoColumnHandle countCol = new MongoColumnHandle("cnt", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator countStar = new MongoAccumulator(countCol, "count", ImmutableList.of(), false);
        assertThat(countStar.toString()).isEqualTo("cnt := count(*)");

        MongoColumnHandle sumCol = new MongoColumnHandle("total", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoColumnHandle argCol = new MongoColumnHandle("amount", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator sum = new MongoAccumulator(sumCol, "sum", ImmutableList.of(argCol), false);
        assertThat(sum.toString()).isEqualTo("total := sum(amount)");

        MongoColumnHandle distinctCountCol = new MongoColumnHandle("unique_users", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoColumnHandle userCol = new MongoColumnHandle("user_id", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoAccumulator distinctCount = new MongoAccumulator(distinctCountCol, "count", ImmutableList.of(userCol), true);
        assertThat(distinctCount.toString()).isEqualTo("unique_users := count(DISTINCT user_id)");
    }

    @Test
    public void testAccumulatorImmutability()
    {
        MongoColumnHandle projected = new MongoColumnHandle("cnt", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoColumnHandle arg = new MongoColumnHandle("col", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        List<MongoColumnHandle> args = new ArrayList<>();
        args.add(arg);

        MongoAccumulator accumulator = new MongoAccumulator(projected, "count", args, false);
        args.clear();
        assertThat(accumulator.arguments()).hasSize(1);

        assertThatThrownBy(() -> accumulator.arguments().add(arg))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testAggregationRoundTrip()
    {
        MongoColumnHandle groupCol = new MongoColumnHandle("region", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle countCol = new MongoColumnHandle("cnt", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator countStar = new MongoAccumulator(countCol, "count", ImmutableList.of(), false);

        MongoAggregation expected = MongoAggregation.singleGroupingSet(ImmutableList.of(groupCol), ImmutableList.of(countStar));

        String json = aggregationCodec.toJson(expected);
        MongoAggregation actual = aggregationCodec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.hashCode()).isEqualTo(expected.hashCode());
    }

    @Test
    public void testAggregationToString()
    {
        MongoColumnHandle groupCol = new MongoColumnHandle("region", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle countCol = new MongoColumnHandle("cnt", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator countStar = new MongoAccumulator(countCol, "count", ImmutableList.of(), false);

        MongoAggregation aggregation = MongoAggregation.singleGroupingSet(ImmutableList.of(groupCol), ImmutableList.of(countStar));
        assertThat(aggregation.toString())
                .isEqualTo("groupingSets=[[region]] accumulators=[cnt := count(*)]");

        MongoAggregation globalAggregation = new MongoAggregation(
                ImmutableList.of(ImmutableList.of()),
                ImmutableList.of(countStar));
        assertThat(globalAggregation.toString())
                .isEqualTo("groupingSets=[[]] accumulators=[cnt := count(*)]");
    }

    @Test
    public void testAggregationImmutability()
    {
        MongoColumnHandle groupCol = new MongoColumnHandle("region", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
        MongoColumnHandle countCol = new MongoColumnHandle("cnt", ImmutableList.of(), BIGINT, false, false, Optional.empty());
        MongoAccumulator countStar = new MongoAccumulator(countCol, "count", ImmutableList.of(), false);

        List<MongoColumnHandle> groupingSet = new ArrayList<>();
        groupingSet.add(groupCol);
        List<List<MongoColumnHandle>> groupingSets = new ArrayList<>();
        groupingSets.add(groupingSet);
        List<MongoAccumulator> accumulators = new ArrayList<>();
        accumulators.add(countStar);

        MongoAggregation aggregation = new MongoAggregation(groupingSets, accumulators);

        groupingSet.clear();
        groupingSets.clear();
        accumulators.clear();

        assertThat(aggregation.groupingSets()).hasSize(1);
        assertThat(aggregation.groupingSets().getFirst()).hasSize(1);
        assertThat(aggregation.accumulators()).hasSize(1);

        assertThatThrownBy(() -> aggregation.groupingSets().add(ImmutableList.of()))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> aggregation.groupingSets().getFirst().add(groupCol))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> aggregation.accumulators().add(countStar))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
