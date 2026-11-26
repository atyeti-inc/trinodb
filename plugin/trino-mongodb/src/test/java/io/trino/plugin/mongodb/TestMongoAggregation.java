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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMongoAggregation
{
    private static final MongoColumnHandle COL_DEPT = new MongoColumnHandle("dept_id", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
    private static final MongoColumnHandle COL_STATUS = new MongoColumnHandle("status", ImmutableList.of(), VARCHAR, false, false, Optional.empty());
    private static final MongoColumnHandle COL_SALARY = new MongoColumnHandle("salary", ImmutableList.of(), DOUBLE, false, false, Optional.empty());
    private static final MongoColumnHandle COL_COUNT = new MongoColumnHandle("_col0", ImmutableList.of(), BIGINT, false, false, Optional.empty());
    private static final MongoColumnHandle COL_SUM = new MongoColumnHandle("total", ImmutableList.of(), DOUBLE, false, false, Optional.empty());

    @Test
    public void testImmutability()
    {
        List<MongoColumnHandle> mutableArguments = new ArrayList<>();
        mutableArguments.add(COL_SALARY);
        MongoAccumulator accumulator = new MongoAccumulator("sum", mutableArguments, COL_SUM);

        mutableArguments.clear();
        assertThat(accumulator.arguments()).containsExactly(COL_SALARY);
        assertThatThrownBy(() -> accumulator.arguments().add(COL_SALARY))
                .isInstanceOf(UnsupportedOperationException.class);

        List<MongoColumnHandle> mutableGroupingKeys = new ArrayList<>();
        mutableGroupingKeys.add(COL_DEPT);
        List<MongoAccumulator> mutableAccumulators = new ArrayList<>();
        mutableAccumulators.add(accumulator);

        MongoAggregation aggregation = new MongoAggregation(mutableGroupingKeys, mutableAccumulators);
        mutableGroupingKeys.clear();
        mutableAccumulators.clear();

        assertThat(aggregation.groupingKeys()).containsExactly(COL_DEPT);
        assertThat(aggregation.aggregations()).containsExactly(accumulator);

        assertThatThrownBy(() -> aggregation.groupingKeys().add(COL_STATUS))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> aggregation.aggregations().add(accumulator))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testNullChecks()
    {
        assertThatThrownBy(() -> new MongoAccumulator(null, ImmutableList.of(), COL_COUNT))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("functionName is null");
        assertThatThrownBy(() -> new MongoAccumulator("count", null, COL_COUNT))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("arguments is null");
        assertThatThrownBy(() -> new MongoAccumulator("count", ImmutableList.of(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("resultColumn is null");

        assertThatThrownBy(() -> new MongoAggregation(null, ImmutableList.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("groupingKeys is null");
        assertThatThrownBy(() -> new MongoAggregation(ImmutableList.of(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("aggregations is null");
    }

    @Test
    public void testEqualsAndHashCode()
    {
        MongoAccumulator acc1 = new MongoAccumulator("sum", ImmutableList.of(COL_SALARY), COL_SUM);
        MongoAccumulator acc2 = new MongoAccumulator("sum", ImmutableList.of(COL_SALARY), COL_SUM);
        MongoAccumulator acc3 = new MongoAccumulator("count", ImmutableList.of(), COL_COUNT);

        assertThat(acc1).isEqualTo(acc2);
        assertThat(acc1.hashCode()).isEqualTo(acc2.hashCode());
        assertThat(acc1).isNotEqualTo(acc3);

        MongoAggregation agg1 = new MongoAggregation(ImmutableList.of(COL_DEPT), ImmutableList.of(acc1));
        MongoAggregation agg2 = new MongoAggregation(ImmutableList.of(COL_DEPT), ImmutableList.of(acc2));
        MongoAggregation agg3 = new MongoAggregation(ImmutableList.of(COL_DEPT, COL_STATUS), ImmutableList.of(acc1));
        MongoAggregation agg4 = new MongoAggregation(ImmutableList.of(), ImmutableList.of(acc3));

        assertThat(agg1).isEqualTo(agg2);
        assertThat(agg1.hashCode()).isEqualTo(agg2.hashCode());
        assertThat(agg1).isNotEqualTo(agg3);
        assertThat(agg1).isNotEqualTo(agg4);
    }

    @Test
    public void testToString()
    {
        MongoAccumulator countAcc = new MongoAccumulator("count", ImmutableList.of(), COL_COUNT);
        assertThat(countAcc.toString()).isEqualTo("_col0 := count(*)");

        MongoAccumulator sumAcc = new MongoAccumulator("sum", ImmutableList.of(COL_SALARY), COL_SUM);
        assertThat(sumAcc.toString()).isEqualTo("total := sum(salary)");

        MongoAggregation globalAgg = new MongoAggregation(ImmutableList.of(), ImmutableList.of(countAcc));
        assertThat(globalAgg.toString()).isEqualTo("global, aggregations=[_col0 := count(*)]");

        MongoAggregation groupedAgg = new MongoAggregation(ImmutableList.of(COL_DEPT, COL_STATUS), ImmutableList.of(sumAcc, countAcc));
        assertThat(groupedAgg.toString()).isEqualTo("groupBy=[dept_id, status], aggregations=[total := sum(salary), _col0 := count(*)]");
    }
}
