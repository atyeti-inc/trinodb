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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public record MongoAggregation(
        @JsonProperty("groupByColumns") List<MongoColumnHandle> groupByColumns,
        @JsonProperty("accumulators") List<MongoAccumulator> accumulators)
{
    @JsonCreator
    public MongoAggregation
    {
        groupByColumns = ImmutableList.copyOf(requireNonNull(groupByColumns, "groupByColumns is null"));
        accumulators = ImmutableList.copyOf(requireNonNull(accumulators, "accumulators is null"));
        checkArgument(!groupByColumns.isEmpty() || !accumulators.isEmpty(), "both groupByColumns and accumulators are empty");
    }

    public boolean isGlobalAggregation()
    {
        return groupByColumns.isEmpty() && !accumulators.isEmpty();
    }

    @Override
    public String toString()
    {
        ImmutableList.Builder<String> parts = ImmutableList.builder();
        if (!groupByColumns.isEmpty()) {
            parts.add("groupBy=[" + groupByColumns.stream().map(MongoColumnHandle::baseName).collect(joining(", ")) + "]");
        }
        if (!accumulators.isEmpty()) {
            parts.add("aggregates=[" + accumulators.stream().map(MongoAccumulator::toString).collect(joining(", ")) + "]");
        }
        return String.join(", ", parts.build());
    }
}
