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

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public record MongoAggregation(
        List<List<MongoColumnHandle>> groupingSets,
        List<MongoAccumulator> accumulators)
{
    public MongoAggregation
    {
        requireNonNull(groupingSets, "groupingSets is null");
        groupingSets = groupingSets.stream()
                .map(groupingSet -> (List<MongoColumnHandle>) ImmutableList.copyOf(requireNonNull(groupingSet, "groupingSet is null")))
                .collect(toImmutableList());
        accumulators = ImmutableList.copyOf(requireNonNull(accumulators, "accumulators is null"));
    }

    public static MongoAggregation singleGroupingSet(List<MongoColumnHandle> groupBy, List<MongoAccumulator> accumulators)
    {
        return new MongoAggregation(ImmutableList.of(ImmutableList.copyOf(requireNonNull(groupBy, "groupBy is null"))), accumulators);
    }

    @Override
    public String toString()
    {
        return "groupingSets=" + groupingSets.stream()
                .map(set -> set.stream().map(MongoColumnHandle::getQualifiedName).collect(joining(", ", "[", "]")))
                .collect(joining(", ", "[", "]")) +
                " accumulators=" + accumulators;
    }
}
