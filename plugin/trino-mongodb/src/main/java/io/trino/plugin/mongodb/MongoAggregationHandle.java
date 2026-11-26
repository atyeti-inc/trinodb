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

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public record MongoAggregationHandle(
        List<MongoColumnHandle> groupingKeys,
        List<MongoAccumulator> accumulators)
{
    public MongoAggregationHandle
    {
        groupingKeys = ImmutableList.copyOf(requireNonNull(groupingKeys, "groupingKeys is null"));
        accumulators = ImmutableList.copyOf(requireNonNull(accumulators, "accumulators is null"));
    }

    @Override
    public String toString()
    {
        return "[groupBy=" + groupingKeys.stream().map(MongoColumnHandle::getQualifiedName).collect(joining(", ", "[", "]")) + ", accumulators=" + accumulators + "]";
    }
}
