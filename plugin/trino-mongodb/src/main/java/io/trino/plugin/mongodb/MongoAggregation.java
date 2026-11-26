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

public record MongoAggregation(
        List<MongoColumnHandle> groupingKeys,
        List<MongoAccumulator> aggregations)
{
    public MongoAggregation
    {
        groupingKeys = ImmutableList.copyOf(requireNonNull(groupingKeys, "groupingKeys is null"));
        aggregations = ImmutableList.copyOf(requireNonNull(aggregations, "aggregations is null"));
    }

    @Override
    public String toString()
    {
        String groupDescription = groupingKeys.isEmpty()
                ? "global"
                : "groupBy=" + groupingKeys.stream().map(MongoColumnHandle::getQualifiedName).collect(joining(", ", "[", "]"));
        return groupDescription + ", aggregations=" + aggregations;
    }
}
