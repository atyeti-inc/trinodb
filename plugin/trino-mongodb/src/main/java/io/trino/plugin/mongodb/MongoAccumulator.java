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

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public record MongoAccumulator(
        @JsonProperty("outputColumnHandle") MongoColumnHandle outputColumnHandle,
        @JsonProperty("functionName") String functionName,
        @JsonProperty("arguments") List<MongoColumnHandle> arguments,
        @JsonProperty("distinct") boolean distinct)
{
    @JsonCreator
    public MongoAccumulator
    {
        requireNonNull(outputColumnHandle, "outputColumnHandle is null");
        requireNonNull(functionName, "functionName is null");
        arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
    }

    public MongoAccumulator(MongoColumnHandle outputColumnHandle, String functionName, List<MongoColumnHandle> arguments)
    {
        this(outputColumnHandle, functionName, arguments, false);
    }

    public MongoColumnHandle resultColumnHandle()
    {
        return outputColumnHandle;
    }

    public boolean isCountAll()
    {
        return "count".equalsIgnoreCase(functionName) && arguments.isEmpty();
    }

    @Override
    public String toString()
    {
        String args = arguments.isEmpty() ? "*" : arguments.stream().map(MongoColumnHandle::getQualifiedName).collect(joining(", "));
        return outputColumnHandle.getQualifiedName() + " := " + functionName + "(" + (distinct ? "DISTINCT " : "") + args + ")";
    }
}
