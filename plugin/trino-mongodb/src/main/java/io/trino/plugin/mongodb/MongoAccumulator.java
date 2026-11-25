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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public record MongoAccumulator(
        MongoColumnHandle resultHandle,
        String functionName,
        List<MongoColumnHandle> arguments,
        boolean distinct)
{
    public MongoAccumulator
    {
        requireNonNull(resultHandle, "resultHandle is null");
        requireNonNull(functionName, "functionName is null");
        arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
    }

    @JsonIgnore
    public Type outputType()
    {
        return resultHandle.type();
    }

    @JsonIgnore
    public boolean isCountAll()
    {
        return arguments.isEmpty() && functionName.equalsIgnoreCase("count");
    }

    @Override
    public String toString()
    {
        String args = arguments.stream().map(MongoColumnHandle::getQualifiedName).collect(joining(", "));
        if (distinct) {
            args = "DISTINCT " + args;
        }
        else if (args.isEmpty() && functionName.equalsIgnoreCase("count")) {
            args = "*";
        }
        return resultHandle.getQualifiedName() + " := " + functionName + "(" + args + ")";
    }
}
