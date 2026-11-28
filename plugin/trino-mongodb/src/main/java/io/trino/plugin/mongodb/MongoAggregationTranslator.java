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
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.mongodb.TypeUtils.isNumericType;
import static io.trino.plugin.mongodb.TypeUtils.isPushdownSupportedType;

final class MongoAggregationTranslator
{
    static final String SYNTHETIC_COLUMN_NAME_PREFIX = "_mfgnrtd_";

    private MongoAggregationTranslator() {}

    public static Optional<AggregationApplicationResult<ConnectorTableHandle>> translate(
            MongoTableHandle tableHandle,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<MongoColumnHandle> groupingColumns)
    {
        Set<String> takenNames = new HashSet<>();
        for (ColumnHandle handle : assignments.values()) {
            if (handle instanceof MongoColumnHandle mongoColumn) {
                takenNames.add(mongoColumn.baseName());
            }
        }
        for (MongoColumnHandle mongoColumn : groupingColumns) {
            takenNames.add(mongoColumn.baseName());
        }

        ImmutableList.Builder<ConnectorExpression> projections = ImmutableList.builder();
        ImmutableList.Builder<Assignment> resultAssignments = ImmutableList.builder();
        ImmutableList.Builder<MongoAccumulator> accumulators = ImmutableList.builder();
        ImmutableList.Builder<MongoColumnHandle> syntheticColumns = ImmutableList.builder();

        for (int i = 0; i < aggregates.size(); i++) {
            AggregateFunction aggregate = aggregates.get(i);
            if (aggregate.isDistinct() || aggregate.getFilter().isPresent() || !aggregate.getSortItems().isEmpty()) {
                return Optional.empty();
            }

            String colName = SYNTHETIC_COLUMN_NAME_PREFIX + i;
            while (takenNames.contains(colName) || assignments.containsKey(colName)) {
                colName = "_" + colName;
            }
            takenNames.add(colName);

            MongoColumnHandle syntheticColumn = new MongoColumnHandle(
                    colName,
                    ImmutableList.of(),
                    aggregate.getOutputType(),
                    false,
                    false,
                    Optional.of("synthetic"));

            String functionName = aggregate.getFunctionName().toLowerCase(Locale.ENGLISH);
            List<ConnectorExpression> arguments = aggregate.getArguments();

            MongoAccumulator accumulator;
            switch (functionName) {
                case "count" -> {
                    if (arguments.isEmpty()) {
                        accumulator = new MongoAccumulator(syntheticColumn, "count", ImmutableList.of(), false);
                    }
                    else if (arguments.size() == 1) {
                        Optional<MongoColumnHandle> argumentColumn = resolveArgumentColumn(arguments.get(0), assignments);
                        if (argumentColumn.isEmpty()) {
                            return Optional.empty();
                        }
                        accumulator = new MongoAccumulator(syntheticColumn, "count", ImmutableList.of(argumentColumn.get()), false);
                    }
                    else {
                        return Optional.empty();
                    }
                }
                case "sum", "avg" -> {
                    if (arguments.size() != 1) {
                        return Optional.empty();
                    }
                    Optional<MongoColumnHandle> argumentColumn = resolveArgumentColumn(arguments.get(0), assignments);
                    if (argumentColumn.isEmpty() || !isNumericType(argumentColumn.get().type())) {
                        return Optional.empty();
                    }
                    accumulator = new MongoAccumulator(syntheticColumn, functionName, ImmutableList.of(argumentColumn.get()), false);
                }
                case "min", "max" -> {
                    if (arguments.size() != 1) {
                        return Optional.empty();
                    }
                    Optional<MongoColumnHandle> argumentColumn = resolveArgumentColumn(arguments.get(0), assignments);
                    if (argumentColumn.isEmpty()) {
                        return Optional.empty();
                    }
                    Type type = argumentColumn.get().type();
                    if (!isPushdownSupportedType(type) || !type.isOrderable()) {
                        return Optional.empty();
                    }
                    accumulator = new MongoAccumulator(syntheticColumn, functionName, ImmutableList.of(argumentColumn.get()), false);
                }
                default -> {
                    return Optional.empty();
                }
            }

            accumulators.add(accumulator);
            syntheticColumns.add(syntheticColumn);
            projections.add(new Variable(colName, aggregate.getOutputType()));
            resultAssignments.add(new Assignment(colName, syntheticColumn, aggregate.getOutputType()));
        }

        MongoAggregation aggregation = new MongoAggregation(
                ImmutableList.of(ImmutableList.copyOf(groupingColumns)),
                accumulators.build());

        ImmutableSet.Builder<MongoColumnHandle> projectedColumns = ImmutableSet.builder();
        projectedColumns.addAll(groupingColumns);
        projectedColumns.addAll(syntheticColumns.build());

        MongoTableHandle newTableHandle = tableHandle
                .withProjectedColumns(projectedColumns.build())
                .withAggregation(aggregation);

        return Optional.of(new AggregationApplicationResult<>(
                newTableHandle,
                projections.build(),
                resultAssignments.build(),
                ImmutableMap.of(),
                false));
    }

    private static Optional<MongoColumnHandle> resolveArgumentColumn(
            ConnectorExpression argument,
            Map<String, ColumnHandle> assignments)
    {
        if (!(argument instanceof Variable variable)) {
            return Optional.empty();
        }
        ColumnHandle columnHandle = assignments.get(variable.getName());
        if (!(columnHandle instanceof MongoColumnHandle mongoColumn) || mongoColumn.hidden()) {
            return Optional.empty();
        }
        return Optional.of(mongoColumn);
    }
}
