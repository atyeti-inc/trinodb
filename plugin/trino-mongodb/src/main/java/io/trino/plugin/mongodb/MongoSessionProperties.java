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
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.plugin.base.session.PropertyMetadataUtil.validateMinDataSize;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static java.lang.String.format;

public final class MongoSessionProperties
        implements SessionPropertiesProvider
{
    private static final String PROJECTION_PUSHDOWN_ENABLED = "projection_pushdown_enabled";
    public static final String DYNAMIC_FILTERING_WAIT_TIMEOUT = "dynamic_filtering_wait_timeout";
    public static final String PARALLEL_SPLIT_ENABLED = "parallel_split_enabled";
    public static final String SPLIT_SIZE = "split_size";
    public static final String MIN_RECORDS_PER_SPLIT = "min_records_per_split";
    public static final String SPLIT_STRATEGY = "split_strategy";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public MongoSessionProperties(MongoClientConfig mongoConfig)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(booleanProperty(
                        PROJECTION_PUSHDOWN_ENABLED,
                        "Read only required fields from a row type",
                        mongoConfig.isProjectionPushdownEnabled(),
                        false))
                .add(durationProperty(
                        DYNAMIC_FILTERING_WAIT_TIMEOUT,
                        "Duration to wait for completion of dynamic filters",
                        mongoConfig.getDynamicFilteringWaitTimeout(),
                        false))
                .add(booleanProperty(
                        PARALLEL_SPLIT_ENABLED,
                        "Enable parallel split generation for collection scans",
                        mongoConfig.isParallelSplitEnabled(),
                        false))
                .add(dataSizeProperty(
                        SPLIT_SIZE,
                        "Target data volume per split for parallel collection scans",
                        mongoConfig.getSplitSize(),
                        value -> validateMinDataSize(SPLIT_SIZE, value, DataSize.ofBytes(1)),
                        false))
                .add(longProperty(
                        MIN_RECORDS_PER_SPLIT,
                        "Minimum records per split required for parallel split generation",
                        mongoConfig.getMinRecordsPerSplit(),
                        value -> {
                            if (value < 0) {
                                throw new TrinoException(INVALID_SESSION_PROPERTY, format("%s must be greater than or equal to 0: %s", MIN_RECORDS_PER_SPLIT, value));
                            }
                        },
                        false))
                .add(enumProperty(
                        SPLIT_STRATEGY,
                        "Partitioning strategy for parallel split generation",
                        MongoSplitStrategy.class,
                        mongoConfig.getSplitStrategy(),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isProjectionPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(PROJECTION_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static Duration getDynamicFilteringWaitTimeout(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_FILTERING_WAIT_TIMEOUT, Duration.class);
    }

    public static boolean isParallelSplitEnabled(ConnectorSession session)
    {
        return session.getProperty(PARALLEL_SPLIT_ENABLED, Boolean.class);
    }

    public static DataSize getSplitSize(ConnectorSession session)
    {
        return session.getProperty(SPLIT_SIZE, DataSize.class);
    }

    public static long getMinRecordsPerSplit(ConnectorSession session)
    {
        return session.getProperty(MIN_RECORDS_PER_SPLIT, Long.class);
    }

    public static MongoSplitStrategy getSplitStrategy(ConnectorSession session)
    {
        return session.getProperty(SPLIT_STRATEGY, MongoSplitStrategy.class);
    }
}
