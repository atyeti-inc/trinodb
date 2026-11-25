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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.mongodb.MongoSessionProperties.MIN_RECORDS_PER_SPLIT;
import static io.trino.plugin.mongodb.MongoSessionProperties.PARALLEL_SPLIT_ENABLED;
import static io.trino.plugin.mongodb.MongoSessionProperties.SPLIT_SIZE;
import static io.trino.plugin.mongodb.MongoSessionProperties.SPLIT_STRATEGY;
import static io.trino.plugin.mongodb.MongoSessionProperties.getMinRecordsPerSplit;
import static io.trino.plugin.mongodb.MongoSessionProperties.getSplitSize;
import static io.trino.plugin.mongodb.MongoSessionProperties.getSplitStrategy;
import static io.trino.plugin.mongodb.MongoSessionProperties.isParallelSplitEnabled;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMongoSessionProperties
{
    @Test
    public void testDefaultSessionPropertiesMatchConfig()
    {
        MongoClientConfig defaultConfig = new MongoClientConfig();
        MongoSessionProperties sessionProperties = new MongoSessionProperties(defaultConfig);
        ConnectorSession defaultSession = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .build();

        assertThat(isParallelSplitEnabled(defaultSession)).isTrue();
        assertThat(getSplitSize(defaultSession)).isEqualTo(DataSize.of(64, MEGABYTE));
        assertThat(getMinRecordsPerSplit(defaultSession)).isEqualTo(0L);
        assertThat(getSplitStrategy(defaultSession)).isEqualTo(MongoSplitStrategy.AUTOMATIC);

        MongoClientConfig customConfig = new MongoClientConfig()
                .setParallelSplitEnabled(false)
                .setSplitSize(DataSize.of(128, MEGABYTE))
                .setMinRecordsPerSplit(10_000L)
                .setSplitStrategy(MongoSplitStrategy.CHUNK);
        MongoSessionProperties customSessionProperties = new MongoSessionProperties(customConfig);
        ConnectorSession customSession = TestingConnectorSession.builder()
                .setPropertyMetadata(customSessionProperties.getSessionProperties())
                .build();

        assertThat(isParallelSplitEnabled(customSession)).isFalse();
        assertThat(getSplitSize(customSession)).isEqualTo(DataSize.of(128, MEGABYTE));
        assertThat(getMinRecordsPerSplit(customSession)).isEqualTo(10_000L);
        assertThat(getSplitStrategy(customSession)).isEqualTo(MongoSplitStrategy.CHUNK);
    }

    @Test
    public void testSessionPropertyOverrides()
    {
        MongoSessionProperties sessionProperties = new MongoSessionProperties(new MongoClientConfig());
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        PARALLEL_SPLIT_ENABLED, false,
                        SPLIT_SIZE, "32MB",
                        MIN_RECORDS_PER_SPLIT, 500L,
                        SPLIT_STRATEGY, "RANGE"))
                .build();

        assertThat(isParallelSplitEnabled(session)).isFalse();
        assertThat(getSplitSize(session)).isEqualTo(DataSize.of(32, MEGABYTE));
        assertThat(getMinRecordsPerSplit(session)).isEqualTo(500L);
        assertThat(getSplitStrategy(session)).isEqualTo(MongoSplitStrategy.RANGE);
    }

    @Test
    public void testInvalidSplitSize()
    {
        MongoSessionProperties sessionProperties = new MongoSessionProperties(new MongoClientConfig());
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .setPropertyValues(ImmutableMap.of(SPLIT_SIZE, "0B"))
                .build();

        assertThatThrownBy(() -> getSplitSize(session))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("split_size must be at least 1B")
                .matches(throwable -> ((TrinoException) throwable).getErrorCode().equals(INVALID_SESSION_PROPERTY.toErrorCode()));
    }

    @Test
    public void testInvalidMinRecordsPerSplit()
    {
        MongoSessionProperties sessionProperties = new MongoSessionProperties(new MongoClientConfig());
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .setPropertyValues(ImmutableMap.of(MIN_RECORDS_PER_SPLIT, -1L))
                .build();

        assertThatThrownBy(() -> getMinRecordsPerSplit(session))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("min_records_per_split must be greater than or equal to 0")
                .matches(throwable -> ((TrinoException) throwable).getErrorCode().equals(INVALID_SESSION_PROPERTY.toErrorCode()));
    }

    @Test
    public void testInvalidSplitStrategy()
    {
        MongoSessionProperties sessionProperties = new MongoSessionProperties(new MongoClientConfig());
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .setPropertyValues(ImmutableMap.of(SPLIT_STRATEGY, "UNKNOWN"))
                .build();

        assertThatThrownBy(() -> getSplitStrategy(session))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid value [UNKNOWN]")
                .hasMessageContaining("[AUTOMATIC, CHUNK, RANGE]");
    }
}
