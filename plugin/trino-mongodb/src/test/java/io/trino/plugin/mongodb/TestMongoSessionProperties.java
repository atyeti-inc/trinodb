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
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.mongodb.MongoSessionProperties.AGGREGATION_PUSHDOWN_ENABLED;
import static io.trino.plugin.mongodb.MongoSessionProperties.isAggregationPushdownEnabled;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoSessionProperties
{
    @Test
    public void testAggregationPushdownEnabledDefault()
    {
        MongoClientConfig config = new MongoClientConfig();
        MongoSessionProperties sessionProperties = new MongoSessionProperties(config);
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .build();
        assertThat(isAggregationPushdownEnabled(session)).isTrue();

        config.setAggregationPushdownEnabled(false);
        sessionProperties = new MongoSessionProperties(config);
        session = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .build();
        assertThat(isAggregationPushdownEnabled(session)).isFalse();
    }

    @Test
    public void testAggregationPushdownEnabledOverride()
    {
        MongoClientConfig config = new MongoClientConfig().setAggregationPushdownEnabled(true);
        MongoSessionProperties sessionProperties = new MongoSessionProperties(config);
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .setPropertyValues(ImmutableMap.of(AGGREGATION_PUSHDOWN_ENABLED, false))
                .build();
        assertThat(isAggregationPushdownEnabled(session)).isFalse();

        config.setAggregationPushdownEnabled(false);
        sessionProperties = new MongoSessionProperties(config);
        session = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .setPropertyValues(ImmutableMap.of(AGGREGATION_PUSHDOWN_ENABLED, true))
                .build();
        assertThat(isAggregationPushdownEnabled(session)).isTrue();
    }
}
