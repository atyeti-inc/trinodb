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
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.airlift.units.MinDataSize;
import jakarta.validation.constraints.Min;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestMongoClientConfig
{
    @Test
    @SuppressWarnings("deprecation")
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(MongoClientConfig.class)
                .setConnectionUrl(null)
                .setSchemaCollection("_schema")
                .setCaseInsensitiveNameMatching(false)
                .setMinConnectionsPerHost(0)
                .setConnectionsPerHost(100)
                .setMaxWaitTime(120_000)
                .setConnectionTimeout(10_000)
                .setSocketTimeout(0)
                .setTlsEnabled(false)
                .setMaxConnectionIdleTime(0)
                .setCursorBatchSize(0)
                .setReadPreference(ReadPreferenceType.PRIMARY)
                .setWriteConcern(WriteConcernType.ACKNOWLEDGED)
                .setRequiredReplicaSetName(null)
                .setImplicitRowFieldPrefix("_pos")
                .setProjectionPushdownEnabled(true)
                .setAllowLocalScheduling(false)
                .setInsertOrdered(false)
                .setDynamicFilteringWaitTimeout(new Duration(5, SECONDS))
                .setParallelSplitEnabled(true)
                .setSplitSize(DataSize.of(64, Unit.MEGABYTE))
                .setMinRecordsPerSplit(0L)
                .setSplitStrategy(MongoSplitStrategy.AUTOMATIC));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testExplicitPropertyMappings()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("mongodb.schema-collection", "_my_schema")
                .put("mongodb.case-insensitive-name-matching", "true")
                .put("mongodb.connection-url", "mongodb://router1.example.com:27017,router2.example2.com:27017,router3.example3.com:27017/")
                .put("mongodb.min-connections-per-host", "1")
                .put("mongodb.connections-per-host", "99")
                .put("mongodb.max-wait-time", "120001")
                .put("mongodb.connection-timeout", "9999")
                .put("mongodb.socket-timeout", "1")
                .put("mongodb.tls.enabled", "true")
                .put("mongodb.max-connection-idle-time", "180000")
                .put("mongodb.cursor-batch-size", "1")
                .put("mongodb.read-preference", "NEAREST")
                .put("mongodb.write-concern", "UNACKNOWLEDGED")
                .put("mongodb.required-replica-set", "replica_set")
                .put("mongodb.implicit-row-field-prefix", "_prefix")
                .put("mongodb.projection-pushdown-enabled", "false")
                .put("mongodb.allow-local-scheduling", "true")
                .put("mongodb.insert-ordered", "true")
                .put("mongodb.dynamic-filtering.wait-timeout", "2ms")
                .put("mongodb.parallel-split-enabled", "false")
                .put("mongodb.split-size", "128MB")
                .put("mongodb.min-records-per-split", "50000")
                .put("mongodb.split-strategy", "CHUNK")
                .buildOrThrow();

        MongoClientConfig expected = new MongoClientConfig()
                .setSchemaCollection("_my_schema")
                .setCaseInsensitiveNameMatching(true)
                .setConnectionUrl("mongodb://router1.example.com:27017,router2.example2.com:27017,router3.example3.com:27017/")
                .setMinConnectionsPerHost(1)
                .setConnectionsPerHost(99)
                .setMaxWaitTime(120_001)
                .setConnectionTimeout(9_999)
                .setSocketTimeout(1)
                .setTlsEnabled(true)
                .setMaxConnectionIdleTime(180_000)
                .setCursorBatchSize(1)
                .setReadPreference(ReadPreferenceType.NEAREST)
                .setWriteConcern(WriteConcernType.UNACKNOWLEDGED)
                .setRequiredReplicaSetName("replica_set")
                .setImplicitRowFieldPrefix("_prefix")
                .setProjectionPushdownEnabled(false)
                .setAllowLocalScheduling(true)
                .setInsertOrdered(true)
                .setDynamicFilteringWaitTimeout(new Duration(2, MILLISECONDS))
                .setParallelSplitEnabled(false)
                .setSplitSize(DataSize.of(128, Unit.MEGABYTE))
                .setMinRecordsPerSplit(50000)
                .setSplitStrategy(MongoSplitStrategy.CHUNK);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidation()
    {
        assertFailsValidation(
                new MongoClientConfig().setSplitSize(DataSize.ofBytes(0)),
                "splitSize",
                "must be greater than or equal to 1B",
                MinDataSize.class);

        assertFailsValidation(
                new MongoClientConfig().setMinRecordsPerSplit(-1),
                "minRecordsPerSplit",
                "must be greater than or equal to 0",
                Min.class);
    }
}
