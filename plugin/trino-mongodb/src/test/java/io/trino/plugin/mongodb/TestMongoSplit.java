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
import io.airlift.json.JsonCodec;
import io.trino.spi.HostAddress;
import org.bson.Document;
import org.bson.json.JsonParseException;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMongoSplit
{
    private final JsonCodec<MongoSplit> codec = jsonCodec(MongoSplit.class);

    @Test
    public void testJsonRoundTripUnpartitioned()
    {
        MongoSplit expected = new MongoSplit(ImmutableList.of(HostAddress.fromString("localhost:27017")));

        String json = codec.toJson(expected);
        MongoSplit actual = codec.fromJson(json);

        assertThat(actual.getAddresses()).isEqualTo(expected.getAddresses());
        assertThat(actual.getPartitionRange()).isEmpty();
    }

    @Test
    public void testBackwardCompatibilityLegacyJson()
    {
        String legacyJson = "{\"addresses\":[\"127.0.0.1:27017\"]}";
        MongoSplit actual = codec.fromJson(legacyJson);

        assertThat(actual.getAddresses()).containsExactly(HostAddress.fromString("127.0.0.1:27017"));
        assertThat(actual.getPartitionRange()).isEmpty();
    }

    @Test
    public void testJsonRoundTripSingleKeyPartition()
    {
        MongoPartitionRange range = MongoPartitionRange.fromDocuments(
                ImmutableList.of("_id"),
                Optional.of(new Document("_id", 100)),
                true,
                Optional.of(new Document("_id", 200)),
                false);

        MongoSplit split = new MongoSplit(
                ImmutableList.of(HostAddress.fromString("localhost:27017")),
                Optional.of(range));

        String json = codec.toJson(split);
        MongoSplit deserialized = codec.fromJson(json);

        assertThat(deserialized.getAddresses()).isEqualTo(split.getAddresses());
        assertThat(deserialized.getPartitionRange()).isPresent();

        MongoPartitionRange deserializedRange = deserialized.getPartitionRange().orElseThrow();
        assertThat(deserializedRange.partitionKeys()).containsExactly("_id");
        assertThat(deserializedRange.lowInclusive()).isTrue();
        assertThat(deserializedRange.highInclusive()).isFalse();
        assertThat(deserializedRange.lowBoundDocument()).contains(new Document("_id", 100));
        assertThat(deserializedRange.highBoundDocument()).contains(new Document("_id", 200));
    }

    @Test
    public void testJsonRoundTripCompoundKeyPartition()
    {
        MongoPartitionRange range = MongoPartitionRange.fromDocuments(
                ImmutableList.of("tenant", "created_date"),
                Optional.of(new Document("tenant", "acme").append("created_date", "2023-01-01")),
                true,
                Optional.of(new Document("tenant", "acme").append("created_date", "2023-12-31")),
                true);

        MongoSplit split = new MongoSplit(
                ImmutableList.of(HostAddress.fromString("10.0.0.1:27017"), HostAddress.fromString("10.0.0.2:27017")),
                Optional.of(range));

        String json = codec.toJson(split);
        MongoSplit deserialized = codec.fromJson(json);

        assertThat(deserialized.getAddresses()).isEqualTo(split.getAddresses());
        assertThat(deserialized.getPartitionRange()).isPresent();

        MongoPartitionRange deserializedRange = deserialized.getPartitionRange().orElseThrow();
        assertThat(deserializedRange.partitionKeys()).containsExactly("tenant", "created_date");
        assertThat(deserializedRange.lowInclusive()).isTrue();
        assertThat(deserializedRange.highInclusive()).isTrue();
        assertThat(deserializedRange.lowBoundDocument()).contains(new Document("tenant", "acme").append("created_date", "2023-01-01"));
        assertThat(deserializedRange.highBoundDocument()).contains(new Document("tenant", "acme").append("created_date", "2023-12-31"));
    }

    @Test
    public void testUnboundedEndpoints()
    {
        MongoPartitionRange range = MongoPartitionRange.fromDocuments(
                ImmutableList.of("_id"),
                Optional.empty(),
                true,
                Optional.empty(),
                false);

        MongoSplit split = new MongoSplit(
                ImmutableList.of(HostAddress.fromString("localhost:27017")),
                Optional.of(range));

        String json = codec.toJson(split);
        MongoSplit deserialized = codec.fromJson(json);

        MongoPartitionRange deserializedRange = deserialized.getPartitionRange().orElseThrow();
        assertThat(deserializedRange.lowBoundJson()).isEmpty();
        assertThat(deserializedRange.highBoundJson()).isEmpty();
        assertThat(deserializedRange.lowBoundDocument()).isEmpty();
        assertThat(deserializedRange.highBoundDocument()).isEmpty();
    }

    @Test
    public void testMinKeyAndMaxKeyEndpoints()
    {
        Document lowDoc = new Document("_id", new MinKey());
        Document highDoc = new Document("_id", new MaxKey());

        MongoPartitionRange range = MongoPartitionRange.fromDocuments(
                ImmutableList.of("_id"),
                Optional.of(lowDoc),
                true,
                Optional.of(highDoc),
                false);

        MongoSplit split = new MongoSplit(
                ImmutableList.of(HostAddress.fromString("localhost:27017")),
                Optional.of(range));

        String json = codec.toJson(split);
        MongoSplit deserialized = codec.fromJson(json);

        MongoPartitionRange deserializedRange = deserialized.getPartitionRange().orElseThrow();
        Optional<Document> deserializedLow = deserializedRange.lowBoundDocument();
        Optional<Document> deserializedHigh = deserializedRange.highBoundDocument();

        assertThat(deserializedLow).isPresent();
        assertThat(deserializedHigh).isPresent();
        assertThat(deserializedLow.orElseThrow().get("_id")).isInstanceOf(MinKey.class);
        assertThat(deserializedHigh.orElseThrow().get("_id")).isInstanceOf(MaxKey.class);
    }

    @Test
    public void testInclusivityVariations()
    {
        boolean[][] permutations = {
                {true, false},
                {false, true},
                {true, true},
                {false, false},
        };

        for (boolean[] pair : permutations) {
            boolean lowInclusive = pair[0];
            boolean highInclusive = pair[1];

            MongoPartitionRange range = new MongoPartitionRange(
                    ImmutableList.of("key"),
                    Optional.of("{\"key\": 1}"),
                    lowInclusive,
                    Optional.of("{\"key\": 10}"),
                    highInclusive);

            MongoSplit split = new MongoSplit(ImmutableList.of(), Optional.of(range));
            MongoSplit deserialized = codec.fromJson(codec.toJson(split));

            MongoPartitionRange deserializedRange = deserialized.getPartitionRange().orElseThrow();
            assertThat(deserializedRange.lowInclusive()).isEqualTo(lowInclusive);
            assertThat(deserializedRange.highInclusive()).isEqualTo(highInclusive);
        }
    }

    @Test
    public void testRetainedSizeInBytes()
    {
        MongoSplit unpartitioned = new MongoSplit(ImmutableList.of(HostAddress.fromString("localhost:27017")));

        MongoPartitionRange range = MongoPartitionRange.fromDocuments(
                ImmutableList.of("_id"),
                Optional.of(new Document("_id", 100)),
                true,
                Optional.of(new Document("_id", 200)),
                false);

        MongoSplit partitioned = new MongoSplit(
                ImmutableList.of(HostAddress.fromString("localhost:27017")),
                Optional.of(range));

        assertThat(partitioned.getRetainedSizeInBytes())
                .isGreaterThan(unpartitioned.getRetainedSizeInBytes());

        long expectedRetainedSize = instanceSize(MongoSplit.class)
                + estimatedSizeOf(partitioned.getAddresses(), HostAddress::getRetainedSizeInBytes)
                + sizeOf(partitioned.getPartitionRange(), MongoPartitionRange::getRetainedSizeInBytes);
        assertThat(partitioned.getRetainedSizeInBytes()).isEqualTo(expectedRetainedSize);
    }

    @Test
    public void testValidationErrors()
    {
        assertThatThrownBy(() -> new MongoPartitionRange(
                null,
                Optional.of("{\"_id\": 1}"),
                true,
                Optional.empty(),
                false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("partitionKeys is null");

        assertThatThrownBy(() -> new MongoPartitionRange(
                ImmutableList.of(),
                Optional.of("{\"_id\": 1}"),
                true,
                Optional.empty(),
                false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("partitionKeys cannot be empty");

        assertThatThrownBy(() -> new MongoPartitionRange(
                ImmutableList.of("_id"),
                null,
                true,
                Optional.empty(),
                false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("lowBoundJson is null");

        assertThatThrownBy(() -> new MongoPartitionRange(
                ImmutableList.of("_id"),
                Optional.empty(),
                true,
                null,
                false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("highBoundJson is null");

        assertThatThrownBy(() -> new MongoPartitionRange(
                ImmutableList.of("_id"),
                Optional.of("{invalid json}"),
                true,
                Optional.empty(),
                false))
                .isInstanceOf(JsonParseException.class);

        assertThatThrownBy(() -> new MongoPartitionRange(
                ImmutableList.of("_id"),
                Optional.empty(),
                true,
                Optional.of("not json"),
                false))
                .isInstanceOf(JsonParseException.class);

        assertThatThrownBy(() -> MongoPartitionRange.fromDocuments(
                ImmutableList.of("_id"),
                null,
                true,
                Optional.empty(),
                false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("lowBound is null");

        assertThatThrownBy(() -> MongoPartitionRange.fromDocuments(
                ImmutableList.of("_id"),
                Optional.empty(),
                true,
                null,
                false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("highBound is null");

        assertThatThrownBy(() -> new MongoSplit(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("addresses is null");

        assertThatThrownBy(() -> new MongoSplit(ImmutableList.of(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("partitionRange is null");
    }
}
