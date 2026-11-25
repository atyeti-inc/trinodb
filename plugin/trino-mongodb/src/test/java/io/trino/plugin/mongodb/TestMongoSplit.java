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
import io.airlift.slice.SizeOf;
import io.trino.spi.HostAddress;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.List;
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
    public void testJsonRoundTrip()
    {
        MongoSplit expected = new MongoSplit(ImmutableList.of());

        String json = codec.toJson(expected);
        MongoSplit actual = codec.fromJson(json);

        assertThat(actual.getAddresses())
                .isEqualTo(ImmutableList.of());
        assertThat(actual.getPartitionBounds()).isEmpty();
    }

    @Test
    public void testLegacyUnpartitionedJsonRoundTrip()
    {
        String legacyJson = "{\"addresses\":[\"127.0.0.1:27017\"]}";
        MongoSplit actual = codec.fromJson(legacyJson);

        assertThat(actual.getAddresses()).containsExactly(HostAddress.fromParts("127.0.0.1", 27017));
        assertThat(actual.getPartitionBounds()).isEmpty();
    }

    @Test
    public void testExplicitUnpartitionedJsonRoundTrip()
    {
        MongoSplit expected = new MongoSplit(
                ImmutableList.of(HostAddress.fromParts("127.0.0.1", 27017)),
                Optional.empty());

        String json = codec.toJson(expected);
        MongoSplit actual = codec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getAddresses()).containsExactly(HostAddress.fromParts("127.0.0.1", 27017));
        assertThat(actual.getPartitionBounds()).isEmpty();
    }

    @Test
    public void testSingleKeyPartitionedSplitRoundTrip()
    {
        ObjectId lowerId = new ObjectId("507f1f77bcf86cd799439011");
        ObjectId upperId = new ObjectId("507f1f77bcf86cd799439012");
        BsonDocument lowerDoc = new BsonDocument("_id", new BsonObjectId(lowerId));
        BsonDocument upperDoc = new BsonDocument("_id", new BsonObjectId(upperId));

        MongoPartitionBounds bounds = MongoPartitionBounds.fromBson(
                ImmutableList.of("_id"),
                lowerDoc,
                true,
                upperDoc,
                false);

        MongoSplit expected = new MongoSplit(
                ImmutableList.of(HostAddress.fromParts("127.0.0.1", 27017)),
                Optional.of(bounds));

        String json = codec.toJson(expected);
        MongoSplit actual = codec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getPartitionBounds()).isPresent();

        MongoPartitionBounds actualBounds = actual.getPartitionBounds().get();
        assertThat(actualBounds.keyNames()).containsExactly("_id");
        assertThat(actualBounds.lowerInclusive()).isTrue();
        assertThat(actualBounds.upperInclusive()).isFalse();

        assertThat(actualBounds.parseLowerBoundDocument()).contains(lowerDoc);
        assertThat(actualBounds.parseUpperBoundDocument()).contains(upperDoc);
        assertThat(actualBounds.parseLowerBoundDocument().get().get("_id").asObjectId().getValue()).isEqualTo(lowerId);
        assertThat(actualBounds.parseUpperBoundDocument().get().get("_id").asObjectId().getValue()).isEqualTo(upperId);
    }

    @Test
    public void testCompoundKeyPartitionedSplitRoundTrip()
    {
        ObjectId lowerId = new ObjectId("507f1f77bcf86cd799439011");
        ObjectId upperId = new ObjectId("507f1f77bcf86cd799439012");

        BsonDocument lowerDoc = new BsonDocument()
                .append("tenant", new BsonString("tenant_a"))
                .append("region", new BsonInt32(10))
                .append("created", new BsonDateTime(1609459200000L))
                .append("_id", new BsonObjectId(lowerId));

        BsonDocument upperDoc = new BsonDocument()
                .append("tenant", new BsonString("tenant_a"))
                .append("region", new BsonInt32(20))
                .append("created", new BsonDateTime(1609545600000L))
                .append("_id", new BsonObjectId(upperId));

        List<String> keyNames = ImmutableList.of("tenant", "region", "created", "_id");
        MongoPartitionBounds bounds = MongoPartitionBounds.fromBson(
                keyNames,
                lowerDoc,
                true,
                upperDoc,
                false);

        MongoSplit split = new MongoSplit(
                ImmutableList.of(HostAddress.fromParts("10.0.0.1", 27017)),
                Optional.of(bounds));

        String json = codec.toJson(split);
        MongoSplit deserialized = codec.fromJson(json);

        assertThat(deserialized).isEqualTo(split);
        MongoPartitionBounds deserializedBounds = deserialized.getPartitionBounds().orElseThrow();
        assertThat(deserializedBounds.keyNames()).isEqualTo(keyNames);
        assertThat(deserializedBounds.parseLowerBoundDocument()).contains(lowerDoc);
        assertThat(deserializedBounds.parseUpperBoundDocument()).contains(upperDoc);

        BsonDocument parsedLower = deserializedBounds.parseLowerBoundDocument().get();
        assertThat(parsedLower.keySet()).containsExactlyElementsOf(keyNames);
        assertThat(parsedLower.get("tenant").asString().getValue()).isEqualTo("tenant_a");
        assertThat(parsedLower.get("region").asInt32().getValue()).isEqualTo(10);
        assertThat(parsedLower.get("created").asDateTime().getValue()).isEqualTo(1609459200000L);
        assertThat(parsedLower.get("_id").asObjectId().getValue()).isEqualTo(lowerId);
    }

    @Test
    public void testUnboundedEndpointsWithMinKeyAndMaxKey()
    {
        BsonDocument lowerDoc = new BsonDocument("_id", new BsonMinKey());
        BsonDocument upperDoc = new BsonDocument("_id", new BsonMaxKey());

        MongoPartitionBounds bounds = MongoPartitionBounds.fromBson(
                ImmutableList.of("_id"),
                lowerDoc,
                true,
                upperDoc,
                false);

        MongoSplit split = new MongoSplit(
                ImmutableList.of(HostAddress.fromParts("127.0.0.1", 27017)),
                Optional.of(bounds));

        String json = codec.toJson(split);
        MongoSplit deserialized = codec.fromJson(json);

        assertThat(deserialized).isEqualTo(split);
        MongoPartitionBounds deserializedBounds = deserialized.getPartitionBounds().orElseThrow();
        BsonDocument parsedLower = deserializedBounds.parseLowerBoundDocument().orElseThrow();
        BsonDocument parsedUpper = deserializedBounds.parseUpperBoundDocument().orElseThrow();

        assertThat(parsedLower.get("_id").getBsonType()).isEqualTo(BsonType.MIN_KEY);
        assertThat(parsedUpper.get("_id").getBsonType()).isEqualTo(BsonType.MAX_KEY);
    }

    @Test
    public void testOpenEndedInfinityWithEmptyBounds()
    {
        ObjectId boundaryId = new ObjectId("507f1f77bcf86cd799439011");
        BsonDocument doc = new BsonDocument("_id", new BsonObjectId(boundaryId));

        // [-inf, upper)
        MongoPartitionBounds openLower = MongoPartitionBounds.fromBson(
                ImmutableList.of("_id"),
                Optional.empty(),
                false,
                Optional.of(doc),
                false);

        MongoSplit splitOpenLower = new MongoSplit(
                ImmutableList.of(HostAddress.fromParts("127.0.0.1", 27017)),
                Optional.of(openLower));

        MongoSplit deserializedOpenLower = codec.fromJson(codec.toJson(splitOpenLower));
        assertThat(deserializedOpenLower).isEqualTo(splitOpenLower);
        assertThat(deserializedOpenLower.getPartitionBounds().orElseThrow().lowerBound()).isEmpty();
        assertThat(deserializedOpenLower.getPartitionBounds().orElseThrow().parseLowerBoundDocument()).isEmpty();
        assertThat(deserializedOpenLower.getPartitionBounds().orElseThrow().parseUpperBoundDocument()).contains(doc);

        // [lower, +inf)
        MongoPartitionBounds openUpper = MongoPartitionBounds.fromBson(
                ImmutableList.of("_id"),
                Optional.of(doc),
                true,
                Optional.empty(),
                false);

        MongoSplit splitOpenUpper = new MongoSplit(
                ImmutableList.of(HostAddress.fromParts("127.0.0.1", 27017)),
                Optional.of(openUpper));

        MongoSplit deserializedOpenUpper = codec.fromJson(codec.toJson(splitOpenUpper));
        assertThat(deserializedOpenUpper).isEqualTo(splitOpenUpper);
        assertThat(deserializedOpenUpper.getPartitionBounds().orElseThrow().upperBound()).isEmpty();
        assertThat(deserializedOpenUpper.getPartitionBounds().orElseThrow().parseUpperBoundDocument()).isEmpty();
        assertThat(deserializedOpenUpper.getPartitionBounds().orElseThrow().parseLowerBoundDocument()).contains(doc);
    }

    @Test
    public void testBoundaryInclusivityPermutations()
    {
        boolean[][] permutations = {
                {true, false},
                {false, true},
                {true, true},
                {false, false},
        };

        BsonDocument lowerDoc = new BsonDocument("_id", new BsonInt32(0));
        BsonDocument upperDoc = new BsonDocument("_id", new BsonInt32(100));

        for (boolean[] perm : permutations) {
            boolean lowerInclusive = perm[0];
            boolean upperInclusive = perm[1];

            MongoPartitionBounds bounds = MongoPartitionBounds.fromBson(
                    ImmutableList.of("_id"),
                    lowerDoc,
                    lowerInclusive,
                    upperDoc,
                    upperInclusive);

            MongoSplit split = new MongoSplit(
                    ImmutableList.of(HostAddress.fromParts("127.0.0.1", 27017)),
                    Optional.of(bounds));

            MongoSplit deserialized = codec.fromJson(codec.toJson(split));
            assertThat(deserialized.getPartitionBounds().orElseThrow().lowerInclusive()).isEqualTo(lowerInclusive);
            assertThat(deserialized.getPartitionBounds().orElseThrow().upperInclusive()).isEqualTo(upperInclusive);
        }
    }

    @Test
    public void testRetainedSizeAccounting()
    {
        MongoSplit emptySplit = new MongoSplit(ImmutableList.of());
        long emptySplitExpectedSize = instanceSize(MongoSplit.class)
                + estimatedSizeOf(ImmutableList.of(), HostAddress::getRetainedSizeInBytes)
                + sizeOf(Optional.empty(), MongoPartitionBounds::getRetainedSizeInBytes);
        assertThat(emptySplit.getRetainedSizeInBytes()).isEqualTo(emptySplitExpectedSize);

        HostAddress address = HostAddress.fromParts("127.0.0.1", 27017);
        MongoSplit addressOnlySplit = new MongoSplit(ImmutableList.of(address));
        assertThat(addressOnlySplit.getRetainedSizeInBytes()).isGreaterThan(emptySplit.getRetainedSizeInBytes());

        ObjectId lowerId = new ObjectId("507f1f77bcf86cd799439011");
        ObjectId upperId = new ObjectId("507f1f77bcf86cd799439012");
        MongoPartitionBounds bounds = MongoPartitionBounds.fromBson(
                ImmutableList.of("_id"),
                new BsonDocument("_id", new BsonObjectId(lowerId)),
                true,
                new BsonDocument("_id", new BsonObjectId(upperId)),
                false);

        long expectedBoundsSize = instanceSize(MongoPartitionBounds.class)
                + estimatedSizeOf(bounds.keyNames(), SizeOf::estimatedSizeOf)
                + sizeOf(bounds.lowerBound(), SizeOf::estimatedSizeOf)
                + sizeOf(bounds.upperBound(), SizeOf::estimatedSizeOf);
        assertThat(bounds.getRetainedSizeInBytes()).isEqualTo(expectedBoundsSize);

        MongoSplit partitionedSplit = new MongoSplit(ImmutableList.of(address), Optional.of(bounds));
        long expectedPartitionedSplitSize = instanceSize(MongoSplit.class)
                + estimatedSizeOf(ImmutableList.of(address), HostAddress::getRetainedSizeInBytes)
                + sizeOf(Optional.of(bounds), MongoPartitionBounds::getRetainedSizeInBytes);

        assertThat(partitionedSplit.getRetainedSizeInBytes()).isEqualTo(expectedPartitionedSplitSize);
        assertThat(partitionedSplit.getRetainedSizeInBytes()).isGreaterThan(addressOnlySplit.getRetainedSizeInBytes());
    }

    @Test
    public void testValidationFailures()
    {
        // Null keyNames
        assertThatThrownBy(() -> new MongoPartitionBounds(null, Optional.empty(), Optional.empty(), true, false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("keyNames is null");

        // Empty keyNames
        assertThatThrownBy(() -> new MongoPartitionBounds(ImmutableList.of(), Optional.empty(), Optional.empty(), true, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("keyNames cannot be empty");

        // Null lowerBound Optional
        assertThatThrownBy(() -> new MongoPartitionBounds(ImmutableList.of("_id"), null, Optional.empty(), true, false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("lowerBound is null");

        // Null upperBound Optional
        assertThatThrownBy(() -> new MongoPartitionBounds(ImmutableList.of("_id"), Optional.empty(), null, true, false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("upperBound is null");

        // Malformed Extended JSON string in lowerBound
        assertThatThrownBy(() -> new MongoPartitionBounds(
                ImmutableList.of("_id"),
                Optional.of("not-a-valid-bson-json"),
                Optional.empty(),
                true,
                false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Invalid BSON Extended JSON document:");

        // Malformed Extended JSON string in upperBound
        assertThatThrownBy(() -> new MongoPartitionBounds(
                ImmutableList.of("_id"),
                Optional.empty(),
                Optional.of("{unclosed-json"),
                true,
                false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Invalid BSON Extended JSON document:");
    }
}
