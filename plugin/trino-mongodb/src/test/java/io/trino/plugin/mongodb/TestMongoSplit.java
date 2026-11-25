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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMongoSplit
{
    private final JsonCodec<MongoSplit> codec = jsonCodec(MongoSplit.class);

    @Test
    public void testLegacyJsonDeserialization()
    {
        String legacyJson = "{\"addresses\":[\"127.0.0.1:27017\"]}";
        MongoSplit actual = codec.fromJson(legacyJson);

        assertThat(actual.getAddresses())
                .containsExactly(HostAddress.fromString("127.0.0.1:27017"));
        assertThat(actual.getPartitionBounds()).isEmpty();
    }

    @Test
    public void testExplicitUnpartitionedRoundTrip()
    {
        MongoSplit expected = new MongoSplit(
                ImmutableList.of(HostAddress.fromString("127.0.0.1:27017")),
                Optional.empty());

        String json = codec.toJson(expected);
        MongoSplit actual = codec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getAddresses())
                .containsExactly(HostAddress.fromString("127.0.0.1:27017"));
        assertThat(actual.getPartitionBounds()).isEmpty();
    }

    @Test
    public void testSingleKeyPartitionedSplitRoundTrip()
    {
        ObjectId id1 = new ObjectId("507f1f77bcf86cd799439011");
        ObjectId id2 = new ObjectId("507f1f77bcf86cd799439012");
        BsonDocument lower = new BsonDocument("_id", new BsonObjectId(id1));
        BsonDocument upper = new BsonDocument("_id", new BsonObjectId(id2));

        MongoPartitionBounds bounds = MongoPartitionBounds.fromBson(
                ImmutableList.of("_id"),
                lower,
                true,
                upper,
                false);

        MongoSplit expected = new MongoSplit(
                ImmutableList.of(HostAddress.fromString("10.0.0.1:27017")),
                Optional.of(bounds));

        String json = codec.toJson(expected);
        MongoSplit actual = codec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getPartitionBounds()).isPresent();

        MongoPartitionBounds actualBounds = actual.getPartitionBounds().get();
        assertThat(actualBounds.keyNames()).containsExactly("_id");
        assertThat(actualBounds.lowerInclusive()).isTrue();
        assertThat(actualBounds.upperInclusive()).isFalse();
        assertThat(actualBounds.parseLowerBoundDocument()).contains(lower);
        assertThat(actualBounds.parseUpperBoundDocument()).contains(upper);
    }

    @Test
    public void testCompoundKeyPartitionedSplitRoundTrip()
    {
        List<String> keyNames = ImmutableList.of("tenant", "region", "created", "_id");
        BsonDocument lower = new BsonDocument()
                .append("tenant", new BsonString("corp-a"))
                .append("region", new BsonInt32(1))
                .append("created", new BsonDateTime(1609459200000L))
                .append("_id", new BsonObjectId(new ObjectId("507f1f77bcf86cd799439011")));
        BsonDocument upper = new BsonDocument()
                .append("tenant", new BsonString("corp-b"))
                .append("region", new BsonInt32(2))
                .append("created", new BsonDateTime(1640995200000L))
                .append("_id", new BsonObjectId(new ObjectId("507f1f77bcf86cd799439012")));

        MongoPartitionBounds bounds = MongoPartitionBounds.fromBson(keyNames, lower, true, upper, false);
        MongoSplit expected = new MongoSplit(
                ImmutableList.of(HostAddress.fromString("127.0.0.1:27017")),
                Optional.of(bounds));

        String json = codec.toJson(expected);
        MongoSplit actual = codec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
        MongoPartitionBounds actualBounds = actual.getPartitionBounds().orElseThrow();
        assertThat(actualBounds.keyNames()).isEqualTo(keyNames);
        assertThat(actualBounds.parseLowerBoundDocument()).contains(lower);
        assertThat(actualBounds.parseUpperBoundDocument()).contains(upper);
    }

    @Test
    public void testMinKeyMaxKeyInfinityEndpoints()
    {
        BsonDocument lower = new BsonDocument("_id", new BsonMinKey());
        BsonDocument upper = new BsonDocument("_id", new BsonMaxKey());

        MongoPartitionBounds bounds = MongoPartitionBounds.fromBson(
                ImmutableList.of("_id"),
                lower,
                true,
                upper,
                false);

        MongoSplit expected = new MongoSplit(
                ImmutableList.of(HostAddress.fromString("127.0.0.1:27017")),
                Optional.of(bounds));

        String json = codec.toJson(expected);
        MongoSplit actual = codec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
        MongoPartitionBounds actualBounds = actual.getPartitionBounds().orElseThrow();
        BsonDocument parsedLower = actualBounds.parseLowerBoundDocument().orElseThrow();
        BsonDocument parsedUpper = actualBounds.parseUpperBoundDocument().orElseThrow();

        assertThat(parsedLower.get("_id").getBsonType()).isEqualTo(BsonType.MIN_KEY);
        assertThat(parsedUpper.get("_id").getBsonType()).isEqualTo(BsonType.MAX_KEY);
    }

    @Test
    public void testOpenEndedUnboundedIntervals()
    {
        MongoPartitionBounds lowerUnbounded = MongoPartitionBounds.fromBson(
                ImmutableList.of("_id"),
                Optional.empty(),
                true,
                Optional.of(new BsonDocument("_id", new BsonInt32(100))),
                false);

        MongoSplit split1 = new MongoSplit(ImmutableList.of(), Optional.of(lowerUnbounded));
        MongoSplit actual1 = codec.fromJson(codec.toJson(split1));

        assertThat(actual1).isEqualTo(split1);
        assertThat(actual1.getPartitionBounds().orElseThrow().lowerBound()).isEmpty();
        assertThat(actual1.getPartitionBounds().orElseThrow().parseLowerBoundDocument()).isEmpty();
        assertThat(actual1.getPartitionBounds().orElseThrow().parseUpperBoundDocument())
                .contains(new BsonDocument("_id", new BsonInt32(100)));

        MongoPartitionBounds upperUnbounded = MongoPartitionBounds.fromBson(
                ImmutableList.of("_id"),
                Optional.of(new BsonDocument("_id", new BsonInt32(100))),
                true,
                Optional.empty(),
                false);

        MongoSplit split2 = new MongoSplit(ImmutableList.of(), Optional.of(upperUnbounded));
        MongoSplit actual2 = codec.fromJson(codec.toJson(split2));

        assertThat(actual2).isEqualTo(split2);
        assertThat(actual2.getPartitionBounds().orElseThrow().upperBound()).isEmpty();
        assertThat(actual2.getPartitionBounds().orElseThrow().parseUpperBoundDocument()).isEmpty();
        assertThat(actual2.getPartitionBounds().orElseThrow().parseLowerBoundDocument())
                .contains(new BsonDocument("_id", new BsonInt32(100)));
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

        for (boolean[] perm : permutations) {
            boolean lowerInclusive = perm[0];
            boolean upperInclusive = perm[1];

            MongoPartitionBounds bounds = MongoPartitionBounds.fromBson(
                    ImmutableList.of("_id"),
                    new BsonDocument("_id", new BsonInt32(0)),
                    lowerInclusive,
                    new BsonDocument("_id", new BsonInt32(10)),
                    upperInclusive);

            MongoSplit split = new MongoSplit(ImmutableList.of(), Optional.of(bounds));
            MongoSplit actual = codec.fromJson(codec.toJson(split));

            assertThat(actual).isEqualTo(split);
            assertThat(actual.getPartitionBounds().orElseThrow().lowerInclusive()).isEqualTo(lowerInclusive);
            assertThat(actual.getPartitionBounds().orElseThrow().upperInclusive()).isEqualTo(upperInclusive);
        }
    }

    @Test
    public void testRetainedSizeInBytes()
    {
        MongoSplit emptySplit = new MongoSplit(ImmutableList.of());
        MongoSplit addressSplit = new MongoSplit(ImmutableList.of(HostAddress.fromString("127.0.0.1:27017")));

        MongoPartitionBounds bounds = MongoPartitionBounds.fromBson(
                ImmutableList.of("_id"),
                new BsonDocument("_id", new BsonInt32(0)),
                true,
                new BsonDocument("_id", new BsonInt32(10)),
                false);
        MongoSplit partitionedSplit = new MongoSplit(
                ImmutableList.of(HostAddress.fromString("127.0.0.1:27017")),
                Optional.of(bounds));

        assertThat(addressSplit.getRetainedSizeInBytes())
                .isGreaterThan(emptySplit.getRetainedSizeInBytes());
        assertThat(partitionedSplit.getRetainedSizeInBytes())
                .isGreaterThan(addressSplit.getRetainedSizeInBytes());
        assertThat(bounds.getRetainedSizeInBytes())
                .isGreaterThan(0L);
    }

    @Test
    public void testValidationInvariants()
    {
        assertThatThrownBy(() -> new MongoPartitionBounds(null, Optional.empty(), Optional.empty(), true, false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("keyNames is null");

        assertThatThrownBy(() -> new MongoPartitionBounds(ImmutableList.of(), Optional.empty(), Optional.empty(), true, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("keyNames cannot be empty");

        assertThatThrownBy(() -> new MongoPartitionBounds(ImmutableList.of("_id"), null, Optional.empty(), true, false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("lowerBound is null");

        assertThatThrownBy(() -> new MongoPartitionBounds(ImmutableList.of("_id"), Optional.empty(), null, true, false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("upperBound is null");

        assertThatThrownBy(() -> new MongoPartitionBounds(ImmutableList.of("_id"), Optional.of("invalid-json{"), Optional.empty(), true, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Invalid BSON Extended JSON: invalid-json{");

        assertThatThrownBy(() -> new MongoPartitionBounds(ImmutableList.of("_id"), Optional.empty(), Optional.of("not a json"), true, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Invalid BSON Extended JSON: not a json");

        assertThatThrownBy(() -> new MongoSplit(null, Optional.empty()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("addresses is null");
    }
}
