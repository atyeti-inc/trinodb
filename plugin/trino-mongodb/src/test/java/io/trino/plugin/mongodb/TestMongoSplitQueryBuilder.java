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
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoSplitQueryBuilder
{
    @Test
    public void testSingleKeyObjectIdBounded()
    {
        ObjectId lowerId = new ObjectId("507f1f77bcf86cd799439011");
        ObjectId upperId = new ObjectId("507f1f77bcf86cd799439012");

        MongoPartitionBounds bounds = MongoPartitionBounds.fromBson(
                ImmutableList.of("_id"),
                new BsonDocument("_id", new BsonObjectId(lowerId)),
                true,
                new BsonDocument("_id", new BsonObjectId(upperId)),
                false);

        Optional<Document> filter = MongoSplitQueryBuilder.buildSplitFilter(bounds);
        assertThat(filter).contains(new Document("_id", new Document("$gte", lowerId).append("$lt", upperId)));
    }

    @Test
    public void testSingleKeyOpenEndedWithSentinels()
    {
        ObjectId upperId = new ObjectId("507f1f77bcf86cd799439012");
        ObjectId lowerId = new ObjectId("507f1f77bcf86cd799439011");

        // MinKey lower bound is treated as open lower (-infinity)
        MongoPartitionBounds openLower = MongoPartitionBounds.fromBson(
                ImmutableList.of("_id"),
                new BsonDocument("_id", new BsonMinKey()),
                true,
                new BsonDocument("_id", new BsonObjectId(upperId)),
                false);
        assertThat(MongoSplitQueryBuilder.buildSplitFilter(openLower))
                .contains(new Document("_id", new Document("$lt", upperId)));

        // MaxKey upper bound is treated as open upper (+infinity)
        MongoPartitionBounds openUpper = MongoPartitionBounds.fromBson(
                ImmutableList.of("_id"),
                new BsonDocument("_id", new BsonObjectId(lowerId)),
                true,
                new BsonDocument("_id", new BsonMaxKey()),
                false);
        assertThat(MongoSplitQueryBuilder.buildSplitFilter(openUpper))
                .contains(new Document("_id", new Document("$gte", lowerId)));

        // Both bounds MinKey/MaxKey sentinels -> unbounded
        MongoPartitionBounds allSentinels = MongoPartitionBounds.fromBson(
                ImmutableList.of("_id"),
                new BsonDocument("_id", new BsonMinKey()),
                true,
                new BsonDocument("_id", new BsonMaxKey()),
                false);
        assertThat(MongoSplitQueryBuilder.buildSplitFilter(allSentinels)).isEmpty();

        // Absent bounds -> unbounded
        MongoPartitionBounds absentBounds = new MongoPartitionBounds(
                ImmutableList.of("_id"),
                Optional.empty(),
                Optional.empty(),
                true,
                false);
        assertThat(MongoSplitQueryBuilder.buildSplitFilter(absentBounds)).isEmpty();
    }

    @Test
    public void testSingleKeyInclusivityPermutations()
    {
        ObjectId lowerId = new ObjectId("507f1f77bcf86cd799439011");
        ObjectId upperId = new ObjectId("507f1f77bcf86cd799439012");

        // [lower, upper]
        MongoPartitionBounds closed = MongoPartitionBounds.fromBson(
                ImmutableList.of("_id"),
                new BsonDocument("_id", new BsonObjectId(lowerId)),
                true,
                new BsonDocument("_id", new BsonObjectId(upperId)),
                true);
        assertThat(MongoSplitQueryBuilder.buildSplitFilter(closed))
                .contains(new Document("_id", new Document("$gte", lowerId).append("$lte", upperId)));

        // (lower, upper]
        MongoPartitionBounds openClosed = MongoPartitionBounds.fromBson(
                ImmutableList.of("_id"),
                new BsonDocument("_id", new BsonObjectId(lowerId)),
                false,
                new BsonDocument("_id", new BsonObjectId(upperId)),
                true);
        assertThat(MongoSplitQueryBuilder.buildSplitFilter(openClosed))
                .contains(new Document("_id", new Document("$gt", lowerId).append("$lte", upperId)));

        // (lower, upper)
        MongoPartitionBounds open = MongoPartitionBounds.fromBson(
                ImmutableList.of("_id"),
                new BsonDocument("_id", new BsonObjectId(lowerId)),
                false,
                new BsonDocument("_id", new BsonObjectId(upperId)),
                false);
        assertThat(MongoSplitQueryBuilder.buildSplitFilter(open))
                .contains(new Document("_id", new Document("$gt", lowerId).append("$lt", upperId)));
    }

    @Test
    public void testSingleKeyDiverseTypes()
    {
        // Int32
        MongoPartitionBounds intBounds = MongoPartitionBounds.fromBson(
                ImmutableList.of("col"),
                new BsonDocument("col", new BsonInt32(10)),
                true,
                new BsonDocument("col", new BsonInt32(20)),
                false);
        assertThat(MongoSplitQueryBuilder.buildSplitFilter(intBounds))
                .contains(new Document("col", new Document("$gte", 10).append("$lt", 20)));

        // Int64
        MongoPartitionBounds longBounds = MongoPartitionBounds.fromBson(
                ImmutableList.of("col"),
                new BsonDocument("col", new BsonInt64(100L)),
                true,
                new BsonDocument("col", new BsonInt64(200L)),
                false);
        assertThat(MongoSplitQueryBuilder.buildSplitFilter(longBounds))
                .contains(new Document("col", new Document("$gte", 100L).append("$lt", 200L)));

        // Double
        MongoPartitionBounds doubleBounds = MongoPartitionBounds.fromBson(
                ImmutableList.of("col"),
                new BsonDocument("col", new BsonDouble(1.5)),
                true,
                new BsonDocument("col", new BsonDouble(2.5)),
                false);
        assertThat(MongoSplitQueryBuilder.buildSplitFilter(doubleBounds))
                .contains(new Document("col", new Document("$gte", 1.5).append("$lt", 2.5)));

        // Decimal128
        Decimal128 lowerDec = new Decimal128(new BigDecimal("123.45"));
        Decimal128 upperDec = new Decimal128(new BigDecimal("678.90"));
        MongoPartitionBounds decimalBounds = MongoPartitionBounds.fromBson(
                ImmutableList.of("col"),
                new BsonDocument("col", new BsonDecimal128(lowerDec)),
                true,
                new BsonDocument("col", new BsonDecimal128(upperDec)),
                false);
        assertThat(MongoSplitQueryBuilder.buildSplitFilter(decimalBounds))
                .contains(new Document("col", new Document("$gte", lowerDec).append("$lt", upperDec)));

        // String
        MongoPartitionBounds stringBounds = MongoPartitionBounds.fromBson(
                ImmutableList.of("col"),
                new BsonDocument("col", new BsonString("aaa")),
                true,
                new BsonDocument("col", new BsonString("zzz")),
                false);
        assertThat(MongoSplitQueryBuilder.buildSplitFilter(stringBounds))
                .contains(new Document("col", new Document("$gte", "aaa").append("$lt", "zzz")));

        // Boolean
        MongoPartitionBounds booleanBounds = MongoPartitionBounds.fromBson(
                ImmutableList.of("col"),
                new BsonDocument("col", new BsonBoolean(false)),
                true,
                new BsonDocument("col", new BsonBoolean(true)),
                true);
        assertThat(MongoSplitQueryBuilder.buildSplitFilter(booleanBounds))
                .contains(new Document("col", new Document("$gte", false).append("$lte", true)));

        // DateTime
        long lowerTime = 1609459200000L;
        long upperTime = 1609545600000L;
        MongoPartitionBounds dateBounds = MongoPartitionBounds.fromBson(
                ImmutableList.of("col"),
                new BsonDocument("col", new BsonDateTime(lowerTime)),
                true,
                new BsonDocument("col", new BsonDateTime(upperTime)),
                false);
        assertThat(MongoSplitQueryBuilder.buildSplitFilter(dateBounds))
                .contains(new Document("col", new Document("$gte", new Date(lowerTime)).append("$lt", new Date(upperTime))));

        // Binary
        byte[] lowerBytes = new byte[] {1, 2, 3};
        byte[] upperBytes = new byte[] {4, 5, 6};
        MongoPartitionBounds binaryBounds = MongoPartitionBounds.fromBson(
                ImmutableList.of("col"),
                new BsonDocument("col", new BsonBinary(lowerBytes)),
                true,
                new BsonDocument("col", new BsonBinary(upperBytes)),
                false);
        assertThat(MongoSplitQueryBuilder.buildSplitFilter(binaryBounds))
                .contains(new Document("col", new Document("$gte", new Binary(lowerBytes)).append("$lt", new Binary(upperBytes))));
    }

    @Test
    public void testCompoundKeyCommonPrefixWithTerminalRange()
    {
        ObjectId id1 = new ObjectId("507f1f77bcf86cd799439011");
        ObjectId id2 = new ObjectId("507f1f77bcf86cd799439012");

        BsonDocument lower = new BsonDocument("tenant", new BsonString("acme"))
                .append("created", new BsonInt64(1000L))
                .append("_id", new BsonObjectId(id1));
        BsonDocument upper = new BsonDocument("tenant", new BsonString("acme"))
                .append("created", new BsonInt64(1000L))
                .append("_id", new BsonObjectId(id2));

        MongoPartitionBounds bounds = MongoPartitionBounds.fromBson(
                ImmutableList.of("tenant", "created", "_id"),
                lower,
                true,
                upper,
                false);

        Optional<Document> filter = MongoSplitQueryBuilder.buildSplitFilter(bounds);
        Document expected = new Document("tenant", "acme")
                .append("created", 1000L)
                .append("_id", new Document("$gte", id1).append("$lt", id2));
        assertThat(filter).contains(expected);
    }

    @Test
    public void testCompoundKeyDifferingPrefixesLexicographical()
    {
        BsonDocument lower = new BsonDocument("tenant", new BsonString("a"))
                .append("region", new BsonInt32(10));
        BsonDocument upper = new BsonDocument("tenant", new BsonString("b"))
                .append("region", new BsonInt32(20));

        MongoPartitionBounds bounds = MongoPartitionBounds.fromBson(
                ImmutableList.of("tenant", "region"),
                lower,
                true,
                upper,
                false);

        Optional<Document> filter = MongoSplitQueryBuilder.buildSplitFilter(bounds);

        Document expectedLowerAst = new Document("$or", ImmutableList.of(
                new Document("tenant", new Document("$gt", "a")),
                new Document("tenant", "a").append("region", new Document("$gte", 10))));

        Document expectedUpperAst = new Document("$or", ImmutableList.of(
                new Document("tenant", new Document("$lt", "b")),
                new Document("tenant", "b").append("region", new Document("$lt", 20))));

        Document expected = new Document("$and", ImmutableList.of(expectedLowerAst, expectedUpperAst));
        assertThat(filter).contains(expected);
    }

    @Test
    public void testCompoundKeyCommonPrefixWithDifferingSuffix()
    {
        BsonDocument lower = new BsonDocument("org", new BsonString("corp"))
                .append("tenant", new BsonString("a"))
                .append("region", new BsonInt32(10));
        BsonDocument upper = new BsonDocument("org", new BsonString("corp"))
                .append("tenant", new BsonString("b"))
                .append("region", new BsonInt32(20));

        MongoPartitionBounds bounds = MongoPartitionBounds.fromBson(
                ImmutableList.of("org", "tenant", "region"),
                lower,
                true,
                upper,
                false);

        Optional<Document> filter = MongoSplitQueryBuilder.buildSplitFilter(bounds);

        Document expectedPrefix = new Document("org", "corp");
        Document expectedLowerAst = new Document("$or", ImmutableList.of(
                new Document("tenant", new Document("$gt", "a")),
                new Document("tenant", "a").append("region", new Document("$gte", 10))));
        Document expectedUpperAst = new Document("$or", ImmutableList.of(
                new Document("tenant", new Document("$lt", "b")),
                new Document("tenant", "b").append("region", new Document("$lt", 20))));

        Document expected = new Document("$and", ImmutableList.of(expectedPrefix, expectedLowerAst, expectedUpperAst));
        assertThat(filter).contains(expected);
    }

    @Test
    public void testCompoundKeyTrailingSentinels()
    {
        // Trailing sentinels truncate to prefix condition
        BsonDocument lower = new BsonDocument("tenant", new BsonString("acme"))
                .append("region", new BsonMinKey())
                .append("created", new BsonMinKey());
        BsonDocument upper = new BsonDocument("tenant", new BsonString("acme"))
                .append("region", new BsonMaxKey())
                .append("created", new BsonMaxKey());

        MongoPartitionBounds bounds = MongoPartitionBounds.fromBson(
                ImmutableList.of("tenant", "region", "created"),
                lower,
                true,
                upper,
                false);

        Optional<Document> filter = MongoSplitQueryBuilder.buildSplitFilter(bounds);
        assertThat(filter).contains(new Document("tenant", "acme"));

        // All keys are sentinels -> unbounded
        BsonDocument allMin = new BsonDocument("tenant", new BsonMinKey())
                .append("region", new BsonMinKey());
        BsonDocument allMax = new BsonDocument("tenant", new BsonMaxKey())
                .append("region", new BsonMaxKey());

        MongoPartitionBounds unboundedBounds = MongoPartitionBounds.fromBson(
                ImmutableList.of("tenant", "region"),
                allMin,
                true,
                allMax,
                false);

        assertThat(MongoSplitQueryBuilder.buildSplitFilter(unboundedBounds)).isEmpty();
    }

    @Test
    public void testCompoundKeyAsymmetricBounds()
    {
        // Only lower bound present
        BsonDocument lower = new BsonDocument("tenant", new BsonString("a"))
                .append("region", new BsonInt32(10));

        MongoPartitionBounds lowerOnly = MongoPartitionBounds.fromBson(
                ImmutableList.of("tenant", "region"),
                Optional.of(lower),
                true,
                Optional.empty(),
                false);

        Optional<Document> filter = MongoSplitQueryBuilder.buildSplitFilter(lowerOnly);
        Document expectedLowerAst = new Document("$or", ImmutableList.of(
                new Document("tenant", new Document("$gt", "a")),
                new Document("tenant", "a").append("region", new Document("$gte", 10))));
        assertThat(filter).contains(expectedLowerAst);
    }
}
