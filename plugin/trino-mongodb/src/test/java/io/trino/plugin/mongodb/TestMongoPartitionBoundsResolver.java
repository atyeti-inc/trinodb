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

import io.trino.spi.connector.SortOrder;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.mongodb.MongoPartitionBoundsResolver.buildPartitionBounds;
import static io.trino.plugin.mongodb.MongoPartitionBoundsResolver.calculateSplitCount;
import static io.trino.plugin.mongodb.MongoPartitionBoundsResolver.choosePartitionKey;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoPartitionBoundsResolver
{
    @Test
    public void testChoosePartitionKeyPrioritizesId()
    {
        MongoIndex idIndex = new MongoIndex(List.of(new MongoIndex.MongodbIndexKey("_id", Optional.of(SortOrder.ASC_NULLS_LAST))));
        MongoIndex otherIndex = new MongoIndex(List.of(new MongoIndex.MongodbIndexKey("user_id", Optional.of(SortOrder.ASC_NULLS_LAST))));

        assertThat(choosePartitionKey(List.of(otherIndex, idIndex))).contains("_id");
        assertThat(choosePartitionKey(List.of(idIndex))).contains("_id");
    }

    @Test
    public void testChoosePartitionKeyExcludesHashedIndex()
    {
        // Hashed index has empty sortOrder
        MongoIndex hashedIdIndex = new MongoIndex(List.of(new MongoIndex.MongodbIndexKey("_id", Optional.empty())));
        MongoIndex rangeOtherIndex = new MongoIndex(List.of(new MongoIndex.MongodbIndexKey("user_id", Optional.of(SortOrder.ASC_NULLS_LAST))));

        // When _id is hashed, fall back to other single-field range index
        assertThat(choosePartitionKey(List.of(hashedIdIndex, rangeOtherIndex))).contains("user_id");

        // When only hashed index is present, return empty
        assertThat(choosePartitionKey(List.of(hashedIdIndex))).isEmpty();
    }

    @Test
    public void testChoosePartitionKeyHandlesCompoundAndDescendingIndexes()
    {
        MongoIndex compoundIndex = new MongoIndex(List.of(
                new MongoIndex.MongodbIndexKey("a", Optional.of(SortOrder.ASC_NULLS_LAST)),
                new MongoIndex.MongodbIndexKey("b", Optional.of(SortOrder.ASC_NULLS_LAST))));
        MongoIndex descIndex = new MongoIndex(List.of(new MongoIndex.MongodbIndexKey("created_at", Optional.of(SortOrder.DESC_NULLS_LAST))));

        assertThat(choosePartitionKey(List.of(compoundIndex))).isEmpty();
        assertThat(choosePartitionKey(List.of(compoundIndex, descIndex))).contains("created_at");
        assertThat(choosePartitionKey(List.of())).isEmpty();
    }

    @Test
    public void testCalculateSplitCount()
    {
        // Doc count 0 or negative
        assertThat(calculateSplitCount(0, 100_000, 10_000, 1_000)).isEqualTo(1);
        assertThat(calculateSplitCount(-1, 100_000, 10_000, 1_000)).isEqualTo(1);

        // Doc count below minRecordsPerSplit
        assertThat(calculateSplitCount(500, 100_000, 10_000, 1_000)).isEqualTo(1);

        // Collection size unavailable (<= 0), minRecordsPerSplit set
        assertThat(calculateSplitCount(10_000, 0, 10_000, 1_000)).isEqualTo(10);
        assertThat(calculateSplitCount(10_000, -1, 10_000, 1_000)).isEqualTo(10);

        // Collection size available and minRecordsPerSplit set
        // Size splits = 100MB / 10MB = 10; Record splits = 10_000 / 2_000 = 5; Target = min(10, 5) = 5
        assertThat(calculateSplitCount(10_000, 100 * 1024 * 1024L, 10 * 1024 * 1024L, 2_000)).isEqualTo(5);

        // Size splits = 20MB / 10MB = 2; Record splits = 10_000 / 1_000 = 10; Target = min(2, 10) = 2
        assertThat(calculateSplitCount(10_000, 20 * 1024 * 1024L, 10 * 1024 * 1024L, 1_000)).isEqualTo(2);

        // Size unavailable, minRecordsPerSplit = 0, uses estimated doc size (1024 bytes)
        // 10_000 * 1024 = 10,240,000 bytes; splitSize = 1,024,000 bytes -> 10 splits
        assertThat(calculateSplitCount(10_000, 0, 1_024_000L, 0)).isEqualTo(10);
    }

    @Test
    public void testBuildPartitionBounds()
    {
        List<BsonValue> sampledKeys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            sampledKeys.add(new BsonInt32(i));
        }

        List<MongoPartitionBounds> bounds = buildPartitionBounds("_id", sampledKeys, 4);
        assertThat(bounds).hasSize(4);

        // Split 0: (-inf, 25)
        MongoPartitionBounds split0 = bounds.get(0);
        assertThat(split0.keyNames()).containsExactly("_id");
        assertThat(split0.parseLowerBoundDocument()).isEmpty();
        assertThat(split0.parseUpperBoundDocument()).contains(new BsonDocument("_id", new BsonInt32(25)));
        assertThat(split0.lowerInclusive()).isTrue();
        assertThat(split0.upperInclusive()).isFalse();

        // Split 1: [25, 50)
        MongoPartitionBounds split1 = bounds.get(1);
        assertThat(split1.parseLowerBoundDocument()).contains(new BsonDocument("_id", new BsonInt32(25)));
        assertThat(split1.parseUpperBoundDocument()).contains(new BsonDocument("_id", new BsonInt32(50)));
        assertThat(split1.lowerInclusive()).isTrue();
        assertThat(split1.upperInclusive()).isFalse();

        // Split 2: [50, 75)
        MongoPartitionBounds split2 = bounds.get(2);
        assertThat(split2.parseLowerBoundDocument()).contains(new BsonDocument("_id", new BsonInt32(50)));
        assertThat(split2.parseUpperBoundDocument()).contains(new BsonDocument("_id", new BsonInt32(75)));
        assertThat(split2.lowerInclusive()).isTrue();
        assertThat(split2.upperInclusive()).isFalse();

        // Split 3: [75, +inf)
        MongoPartitionBounds split3 = bounds.get(3);
        assertThat(split3.parseLowerBoundDocument()).contains(new BsonDocument("_id", new BsonInt32(75)));
        assertThat(split3.parseUpperBoundDocument()).isEmpty();
        assertThat(split3.lowerInclusive()).isTrue();
        assertThat(split3.upperInclusive()).isTrue();

        // Verify with MongoSplitQueryBuilder
        Optional<Document> filter0 = MongoSplitQueryBuilder.buildSplitFilter(split0);
        assertThat(filter0).contains(new Document("_id", new Document("$lt", 25)));

        Optional<Document> filter1 = MongoSplitQueryBuilder.buildSplitFilter(split1);
        assertThat(filter1).contains(new Document("_id", new Document("$gte", 25).append("$lt", 50)));

        Optional<Document> filter3 = MongoSplitQueryBuilder.buildSplitFilter(split3);
        assertThat(filter3).contains(new Document("_id", new Document("$gte", 75)));
    }

    @Test
    public void testBuildPartitionBoundsEdgeCases()
    {
        // Target splits <= 1
        assertThat(buildPartitionBounds("_id", List.of(new BsonInt32(1), new BsonInt32(2)), 1)).isEmpty();

        // Less than 2 distinct bounds
        assertThat(buildPartitionBounds("_id", List.of(new BsonInt32(1), new BsonInt32(1)), 4)).isEmpty();
        assertThat(buildPartitionBounds("_id", List.of(new BsonInt32(1)), 4)).isEmpty();
        assertThat(buildPartitionBounds("_id", List.of(), 4)).isEmpty();

        // Duplicates handled gracefully
        List<BsonValue> withDuplicates = List.of(
                new BsonInt32(1),
                new BsonInt32(1),
                new BsonInt32(2),
                new BsonInt32(2),
                new BsonInt32(3));
        List<MongoPartitionBounds> bounds = buildPartitionBounds("_id", withDuplicates, 3);
        assertThat(bounds).hasSize(3);
    }
}
