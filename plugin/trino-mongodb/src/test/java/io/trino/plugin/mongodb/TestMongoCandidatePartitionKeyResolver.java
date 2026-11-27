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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoCandidatePartitionKeyResolver
{
    @Test
    public void testResolveIdIndex()
    {
        MongoIndex idIndex = new MongoIndex(List.of(new MongoIndex.MongodbIndexKey("_id", Optional.of(SortOrder.ASC_NULLS_LAST))));
        Optional<String> candidateKey = MongoCandidatePartitionKeyResolver.resolveKey(List.of(idIndex));
        assertThat(candidateKey).contains("_id");
    }

    @Test
    public void testSkipHashedIdIndex()
    {
        // Hashed index has empty sortOrder
        MongoIndex hashedIdIndex = new MongoIndex(List.of(new MongoIndex.MongodbIndexKey("_id", Optional.empty())));
        MongoIndex secondaryIndex = new MongoIndex(List.of(new MongoIndex.MongodbIndexKey("user_id", Optional.of(SortOrder.ASC_NULLS_LAST))));

        Optional<String> candidateKey = MongoCandidatePartitionKeyResolver.resolveKey(List.of(hashedIdIndex, secondaryIndex));
        assertThat(candidateKey).contains("user_id");
    }

    @Test
    public void testPreferSingleKeySecondaryOverCompound()
    {
        MongoIndex compoundIndex = new MongoIndex(List.of(
                new MongoIndex.MongodbIndexKey("region", Optional.of(SortOrder.ASC_NULLS_LAST)),
                new MongoIndex.MongodbIndexKey("ts", Optional.of(SortOrder.ASC_NULLS_LAST))));
        MongoIndex singleKeyIndex = new MongoIndex(List.of(
                new MongoIndex.MongodbIndexKey("order_id", Optional.of(SortOrder.ASC_NULLS_LAST))));

        Optional<String> candidateKey = MongoCandidatePartitionKeyResolver.resolveKey(List.of(compoundIndex, singleKeyIndex));
        assertThat(candidateKey).contains("order_id");
    }

    @Test
    public void testFallbackToCompoundLeadingKey()
    {
        MongoIndex compoundIndex = new MongoIndex(List.of(
                new MongoIndex.MongodbIndexKey("region", Optional.of(SortOrder.ASC_NULLS_LAST)),
                new MongoIndex.MongodbIndexKey("ts", Optional.of(SortOrder.ASC_NULLS_LAST))));

        Optional<String> candidateKey = MongoCandidatePartitionKeyResolver.resolveKey(List.of(compoundIndex));
        assertThat(candidateKey).contains("region");
    }

    @Test
    public void testEmptyWhenNoRangeIndex()
    {
        MongoIndex hashedIdIndex = new MongoIndex(List.of(new MongoIndex.MongodbIndexKey("_id", Optional.empty())));
        MongoIndex hashedOtherIndex = new MongoIndex(List.of(new MongoIndex.MongodbIndexKey("user_id", Optional.empty())));

        Optional<String> candidateKey = MongoCandidatePartitionKeyResolver.resolveKey(List.of(hashedIdIndex, hashedOtherIndex));
        assertThat(candidateKey).isEmpty();

        assertThat(MongoCandidatePartitionKeyResolver.resolveKey(List.of())).isEmpty();
    }
}
