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

import io.trino.plugin.mongodb.MongoIndex.MongodbIndexKey;
import io.trino.spi.connector.SortOrder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoCandidatePartitionKeyResolver
{
    @Test
    public void testResolvePrimaryKey()
    {
        MongoIndex idIndex = new MongoIndex(List.of(new MongodbIndexKey("_id", Optional.of(SortOrder.ASC_NULLS_LAST))));
        MongoIndex secondaryIndex = new MongoIndex(List.of(new MongodbIndexKey("created_at", Optional.of(SortOrder.ASC_NULLS_LAST))));

        assertThat(MongoCandidatePartitionKeyResolver.resolveKey(List.of(secondaryIndex, idIndex)))
                .contains("_id");
    }

    @Test
    public void testSkipHashedIdIndex()
    {
        MongoIndex hashedIdIndex = new MongoIndex(List.of(new MongodbIndexKey("_id", Optional.empty())));
        MongoIndex secondaryIndex = new MongoIndex(List.of(new MongodbIndexKey("user_id", Optional.of(SortOrder.ASC_NULLS_LAST))));

        assertThat(MongoCandidatePartitionKeyResolver.resolveKey(List.of(hashedIdIndex, secondaryIndex)))
                .contains("user_id");
    }

    @Test
    public void testSkipDottedFieldNames()
    {
        MongoIndex dottedIndex = new MongoIndex(List.of(new MongodbIndexKey("address.city", Optional.of(SortOrder.ASC_NULLS_LAST))));
        MongoIndex compoundDottedIndex = new MongoIndex(List.of(
                new MongodbIndexKey("user.age", Optional.of(SortOrder.ASC_NULLS_LAST)),
                new MongodbIndexKey("status", Optional.of(SortOrder.ASC_NULLS_LAST))));

        assertThat(MongoCandidatePartitionKeyResolver.resolveKey(List.of(dottedIndex, compoundDottedIndex)))
                .isEmpty();
    }

    @Test
    public void testResolveCompoundIndexLeadingKey()
    {
        MongoIndex hashedIdIndex = new MongoIndex(List.of(new MongodbIndexKey("_id", Optional.empty())));
        MongoIndex compoundIndex = new MongoIndex(List.of(
                new MongodbIndexKey("tenant_id", Optional.of(SortOrder.ASC_NULLS_LAST)),
                new MongodbIndexKey("created_at", Optional.of(SortOrder.ASC_NULLS_LAST))));

        assertThat(MongoCandidatePartitionKeyResolver.resolveKey(List.of(hashedIdIndex, compoundIndex)))
                .contains("tenant_id");
    }

    @Test
    public void testEmptyOrUnindexed()
    {
        assertThat(MongoCandidatePartitionKeyResolver.resolveKey(List.of())).isEmpty();

        MongoIndex hashedOnly = new MongoIndex(List.of(new MongodbIndexKey("data", Optional.empty())));
        assertThat(MongoCandidatePartitionKeyResolver.resolveKey(List.of(hashedOnly))).isEmpty();
    }
}
