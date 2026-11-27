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

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class MongoCandidatePartitionKeyResolver
{
    private MongoCandidatePartitionKeyResolver() {}

    public static Optional<String> resolveKey(List<MongoIndex> indexes)
    {
        requireNonNull(indexes, "indexes is null");

        // Pass 1 (Primary Key): Scan indexes for _id with range/sort order
        for (MongoIndex index : indexes) {
            if (!index.keys().isEmpty()) {
                MongodbIndexKey leadingKey = index.keys().get(0);
                if (leadingKey.name().equals("_id") && leadingKey.sortOrder().isPresent()) {
                    return Optional.of("_id");
                }
            }
        }

        // Pass 2 (Secondary Single-Key Indexes): Scan for single-key range indexes
        for (MongoIndex index : indexes) {
            if (index.keys().size() == 1) {
                MongodbIndexKey key = index.keys().get(0);
                if (!key.name().equals("_id") && !key.name().contains(".") && key.sortOrder().isPresent()) {
                    return Optional.of(key.name());
                }
            }
        }

        // Pass 3 (Secondary Compound Indexes): Scan for leading keys of compound range indexes
        for (MongoIndex index : indexes) {
            if (index.keys().size() > 1) {
                MongodbIndexKey leadingKey = index.keys().get(0);
                if (!leadingKey.name().equals("_id") && !leadingKey.name().contains(".") && leadingKey.sortOrder().isPresent()) {
                    return Optional.of(leadingKey.name());
                }
            }
        }

        return Optional.empty();
    }
}
