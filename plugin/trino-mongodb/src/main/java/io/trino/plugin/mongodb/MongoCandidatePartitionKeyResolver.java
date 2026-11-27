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

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class MongoCandidatePartitionKeyResolver
{
    private static final String ID_FIELD = "_id";

    private MongoCandidatePartitionKeyResolver() {}

    public static Optional<String> resolveKey(List<MongoIndex> indexes)
    {
        requireNonNull(indexes, "indexes is null");

        // 1. Check for range index on _id
        for (MongoIndex index : indexes) {
            List<MongoIndex.MongodbIndexKey> keys = index.keys();
            if (!keys.isEmpty() && keys.get(0).name().equals(ID_FIELD) && keys.get(0).sortOrder().isPresent()) {
                return Optional.of(ID_FIELD);
            }
        }

        // 2. If _id is missing or hashed, check for secondary indexes with range sortOrder
        // First look for single-key range indexes
        for (MongoIndex index : indexes) {
            List<MongoIndex.MongodbIndexKey> keys = index.keys();
            if (keys.size() == 1 && !keys.get(0).name().equals(ID_FIELD) && keys.get(0).sortOrder().isPresent()) {
                return Optional.of(keys.get(0).name());
            }
        }

        // Next look for compound range indexes, picking the leading key if it has a sortOrder
        for (MongoIndex index : indexes) {
            List<MongoIndex.MongodbIndexKey> keys = index.keys();
            if (!keys.isEmpty() && !keys.get(0).name().equals(ID_FIELD) && keys.get(0).sortOrder().isPresent()) {
                return Optional.of(keys.get(0).name());
            }
        }

        return Optional.empty();
    }
}
