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
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record MongoShardedCollectionMetadata(BsonDocument shardKey, Optional<BsonValue> uuid)
{
    public MongoShardedCollectionMetadata
    {
        requireNonNull(shardKey, "shardKey is null");
        requireNonNull(uuid, "uuid is null");
    }

    public List<String> shardKeyFields()
    {
        return ImmutableList.copyOf(shardKey.keySet());
    }

    public boolean hasHashedShardKey()
    {
        return shardKey.values().stream()
                .anyMatch(val -> val.isString() && "hashed".equals(val.asString().getValue()));
    }
}
