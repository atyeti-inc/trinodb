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

public enum MongoSplitStrategy
{
    /**
     * Automatically determine split strategy: chunk-based for sharded collections,
     * range-based for non-sharded collections.
     */
    AUTOMATIC,

    /**
     * Partition sharded collections using cluster chunk metadata from config.chunks.
     */
    CHUNK,

    /**
     * Partition non-sharded collections using key range boundaries derived from indexed fields.
     */
    RANGE,
}
