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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public record MongoPartitionRange(
        @JsonProperty("partitionKeys") List<String> partitionKeys,
        @JsonProperty("lowBoundJson") Optional<String> lowBoundJson,
        @JsonProperty("lowInclusive") boolean lowInclusive,
        @JsonProperty("highBoundJson") Optional<String> highBoundJson,
        @JsonProperty("highInclusive") boolean highInclusive)
{
    private static final int INSTANCE_SIZE = instanceSize(MongoPartitionRange.class);
    private static final JsonWriterSettings JSON_WRITER_SETTINGS = JsonWriterSettings.builder()
            .outputMode(JsonMode.EXTENDED)
            .build();

    public MongoPartitionRange
    {
        requireNonNull(partitionKeys, "partitionKeys is null");
        requireNonNull(lowBoundJson, "lowBoundJson is null");
        requireNonNull(highBoundJson, "highBoundJson is null");

        if (partitionKeys.isEmpty()) {
            throw new IllegalArgumentException("partitionKeys cannot be empty");
        }
        partitionKeys = ImmutableList.copyOf(partitionKeys);

        lowBoundJson.ifPresent(Document::parse);
        highBoundJson.ifPresent(Document::parse);
    }

    public static MongoPartitionRange fromDocuments(
            List<String> partitionKeys,
            Optional<Document> lowBound,
            boolean lowInclusive,
            Optional<Document> highBound,
            boolean highInclusive)
    {
        requireNonNull(lowBound, "lowBound is null");
        requireNonNull(highBound, "highBound is null");

        return new MongoPartitionRange(
                partitionKeys,
                lowBound.map(doc -> doc.toJson(JSON_WRITER_SETTINGS)),
                lowInclusive,
                highBound.map(doc -> doc.toJson(JSON_WRITER_SETTINGS)),
                highInclusive);
    }

    public Optional<Document> lowBoundDocument()
    {
        return lowBoundJson.map(Document::parse);
    }

    public Optional<Document> highBoundDocument()
    {
        return highBoundJson.map(Document::parse);
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(partitionKeys, SizeOf::estimatedSizeOf)
                + sizeOf(lowBoundJson, SizeOf::estimatedSizeOf)
                + sizeOf(highBoundJson, SizeOf::estimatedSizeOf);
    }
}
