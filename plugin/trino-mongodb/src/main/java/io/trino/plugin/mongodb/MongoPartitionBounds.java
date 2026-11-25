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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import org.bson.BsonDocument;
import org.bson.BsonInvalidOperationException;
import org.bson.json.JsonMode;
import org.bson.json.JsonParseException;
import org.bson.json.JsonWriterSettings;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public record MongoPartitionBounds(
        @JsonProperty("keyNames") List<String> keyNames,
        @JsonProperty("lowerBound") Optional<String> lowerBound,
        @JsonProperty("upperBound") Optional<String> upperBound,
        @JsonProperty("lowerInclusive") boolean lowerInclusive,
        @JsonProperty("upperInclusive") boolean upperInclusive)
{
    private static final int INSTANCE_SIZE = instanceSize(MongoPartitionBounds.class);

    private static final JsonWriterSettings EXTENDED_JSON_WRITER_SETTINGS = JsonWriterSettings.builder()
            .outputMode(JsonMode.EXTENDED)
            .build();

    @JsonCreator
    public MongoPartitionBounds
    {
        requireNonNull(keyNames, "keyNames is null");
        checkArgument(!keyNames.isEmpty(), "keyNames cannot be empty");
        keyNames = ImmutableList.copyOf(keyNames);

        requireNonNull(lowerBound, "lowerBound is null");
        requireNonNull(upperBound, "upperBound is null");

        // Validate syntax and constructibility if bounds are specified
        lowerBound.ifPresent(MongoPartitionBounds::validateExtendedJson);
        upperBound.ifPresent(MongoPartitionBounds::validateExtendedJson);
    }

    public static MongoPartitionBounds fromBson(
            List<String> keyNames,
            BsonDocument lowerBound,
            boolean lowerInclusive,
            BsonDocument upperBound,
            boolean upperInclusive)
    {
        requireNonNull(lowerBound, "lowerBound is null");
        requireNonNull(upperBound, "upperBound is null");
        return fromBson(
                keyNames,
                Optional.of(lowerBound),
                lowerInclusive,
                Optional.of(upperBound),
                upperInclusive);
    }

    public static MongoPartitionBounds fromBson(
            List<String> keyNames,
            Optional<BsonDocument> lowerBound,
            boolean lowerInclusive,
            Optional<BsonDocument> upperBound,
            boolean upperInclusive)
    {
        requireNonNull(lowerBound, "lowerBound is null");
        requireNonNull(upperBound, "upperBound is null");
        return new MongoPartitionBounds(
                keyNames,
                lowerBound.map(doc -> doc.toJson(EXTENDED_JSON_WRITER_SETTINGS)),
                upperBound.map(doc -> doc.toJson(EXTENDED_JSON_WRITER_SETTINGS)),
                lowerInclusive,
                upperInclusive);
    }

    public Optional<BsonDocument> parseLowerBoundDocument()
    {
        return lowerBound.map(BsonDocument::parse);
    }

    public Optional<BsonDocument> parseUpperBoundDocument()
    {
        return upperBound.map(BsonDocument::parse);
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(keyNames, SizeOf::estimatedSizeOf)
                + sizeOf(lowerBound, SizeOf::estimatedSizeOf)
                + sizeOf(upperBound, SizeOf::estimatedSizeOf);
    }

    private static void validateExtendedJson(String json)
    {
        try {
            BsonDocument.parse(json);
        }
        catch (JsonParseException | BsonInvalidOperationException e) {
            throw new IllegalArgumentException("Invalid BSON Extended JSON document: " + json, e);
        }
    }
}
