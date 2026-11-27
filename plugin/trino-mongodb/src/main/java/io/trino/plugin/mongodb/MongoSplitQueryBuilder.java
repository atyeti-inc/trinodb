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
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class MongoSplitQueryBuilder
{
    private static final String AND_OP = "$and";
    private static final String OR_OP = "$or";
    private static final String GTE_OP = "$gte";
    private static final String GT_OP = "$gt";
    private static final String LTE_OP = "$lte";
    private static final String LT_OP = "$lt";

    private MongoSplitQueryBuilder() {}

    public static Optional<Document> buildSplitFilter(MongoPartitionBounds bounds)
    {
        requireNonNull(bounds, "bounds is null");
        List<String> keyNames = bounds.keyNames();
        if (keyNames.size() == 1) {
            return buildSingleKeyFilter(
                    keyNames.get(0),
                    bounds.parseLowerBoundDocument(),
                    bounds.lowerInclusive(),
                    bounds.parseUpperBoundDocument(),
                    bounds.upperInclusive());
        }
        return buildCompoundKeyFilter(bounds);
    }

    private static Optional<Document> buildSingleKeyFilter(
            String keyName,
            Optional<BsonDocument> lowerDocOpt,
            boolean lowerInclusive,
            Optional<BsonDocument> upperDocOpt,
            boolean upperInclusive)
    {
        BsonValue lowerValue = lowerDocOpt.map(doc -> doc.get(keyName)).orElse(null);
        BsonValue upperValue = upperDocOpt.map(doc -> doc.get(keyName)).orElse(null);

        boolean hasLower = lowerValue != null && !isMinKey(lowerValue);
        boolean hasUpper = upperValue != null && !isMaxKey(upperValue);

        if (!hasLower && !hasUpper) {
            return Optional.empty();
        }

        Document rangeDoc = new Document();
        if (hasLower) {
            rangeDoc.append(lowerInclusive ? GTE_OP : GT_OP, toFilterValue(lowerValue));
        }
        if (hasUpper) {
            rangeDoc.append(upperInclusive ? LTE_OP : LT_OP, toFilterValue(upperValue));
        }

        return Optional.of(new Document(keyName, rangeDoc));
    }

    private static Optional<Document> buildCompoundKeyFilter(MongoPartitionBounds bounds)
    {
        List<String> keyNames = bounds.keyNames();
        int n = keyNames.size();
        Optional<BsonDocument> lowerDocOpt = bounds.parseLowerBoundDocument();
        Optional<BsonDocument> upperDocOpt = bounds.parseUpperBoundDocument();

        int p = 0;
        if (lowerDocOpt.isPresent() && upperDocOpt.isPresent()) {
            BsonDocument lowerDoc = lowerDocOpt.get();
            BsonDocument upperDoc = upperDocOpt.get();
            while (p < n) {
                String key = keyNames.get(p);
                BsonValue lowerVal = lowerDoc.get(key);
                BsonValue upperVal = upperDoc.get(key);
                if (lowerVal == null || upperVal == null || isSentinel(lowerVal) || isSentinel(upperVal)) {
                    break;
                }
                if (!lowerVal.equals(upperVal)) {
                    break;
                }
                p++;
            }
        }

        Document prefixDoc = new Document();
        if (p > 0) {
            BsonDocument lowerDoc = lowerDocOpt.get();
            for (int i = 0; i < p; i++) {
                String key = keyNames.get(i);
                prefixDoc.append(key, toFilterValue(lowerDoc.get(key)));
            }
        }

        if (p == n - 1) {
            String lastKey = keyNames.get(n - 1);
            BsonValue lowerVal = lowerDocOpt.map(doc -> doc.get(lastKey)).orElse(null);
            BsonValue upperVal = upperDocOpt.map(doc -> doc.get(lastKey)).orElse(null);

            boolean hasLower = lowerVal != null && !isMinKey(lowerVal);
            boolean hasUpper = upperVal != null && !isMaxKey(upperVal);

            if (hasLower || hasUpper) {
                Document rangeDoc = new Document();
                if (hasLower) {
                    rangeDoc.append(bounds.lowerInclusive() ? GTE_OP : GT_OP, toFilterValue(lowerVal));
                }
                if (hasUpper) {
                    rangeDoc.append(bounds.upperInclusive() ? LTE_OP : LT_OP, toFilterValue(upperVal));
                }
                prefixDoc.append(lastKey, rangeDoc);
            }

            return prefixDoc.isEmpty() ? Optional.empty() : Optional.of(prefixDoc);
        }

        List<String> remainingKeys = keyNames.subList(p, n);
        Optional<Document> lowerFilter = lowerDocOpt.flatMap(doc ->
                buildLexicographicalLowerFilter(remainingKeys, doc, bounds.lowerInclusive()));
        Optional<Document> upperFilter = upperDocOpt.flatMap(doc ->
                buildLexicographicalUpperFilter(remainingKeys, doc, bounds.upperInclusive()));

        List<Document> conjunctions = new ArrayList<>();
        if (!prefixDoc.isEmpty()) {
            conjunctions.add(prefixDoc);
        }
        lowerFilter.ifPresent(conjunctions::add);
        upperFilter.ifPresent(conjunctions::add);

        if (conjunctions.isEmpty()) {
            return Optional.empty();
        }
        if (conjunctions.size() == 1) {
            return Optional.of(conjunctions.get(0));
        }
        return Optional.of(new Document(AND_OP, conjunctions));
    }

    private static Optional<Document> buildLexicographicalLowerFilter(
            List<String> keys,
            BsonDocument doc,
            boolean lowerInclusive)
    {
        int m = keys.size();
        while (m > 0) {
            BsonValue value = doc.get(keys.get(m - 1));
            if (value != null && isMinKey(value)) {
                m--;
            }
            else {
                break;
            }
        }
        if (m == 0) {
            return Optional.empty();
        }

        List<Document> branches = new ArrayList<>();
        for (int i = 0; i < m; i++) {
            Document branch = new Document();
            for (int j = 0; j < i; j++) {
                String prevKey = keys.get(j);
                BsonValue prevVal = requireNonNull(doc.get(prevKey), () -> "Bound value is null for key: " + prevKey);
                branch.append(prevKey, toFilterValue(prevVal));
            }
            String currentKey = keys.get(i);
            String op;
            if (i < m - 1) {
                op = GT_OP;
            }
            else {
                op = (lowerInclusive || m < keys.size()) ? GTE_OP : GT_OP;
            }
            BsonValue currentVal = requireNonNull(doc.get(currentKey), () -> "Bound value is null for key: " + currentKey);
            branch.append(currentKey, new Document(op, toFilterValue(currentVal)));
            branches.add(branch);
        }

        if (branches.size() == 1) {
            return Optional.of(branches.get(0));
        }
        return Optional.of(new Document(OR_OP, branches));
    }

    private static Optional<Document> buildLexicographicalUpperFilter(
            List<String> keys,
            BsonDocument doc,
            boolean upperInclusive)
    {
        int m = keys.size();
        while (m > 0) {
            BsonValue value = doc.get(keys.get(m - 1));
            if (value != null && isMaxKey(value)) {
                m--;
            }
            else {
                break;
            }
        }
        if (m == 0) {
            return Optional.empty();
        }

        List<Document> branches = new ArrayList<>();
        for (int i = 0; i < m; i++) {
            Document branch = new Document();
            for (int j = 0; j < i; j++) {
                String prevKey = keys.get(j);
                BsonValue prevVal = requireNonNull(doc.get(prevKey), () -> "Bound value is null for key: " + prevKey);
                branch.append(prevKey, toFilterValue(prevVal));
            }
            String currentKey = keys.get(i);
            String op;
            if (i < m - 1) {
                op = LT_OP;
            }
            else {
                op = (upperInclusive || m < keys.size()) ? LTE_OP : LT_OP;
            }
            BsonValue currentVal = requireNonNull(doc.get(currentKey), () -> "Bound value is null for key: " + currentKey);
            branch.append(currentKey, new Document(op, toFilterValue(currentVal)));
            branches.add(branch);
        }

        if (branches.size() == 1) {
            return Optional.of(branches.get(0));
        }
        return Optional.of(new Document(OR_OP, branches));
    }

    private static Object toFilterValue(BsonValue bsonValue)
    {
        requireNonNull(bsonValue, "bsonValue is null");
        if (bsonValue.isNull()) {
            return null;
        }
        if (bsonValue instanceof BsonObjectId bsonObjectId) {
            return bsonObjectId.getValue();
        }
        if (bsonValue instanceof BsonInt32 bsonInt32) {
            return bsonInt32.getValue();
        }
        if (bsonValue instanceof BsonInt64 bsonInt64) {
            return bsonInt64.getValue();
        }
        if (bsonValue instanceof BsonDouble bsonDouble) {
            return bsonDouble.getValue();
        }
        if (bsonValue instanceof BsonDecimal128 bsonDecimal128) {
            return bsonDecimal128.getValue();
        }
        if (bsonValue instanceof BsonString bsonString) {
            return bsonString.getValue();
        }
        if (bsonValue instanceof BsonBoolean bsonBoolean) {
            return bsonBoolean.getValue();
        }
        if (bsonValue instanceof BsonDateTime bsonDateTime) {
            return new Date(bsonDateTime.getValue());
        }
        if (bsonValue instanceof BsonBinary bsonBinary) {
            return new Binary(bsonBinary.getType(), bsonBinary.getData());
        }
        if (bsonValue instanceof BsonMinKey) {
            return new MinKey();
        }
        if (bsonValue instanceof BsonMaxKey) {
            return new MaxKey();
        }
        throw new IllegalArgumentException("Unsupported BSON value type: " + bsonValue.getClass().getName());
    }

    private static boolean isSentinel(BsonValue bsonValue)
    {
        return isMinKey(bsonValue) || isMaxKey(bsonValue);
    }

    private static boolean isMinKey(BsonValue bsonValue)
    {
        return bsonValue instanceof BsonMinKey;
    }

    private static boolean isMaxKey(BsonValue bsonValue)
    {
        return bsonValue instanceof BsonMaxKey;
    }
}
