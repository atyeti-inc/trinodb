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

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDbPointer;
import org.bson.BsonDocument;
import org.bson.BsonJavaScript;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonType;
import org.bson.BsonValue;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class BsonValueComparator
        implements Comparator<BsonValue>, Serializable
{
    public static final BsonValueComparator BSON_VALUE_COMPARATOR = new BsonValueComparator();

    @Override
    public int compare(BsonValue v1, BsonValue v2)
    {
        requireNonNull(v1, "v1 is null");
        requireNonNull(v2, "v2 is null");

        if (v1 == v2) {
            return 0;
        }

        int typeOrder1 = typeOrder(v1.getBsonType());
        int typeOrder2 = typeOrder(v2.getBsonType());

        if (typeOrder1 != typeOrder2) {
            return Integer.compare(typeOrder1, typeOrder2);
        }

        return switch (typeOrder1) {
            case 1 -> 0; // MIN_KEY
            case 2 -> 0; // NULL, UNDEFINED
            case 3 -> compareNumbers((BsonNumber) v1, (BsonNumber) v2);
            case 4 -> compareStrings(v1, v2);
            case 5 -> compareDocuments(v1.asDocument(), v2.asDocument());
            case 6 -> compareArrays(v1.asArray(), v2.asArray());
            case 7 -> compareBinary(v1.asBinary(), v2.asBinary());
            case 8 -> v1.asObjectId().compareTo(v2.asObjectId());
            case 9 -> Boolean.compare(v1.asBoolean().getValue(), v2.asBoolean().getValue());
            case 10 -> Long.compare(v1.asDateTime().getValue(), v2.asDateTime().getValue());
            case 11 -> Long.compareUnsigned(v1.asTimestamp().getValue(), v2.asTimestamp().getValue());
            case 12 -> compareRegex(v1, v2);
            case 13 -> compareJavaScript(v1, v2);
            case 14 -> compareDbPointer(v1, v2);
            case 15 -> 0; // MAX_KEY
            default -> throw new IllegalArgumentException("Unknown type order: " + typeOrder1);
        };
    }

    private static int typeOrder(BsonType type)
    {
        return switch (type) {
            case MIN_KEY -> 1;
            case NULL, UNDEFINED -> 2;
            case INT32, INT64, DOUBLE, DECIMAL128 -> 3;
            case SYMBOL, STRING -> 4;
            case DOCUMENT -> 5;
            case ARRAY -> 6;
            case BINARY -> 7;
            case OBJECT_ID -> 8;
            case BOOLEAN -> 9;
            case DATE_TIME -> 10;
            case TIMESTAMP -> 11;
            case REGULAR_EXPRESSION -> 12;
            case JAVASCRIPT, JAVASCRIPT_WITH_SCOPE -> 13;
            case DB_POINTER -> 14;
            case MAX_KEY -> 15;
            case END_OF_DOCUMENT -> throw new IllegalArgumentException("Unexpected END_OF_DOCUMENT type");
        };
    }

    private static int compareNumbers(BsonNumber n1, BsonNumber n2)
    {
        if (n1.getBsonType() == n2.getBsonType()) {
            return switch (n1.getBsonType()) {
                case INT32 -> Integer.compare(n1.asInt32().getValue(), n2.asInt32().getValue());
                case INT64 -> Long.compare(n1.asInt64().getValue(), n2.asInt64().getValue());
                case DOUBLE -> Double.compare(n1.asDouble().getValue(), n2.asDouble().getValue());
                case DECIMAL128 -> n1.asDecimal128().getValue().compareTo(n2.asDecimal128().getValue());
                default -> throw new IllegalStateException("Unexpected numeric type: " + n1.getBsonType());
            };
        }
        if ((n1.getBsonType() == BsonType.INT32 || n1.getBsonType() == BsonType.INT64)
                && (n2.getBsonType() == BsonType.INT32 || n2.getBsonType() == BsonType.INT64)) {
            return Long.compare(n1.longValue(), n2.longValue());
        }
        return n1.decimal128Value().compareTo(n2.decimal128Value());
    }

    private static int compareStrings(BsonValue v1, BsonValue v2)
    {
        return stringValue(v1).compareTo(stringValue(v2));
    }

    private static String stringValue(BsonValue value)
    {
        if (value instanceof BsonString str) {
            return str.getValue();
        }
        if (value instanceof BsonSymbol sym) {
            return sym.getSymbol();
        }
        throw new IllegalArgumentException("Expected string or symbol: " + value);
    }

    private int compareDocuments(BsonDocument doc1, BsonDocument doc2)
    {
        Iterator<Map.Entry<String, BsonValue>> iter1 = doc1.entrySet().iterator();
        Iterator<Map.Entry<String, BsonValue>> iter2 = doc2.entrySet().iterator();
        while (iter1.hasNext() && iter2.hasNext()) {
            Map.Entry<String, BsonValue> entry1 = iter1.next();
            Map.Entry<String, BsonValue> entry2 = iter2.next();
            int keyCompare = entry1.getKey().compareTo(entry2.getKey());
            if (keyCompare != 0) {
                return keyCompare;
            }
            int valueCompare = compare(entry1.getValue(), entry2.getValue());
            if (valueCompare != 0) {
                return valueCompare;
            }
        }
        return Boolean.compare(iter1.hasNext(), iter2.hasNext());
    }

    private int compareArrays(BsonArray arr1, BsonArray arr2)
    {
        int minLen = Math.min(arr1.size(), arr2.size());
        for (int i = 0; i < minLen; i++) {
            int cmp = compare(arr1.get(i), arr2.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(arr1.size(), arr2.size());
    }

    private static int compareBinary(BsonBinary b1, BsonBinary b2)
    {
        int dataCompare = Arrays.compareUnsigned(b1.getData(), b2.getData());
        if (dataCompare != 0) {
            return dataCompare;
        }
        return Byte.compare(b1.getType(), b2.getType());
    }

    private static int compareRegex(BsonValue v1, BsonValue v2)
    {
        int patternCompare = v1.asRegularExpression().getPattern().compareTo(v2.asRegularExpression().getPattern());
        if (patternCompare != 0) {
            return patternCompare;
        }
        return v1.asRegularExpression().getOptions().compareTo(v2.asRegularExpression().getOptions());
    }

    private int compareJavaScript(BsonValue v1, BsonValue v2)
    {
        String code1 = v1 instanceof BsonJavaScript js ? js.getCode() : v1.asJavaScriptWithScope().getCode();
        String code2 = v2 instanceof BsonJavaScript js ? js.getCode() : v2.asJavaScriptWithScope().getCode();
        int codeCompare = code1.compareTo(code2);
        if (codeCompare != 0) {
            return codeCompare;
        }
        if (v1.getBsonType() != v2.getBsonType()) {
            return v1.getBsonType() == BsonType.JAVASCRIPT ? -1 : 1;
        }
        if (v1.getBsonType() == BsonType.JAVASCRIPT_WITH_SCOPE) {
            return compareDocuments(v1.asJavaScriptWithScope().getScope(), v2.asJavaScriptWithScope().getScope());
        }
        return 0;
    }

    private static int compareDbPointer(BsonValue v1, BsonValue v2)
    {
        BsonDbPointer ptr1 = (BsonDbPointer) v1;
        BsonDbPointer ptr2 = (BsonDbPointer) v2;
        int nsCompare = ptr1.getNamespace().compareTo(ptr2.getNamespace());
        if (nsCompare != 0) {
            return nsCompare;
        }
        return ptr1.getId().compareTo(ptr2.getId());
    }
}
