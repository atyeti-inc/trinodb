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
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

public class BsonValueComparator
        implements Comparator<BsonValue>
{
    public static final BsonValueComparator BSON_VALUE_COMPARATOR = new BsonValueComparator();

    @Override
    public int compare(BsonValue v1, BsonValue v2)
    {
        if (v1 == v2) {
            return 0;
        }
        if (v1 == null) {
            return -1;
        }
        if (v2 == null) {
            return 1;
        }

        int order1 = getTypeOrder(v1);
        int order2 = getTypeOrder(v2);

        if (order1 != order2) {
            return Integer.compare(order1, order2);
        }

        return switch (order1) {
            case 1 -> 0; // MinKey
            case 2 -> 0; // Null / Undefined
            case 3 -> compareNumbers(v1, v2);
            case 4 -> compareStrings(v1, v2);
            case 5 -> compareDocuments(v1, v2);
            case 6 -> compareArrays(v1, v2);
            case 7 -> compareBinary(v1.asBinary(), v2.asBinary());
            case 8 -> v1.asObjectId().getValue().compareTo(v2.asObjectId().getValue());
            case 9 -> Boolean.compare(v1.asBoolean().getValue(), v2.asBoolean().getValue());
            case 10 -> Long.compare(v1.asDateTime().getValue(), v2.asDateTime().getValue());
            case 11 -> compareTimestamps(v1.asTimestamp(), v2.asTimestamp());
            case 12 -> compareRegex(v1.asRegularExpression(), v2.asRegularExpression());
            case 13 -> compareJavaScript(v1, v2);
            case 14 -> 0; // MaxKey
            default -> throw new IllegalArgumentException("Unsupported BSON type order: " + order1);
        };
    }

    private static int getTypeOrder(BsonValue value)
    {
        BsonType type = value.getBsonType();
        return switch (type) {
            case MIN_KEY -> 1;
            case NULL, UNDEFINED -> 2;
            case INT32, INT64, DOUBLE, DECIMAL128 -> 3;
            case SYMBOL, STRING -> 4;
            case DOCUMENT, DB_POINTER -> 5;
            case ARRAY -> 6;
            case BINARY -> 7;
            case OBJECT_ID -> 8;
            case BOOLEAN -> 9;
            case DATE_TIME -> 10;
            case TIMESTAMP -> 11;
            case REGULAR_EXPRESSION -> 12;
            case JAVASCRIPT, JAVASCRIPT_WITH_SCOPE -> 13;
            case MAX_KEY -> 14;
            default -> throw new IllegalArgumentException("Unsupported BSON type: " + type);
        };
    }

    private enum NumberCategory
    {
        NAN,
        NEGATIVE_INFINITY,
        FINITE,
        POSITIVE_INFINITY,
    }

    private record NumberClassification(NumberCategory category, BigDecimal value) {}

    private static NumberClassification classifyNumber(BsonValue value)
    {
        if (value.isInt32()) {
            return new NumberClassification(NumberCategory.FINITE, BigDecimal.valueOf(value.asInt32().getValue()));
        }
        if (value.isInt64()) {
            return new NumberClassification(NumberCategory.FINITE, BigDecimal.valueOf(value.asInt64().getValue()));
        }
        if (value.isDouble()) {
            double d = value.asDouble().getValue();
            if (Double.isNaN(d)) {
                return new NumberClassification(NumberCategory.NAN, null);
            }
            if (Double.isInfinite(d)) {
                return new NumberClassification(d < 0 ? NumberCategory.NEGATIVE_INFINITY : NumberCategory.POSITIVE_INFINITY, null);
            }
            return new NumberClassification(NumberCategory.FINITE, BigDecimal.valueOf(d));
        }
        if (value.isDecimal128()) {
            Decimal128 dec = value.asDecimal128().getValue();
            if (dec.isNaN()) {
                return new NumberClassification(NumberCategory.NAN, null);
            }
            if (dec.isInfinite()) {
                return new NumberClassification(dec.isNegative() ? NumberCategory.NEGATIVE_INFINITY : NumberCategory.POSITIVE_INFINITY, null);
            }
            return new NumberClassification(NumberCategory.FINITE, dec.bigDecimalValue());
        }
        throw new IllegalArgumentException("Not a numeric BSON value: " + value);
    }

    private static int compareNumbers(BsonValue v1, BsonValue v2)
    {
        NumberClassification c1 = classifyNumber(v1);
        NumberClassification c2 = classifyNumber(v2);

        int catComp = c1.category().compareTo(c2.category());
        if (catComp != 0) {
            return catComp;
        }
        if (c1.category() == NumberCategory.FINITE) {
            return c1.value().compareTo(c2.value());
        }
        return 0;
    }

    private static int compareStrings(BsonValue v1, BsonValue v2)
    {
        String s1 = v1.isString() ? v1.asString().getValue() : v1.asSymbol().getSymbol();
        String s2 = v2.isString() ? v2.asString().getValue() : v2.asSymbol().getSymbol();
        return s1.compareTo(s2);
    }

    private int compareDocuments(BsonValue v1, BsonValue v2)
    {
        if (v1 instanceof BsonDbPointer p1 && v2 instanceof BsonDbPointer p2) {
            int nsComp = p1.getNamespace().compareTo(p2.getNamespace());
            if (nsComp != 0) {
                return nsComp;
            }
            return p1.getId().compareTo(p2.getId());
        }
        if (v1 instanceof BsonDbPointer) {
            return -1;
        }
        if (v2 instanceof BsonDbPointer) {
            return 1;
        }

        BsonDocument doc1 = v1.asDocument();
        BsonDocument doc2 = v2.asDocument();
        Iterator<Map.Entry<String, BsonValue>> it1 = doc1.entrySet().iterator();
        Iterator<Map.Entry<String, BsonValue>> it2 = doc2.entrySet().iterator();
        while (it1.hasNext() && it2.hasNext()) {
            Map.Entry<String, BsonValue> e1 = it1.next();
            Map.Entry<String, BsonValue> e2 = it2.next();
            int keyComp = e1.getKey().compareTo(e2.getKey());
            if (keyComp != 0) {
                return keyComp;
            }
            int valueComp = compare(e1.getValue(), e2.getValue());
            if (valueComp != 0) {
                return valueComp;
            }
        }
        return Boolean.compare(it1.hasNext(), it2.hasNext());
    }

    private int compareArrays(BsonValue v1, BsonValue v2)
    {
        BsonArray a1 = v1.asArray();
        BsonArray a2 = v2.asArray();
        int minSize = Math.min(a1.size(), a2.size());
        for (int i = 0; i < minSize; i++) {
            int comp = compare(a1.get(i), a2.get(i));
            if (comp != 0) {
                return comp;
            }
        }
        return Integer.compare(a1.size(), a2.size());
    }

    private static int compareBinary(BsonBinary b1, BsonBinary b2)
    {
        byte[] d1 = b1.getData();
        byte[] d2 = b2.getData();
        int lenComp = Integer.compare(d1.length, d2.length);
        if (lenComp != 0) {
            return lenComp;
        }
        int typeComp = Integer.compare(b1.getType() & 0xFF, b2.getType() & 0xFF);
        if (typeComp != 0) {
            return typeComp;
        }
        for (int i = 0; i < d1.length; i++) {
            int byteComp = Integer.compare(d1[i] & 0xFF, d2[i] & 0xFF);
            if (byteComp != 0) {
                return byteComp;
            }
        }
        return 0;
    }

    private static int compareTimestamps(BsonTimestamp t1, BsonTimestamp t2)
    {
        int timeComp = Integer.compareUnsigned(t1.getTime(), t2.getTime());
        if (timeComp != 0) {
            return timeComp;
        }
        return Integer.compareUnsigned(t1.getInc(), t2.getInc());
    }

    private static int compareRegex(BsonRegularExpression r1, BsonRegularExpression r2)
    {
        int patComp = r1.getPattern().compareTo(r2.getPattern());
        if (patComp != 0) {
            return patComp;
        }
        return r1.getOptions().compareTo(r2.getOptions());
    }

    private int compareJavaScript(BsonValue v1, BsonValue v2)
    {
        String c1 = v1.isJavaScript() ? v1.asJavaScript().getCode() : v1.asJavaScriptWithScope().getCode();
        String c2 = v2.isJavaScript() ? v2.asJavaScript().getCode() : v2.asJavaScriptWithScope().getCode();
        int codeComp = c1.compareTo(c2);
        if (codeComp != 0) {
            return codeComp;
        }
        if (v1.isJavaScriptWithScope() && v2.isJavaScriptWithScope()) {
            return compare(v1.asJavaScriptWithScope().getScope(), v2.asJavaScriptWithScope().getScope());
        }
        return Boolean.compare(v1.isJavaScriptWithScope(), v2.isJavaScriptWithScope());
    }
}
