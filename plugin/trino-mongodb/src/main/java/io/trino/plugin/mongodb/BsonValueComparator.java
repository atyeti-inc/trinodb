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
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonValue;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

public final class BsonValueComparator
        implements Comparator<BsonValue>, Serializable
{
    public static final BsonValueComparator INSTANCE = new BsonValueComparator();

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

        int order1 = getTypeOrder(v1.getBsonType());
        int order2 = getTypeOrder(v2.getBsonType());

        if (order1 != order2) {
            return Integer.compare(order1, order2);
        }

        return switch (order1) {
            case 1 -> 0; // MIN_KEY
            case 2 -> 0; // NULL, UNDEFINED
            case 3 -> compareNumbers(v1, v2);
            case 4 -> compareStrings(v1, v2);
            case 5 -> compareDocuments(v1.asDocument(), v2.asDocument());
            case 6 -> compareArrays(v1.asArray(), v2.asArray());
            case 7 -> compareBinary(v1.asBinary(), v2.asBinary());
            case 8 -> compareObjectId(v1.asObjectId(), v2.asObjectId());
            case 9 -> Boolean.compare(v1.asBoolean().getValue(), v2.asBoolean().getValue());
            case 10 -> Long.compare(v1.asDateTime().getValue(), v2.asDateTime().getValue());
            case 11 -> compareTimestamps(v1.asTimestamp(), v2.asTimestamp());
            case 12 -> compareRegex(v1.asRegularExpression(), v2.asRegularExpression());
            case 13 -> compareJavaScript(v1, v2);
            case 14 -> 0; // MAX_KEY
            default -> 0;
        };
    }

    private static int getTypeOrder(BsonType type)
    {
        return switch (type) {
            case MIN_KEY -> 1;
            case NULL, UNDEFINED -> 2;
            case DOUBLE, INT32, INT64, DECIMAL128 -> 3;
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
            case MAX_KEY -> 14;
            default -> 100;
        };
    }

    private static int compareNumbers(BsonValue v1, BsonValue v2)
    {
        boolean nan1 = isNaN(v1);
        boolean nan2 = isNaN(v2);
        if (nan1 || nan2) {
            if (nan1 && nan2) {
                return 0;
            }
            return nan1 ? -1 : 1;
        }

        boolean negInf1 = isNegativeInfinity(v1);
        boolean negInf2 = isNegativeInfinity(v2);
        if (negInf1 || negInf2) {
            if (negInf1 && negInf2) {
                return 0;
            }
            return negInf1 ? -1 : 1;
        }

        boolean posInf1 = isPositiveInfinity(v1);
        boolean posInf2 = isPositiveInfinity(v2);
        if (posInf1 || posInf2) {
            if (posInf1 && posInf2) {
                return 0;
            }
            return posInf1 ? 1 : -1;
        }

        BigDecimal bd1 = toBigDecimal(v1);
        BigDecimal bd2 = toBigDecimal(v2);
        return bd1.compareTo(bd2);
    }

    private static boolean isNaN(BsonValue v)
    {
        if (v.isDouble()) {
            return Double.isNaN(v.asDouble().getValue());
        }
        if (v.isDecimal128()) {
            return v.asDecimal128().getValue().isNaN();
        }
        return false;
    }

    private static boolean isNegativeInfinity(BsonValue v)
    {
        if (v.isDouble()) {
            return v.asDouble().getValue() == Double.NEGATIVE_INFINITY;
        }
        if (v.isDecimal128()) {
            return v.asDecimal128().getValue().isNegative() && v.asDecimal128().getValue().isInfinite();
        }
        return false;
    }

    private static boolean isPositiveInfinity(BsonValue v)
    {
        if (v.isDouble()) {
            return v.asDouble().getValue() == Double.POSITIVE_INFINITY;
        }
        if (v.isDecimal128()) {
            return !v.asDecimal128().getValue().isNegative() && v.asDecimal128().getValue().isInfinite();
        }
        return false;
    }

    private static BigDecimal toBigDecimal(BsonValue v)
    {
        return switch (v.getBsonType()) {
            case INT32 -> BigDecimal.valueOf(v.asInt32().getValue());
            case INT64 -> BigDecimal.valueOf(v.asInt64().getValue());
            case DOUBLE -> BigDecimal.valueOf(v.asDouble().getValue());
            case DECIMAL128 -> v.asDecimal128().getValue().bigDecimalValue();
            default -> throw new IllegalArgumentException("Unsupported numeric BSON type: " + v.getBsonType());
        };
    }

    private static int compareStrings(BsonValue v1, BsonValue v2)
    {
        return getStringValue(v1).compareTo(getStringValue(v2));
    }

    private static String getStringValue(BsonValue v)
    {
        if (v.isString()) {
            return v.asString().getValue();
        }
        if (v.isSymbol()) {
            return v.asSymbol().getSymbol();
        }
        throw new IllegalArgumentException("Unexpected string BSON type: " + v.getBsonType());
    }

    private static int compareDocuments(BsonDocument d1, BsonDocument d2)
    {
        Iterator<Map.Entry<String, BsonValue>> it1 = d1.entrySet().iterator();
        Iterator<Map.Entry<String, BsonValue>> it2 = d2.entrySet().iterator();

        while (it1.hasNext() && it2.hasNext()) {
            Map.Entry<String, BsonValue> e1 = it1.next();
            Map.Entry<String, BsonValue> e2 = it2.next();
            int keyCmp = e1.getKey().compareTo(e2.getKey());
            if (keyCmp != 0) {
                return keyCmp;
            }
            int valCmp = INSTANCE.compare(e1.getValue(), e2.getValue());
            if (valCmp != 0) {
                return valCmp;
            }
        }
        return Integer.compare(d1.size(), d2.size());
    }

    private static int compareArrays(BsonArray a1, BsonArray a2)
    {
        int minLen = Math.min(a1.size(), a2.size());
        for (int i = 0; i < minLen; i++) {
            int cmp = INSTANCE.compare(a1.get(i), a2.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(a1.size(), a2.size());
    }

    private static int compareBinary(BsonBinary b1, BsonBinary b2)
    {
        int lenCmp = Integer.compare(b1.getData().length, b2.getData().length);
        if (lenCmp != 0) {
            return lenCmp;
        }
        int typeCmp = Byte.compareUnsigned(b1.getType(), b2.getType());
        if (typeCmp != 0) {
            return typeCmp;
        }
        return Arrays.compareUnsigned(b1.getData(), b2.getData());
    }

    private static int compareObjectId(BsonObjectId o1, BsonObjectId o2)
    {
        return o1.getValue().compareTo(o2.getValue());
    }

    private static int compareTimestamps(BsonTimestamp t1, BsonTimestamp t2)
    {
        int timeCmp = Integer.compareUnsigned(t1.getTime(), t2.getTime());
        if (timeCmp != 0) {
            return timeCmp;
        }
        return Integer.compareUnsigned(t1.getInc(), t2.getInc());
    }

    private static int compareRegex(BsonRegularExpression r1, BsonRegularExpression r2)
    {
        int patCmp = r1.getPattern().compareTo(r2.getPattern());
        if (patCmp != 0) {
            return patCmp;
        }
        return r1.getOptions().compareTo(r2.getOptions());
    }

    private static int compareJavaScript(BsonValue v1, BsonValue v2)
    {
        String code1 = getJsCode(v1);
        String code2 = getJsCode(v2);
        int codeCmp = code1.compareTo(code2);
        if (codeCmp != 0) {
            return codeCmp;
        }
        if (v1.isJavaScriptWithScope() && v2.isJavaScriptWithScope()) {
            return compareDocuments(v1.asJavaScriptWithScope().getScope(), v2.asJavaScriptWithScope().getScope());
        }
        if (v1.isJavaScriptWithScope()) {
            return 1;
        }
        if (v2.isJavaScriptWithScope()) {
            return -1;
        }
        return 0;
    }

    private static String getJsCode(BsonValue v)
    {
        if (v.isJavaScript()) {
            return v.asJavaScript().getCode();
        }
        if (v.isJavaScriptWithScope()) {
            return v.asJavaScriptWithScope().getCode();
        }
        throw new IllegalArgumentException("Unexpected JavaScript BSON type: " + v.getBsonType());
    }
}
