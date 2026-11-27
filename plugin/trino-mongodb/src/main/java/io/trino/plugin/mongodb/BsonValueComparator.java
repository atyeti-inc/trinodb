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
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonNumber;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonValue;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

public final class BsonValueComparator
        implements Comparator<BsonValue>
{
    public static final BsonValueComparator INSTANCE = new BsonValueComparator();

    private BsonValueComparator() {}

    @Override
    public int compare(BsonValue o1, BsonValue o2)
    {
        if (o1 == o2) {
            return 0;
        }
        if (o1 == null) {
            return -1;
        }
        if (o2 == null) {
            return 1;
        }

        int typeOrder1 = getTypeOrder(o1.getBsonType());
        int typeOrder2 = getTypeOrder(o2.getBsonType());

        if (typeOrder1 != typeOrder2) {
            return Integer.compare(typeOrder1, typeOrder2);
        }

        // Both values belong to the same type category
        if (isNumeric(o1.getBsonType())) {
            return compareNumbers((BsonNumber) o1, (BsonNumber) o2);
        }

        BsonType type = o1.getBsonType();
        return switch (type) {
            case MIN_KEY, MAX_KEY, NULL, UNDEFINED -> 0;
            case STRING -> o1.asString().getValue().compareTo(o2.asString().getValue());
            case SYMBOL -> o1.asSymbol().getSymbol().compareTo(o2.asSymbol().getSymbol());
            case OBJECT_ID -> o1.asObjectId().getValue().compareTo(o2.asObjectId().getValue());
            case BOOLEAN -> Boolean.compare(o1.asBoolean().getValue(), o2.asBoolean().getValue());
            case DATE_TIME -> Long.compare(o1.asDateTime().getValue(), o2.asDateTime().getValue());
            case TIMESTAMP -> compareTimestamps(o1.asTimestamp(), o2.asTimestamp());
            case REGULAR_EXPRESSION -> compareRegex(o1.asRegularExpression(), o2.asRegularExpression());
            case JAVASCRIPT -> o1.asJavaScript().getCode().compareTo(o2.asJavaScript().getCode());
            case JAVASCRIPT_WITH_SCOPE -> compareJavaScriptWithScope(o1.asJavaScriptWithScope(), o2.asJavaScriptWithScope());
            case BINARY -> compareBinary(o1.asBinary(), o2.asBinary());
            case ARRAY -> compareArray(o1.asArray(), o2.asArray());
            case DOCUMENT -> compareDocument(o1.asDocument(), o2.asDocument());
            default -> throw new UnsupportedOperationException("Unsupported BSON type: " + type);
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
            default -> throw new IllegalArgumentException("Unknown BSON type: " + type);
        };
    }

    private static boolean isNumeric(BsonType type)
    {
        return type == BsonType.DOUBLE
                || type == BsonType.INT32
                || type == BsonType.INT64
                || type == BsonType.DECIMAL128;
    }

    private static int compareNumbers(BsonNumber n1, BsonNumber n2)
    {
        boolean isNan1 = isNaN(n1);
        boolean isNan2 = isNaN(n2);

        if (isNan1 && isNan2) {
            return 0;
        }
        if (isNan1) {
            return -1; // NaN is less than any other number in MongoDB
        }
        if (isNan2) {
            return 1;
        }

        // Handle Double infinity
        boolean isPosInf1 = isPositiveInfinity(n1);
        boolean isPosInf2 = isPositiveInfinity(n2);
        if (isPosInf1 && isPosInf2) {
            return 0;
        }
        if (isPosInf1) {
            return 1;
        }
        if (isPosInf2) {
            return -1;
        }

        boolean isNegInf1 = isNegativeInfinity(n1);
        boolean isNegInf2 = isNegativeInfinity(n2);
        if (isNegInf1 && isNegInf2) {
            return 0;
        }
        if (isNegInf1) {
            return -1;
        }
        if (isNegInf2) {
            return 1;
        }

        // Both are finite numbers. Compare via BigDecimal for exact cross-type semantics.
        BigDecimal bd1 = toBigDecimal(n1);
        BigDecimal bd2 = toBigDecimal(n2);
        return bd1.compareTo(bd2);
    }

    private static boolean isNaN(BsonNumber number)
    {
        if (number.getBsonType() == BsonType.DOUBLE) {
            return Double.isNaN(number.doubleValue());
        }
        if (number.getBsonType() == BsonType.DECIMAL128) {
            return number.decimal128Value().isNaN();
        }
        return false;
    }

    private static boolean isPositiveInfinity(BsonNumber number)
    {
        if (number.getBsonType() == BsonType.DOUBLE) {
            return number.doubleValue() == Double.POSITIVE_INFINITY;
        }
        if (number.getBsonType() == BsonType.DECIMAL128) {
            return number.decimal128Value().isInfinite() && !number.decimal128Value().isNegative();
        }
        return false;
    }

    private static boolean isNegativeInfinity(BsonNumber number)
    {
        if (number.getBsonType() == BsonType.DOUBLE) {
            return number.doubleValue() == Double.NEGATIVE_INFINITY;
        }
        if (number.getBsonType() == BsonType.DECIMAL128) {
            return number.decimal128Value().isInfinite() && number.decimal128Value().isNegative();
        }
        return false;
    }

    private static BigDecimal toBigDecimal(BsonNumber number)
    {
        return switch (number.getBsonType()) {
            case DOUBLE -> BigDecimal.valueOf(number.doubleValue());
            case INT32 -> BigDecimal.valueOf(number.intValue());
            case INT64 -> BigDecimal.valueOf(number.longValue());
            case DECIMAL128 -> number.decimal128Value().bigDecimalValue();
            default -> throw new IllegalArgumentException("Not a numeric BSON type: " + number.getBsonType());
        };
    }

    private static int compareTimestamps(BsonTimestamp t1, BsonTimestamp t2)
    {
        int timeCompare = Integer.compareUnsigned(t1.getTime(), t2.getTime());
        if (timeCompare != 0) {
            return timeCompare;
        }
        return Integer.compareUnsigned(t1.getInc(), t2.getInc());
    }

    private static int compareRegex(BsonRegularExpression r1, BsonRegularExpression r2)
    {
        int patternCompare = r1.getPattern().compareTo(r2.getPattern());
        if (patternCompare != 0) {
            return patternCompare;
        }
        return r1.getOptions().compareTo(r2.getOptions());
    }

    private static int compareJavaScriptWithScope(BsonJavaScriptWithScope js1, BsonJavaScriptWithScope js2)
    {
        int codeCompare = js1.getCode().compareTo(js2.getCode());
        if (codeCompare != 0) {
            return codeCompare;
        }
        return compareDocument(js1.getScope(), js2.getScope());
    }

    private static int compareBinary(BsonBinary b1, BsonBinary b2)
    {
        byte[] d1 = b1.getData();
        byte[] d2 = b2.getData();
        int len1 = d1.length;
        int len2 = d2.length;
        if (len1 != len2) {
            return Integer.compare(len1, len2);
        }
        int typeCompare = Byte.compareUnsigned(b1.getType(), b2.getType());
        if (typeCompare != 0) {
            return typeCompare;
        }
        return Arrays.compare(d1, d2);
    }

    private static int compareArray(BsonArray a1, BsonArray a2)
    {
        int size1 = a1.size();
        int size2 = a2.size();
        int minSize = Math.min(size1, size2);
        for (int i = 0; i < minSize; i++) {
            int cmp = INSTANCE.compare(a1.get(i), a2.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(size1, size2);
    }

    private static int compareDocument(BsonDocument d1, BsonDocument d2)
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

        if (it1.hasNext()) {
            return 1;
        }
        if (it2.hasNext()) {
            return -1;
        }
        return 0;
    }
}
