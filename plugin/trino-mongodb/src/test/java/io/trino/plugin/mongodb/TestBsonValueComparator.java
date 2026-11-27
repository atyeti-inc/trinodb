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
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.plugin.mongodb.BsonValueComparator.BSON_VALUE_COMPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBsonValueComparator
{
    @Test
    public void testCrossTypePrecedence()
    {
        List<BsonValue> orderedValues = List.of(
                new BsonMinKey(),
                BsonNull.VALUE,
                new BsonInt32(100),
                new BsonString("sample_string"),
                new BsonDocument("a", new BsonInt32(1)),
                new BsonArray(List.of(new BsonInt32(1))),
                new BsonBinary(new byte[] {1, 2}),
                new BsonObjectId(new ObjectId("507f191e810c19729de860ea")),
                BsonBoolean.TRUE,
                new BsonDateTime(1600000000000L),
                new BsonTimestamp(100, 1),
                new BsonRegularExpression("abc", "i"),
                new BsonJavaScript("function() {}"),
                new BsonMaxKey());

        for (int i = 0; i < orderedValues.size(); i++) {
            for (int j = 0; j < orderedValues.size(); j++) {
                BsonValue v1 = orderedValues.get(i);
                BsonValue v2 = orderedValues.get(j);
                int expectedSign = Integer.compare(i, j);
                int actualSign = Integer.signum(BSON_VALUE_COMPARATOR.compare(v1, v2));
                assertThat(actualSign)
                        .as("Comparison of %s and %s", v1.getClass().getSimpleName(), v2.getClass().getSimpleName())
                        .isEqualTo(expectedSign);
            }
        }
    }

    @Test
    public void testCrossNumericEquivalence()
    {
        BsonInt32 intVal = new BsonInt32(5);
        BsonInt64 longVal = new BsonInt64(5L);
        BsonDouble doubleVal = new BsonDouble(5.0);
        BsonDecimal128 decimalVal = new BsonDecimal128(new Decimal128(5L));

        List<BsonValue> equivalentFives = List.of(intVal, longVal, doubleVal, decimalVal);
        for (BsonValue v1 : equivalentFives) {
            for (BsonValue v2 : equivalentFives) {
                assertThat(BSON_VALUE_COMPARATOR.compare(v1, v2))
                        .as("Comparing %s with %s", v1, v2)
                        .isZero();
            }
        }

        BsonInt32 smaller = new BsonInt32(4);
        BsonDouble larger = new BsonDouble(5.1);
        assertThat(BSON_VALUE_COMPARATOR.compare(smaller, doubleVal)).isNegative();
        assertThat(BSON_VALUE_COMPARATOR.compare(larger, decimalVal)).isPositive();
    }

    @Test
    public void testSpecialFloatingPointValues()
    {
        BsonDouble doubleNan = new BsonDouble(Double.NaN);
        BsonDecimal128 decimalNan = new BsonDecimal128(Decimal128.NaN);
        BsonDouble doubleNegInf = new BsonDouble(Double.NEGATIVE_INFINITY);
        BsonDecimal128 decimalNegInf = new BsonDecimal128(Decimal128.NEGATIVE_INFINITY);
        BsonDouble doublePosInf = new BsonDouble(Double.POSITIVE_INFINITY);
        BsonDecimal128 decimalPosInf = new BsonDecimal128(Decimal128.POSITIVE_INFINITY);
        BsonInt32 finiteNumber = new BsonInt32(-1000);

        // NaN == NaN across Double and Decimal128
        assertThat(BSON_VALUE_COMPARATOR.compare(doubleNan, decimalNan)).isZero();
        assertThat(BSON_VALUE_COMPARATOR.compare(decimalNan, doubleNan)).isZero();

        // -Infinity == -Infinity across Double and Decimal128
        assertThat(BSON_VALUE_COMPARATOR.compare(doubleNegInf, decimalNegInf)).isZero();

        // +Infinity == +Infinity across Double and Decimal128
        assertThat(BSON_VALUE_COMPARATOR.compare(doublePosInf, decimalPosInf)).isZero();

        // Ordering: NaN < -Infinity < Finite < +Infinity
        assertThat(BSON_VALUE_COMPARATOR.compare(doubleNan, doubleNegInf)).isNegative();
        assertThat(BSON_VALUE_COMPARATOR.compare(decimalNan, decimalNegInf)).isNegative();

        assertThat(BSON_VALUE_COMPARATOR.compare(doubleNegInf, finiteNumber)).isNegative();
        assertThat(BSON_VALUE_COMPARATOR.compare(decimalNegInf, finiteNumber)).isNegative();

        assertThat(BSON_VALUE_COMPARATOR.compare(finiteNumber, doublePosInf)).isNegative();
        assertThat(BSON_VALUE_COMPARATOR.compare(finiteNumber, decimalPosInf)).isNegative();
    }

    @Test
    public void testBinaryComparison()
    {
        // 1. Length takes precedence
        BsonBinary shortBin = new BsonBinary(new byte[] {0x05});
        BsonBinary longBin = new BsonBinary(new byte[] {0x01, 0x02});
        assertThat(BSON_VALUE_COMPARATOR.compare(shortBin, longBin)).isNegative();

        // 2. Subtype takes precedence when length is equal
        BsonBinary typeZero = new BsonBinary((byte) 0, new byte[] {0x02});
        BsonBinary typeFour = new BsonBinary((byte) 4, new byte[] {0x01});
        assertThat(BSON_VALUE_COMPARATOR.compare(typeZero, typeFour)).isNegative();

        // 3. Unsigned byte comparison when length and subtype are equal
        BsonBinary binLow = new BsonBinary(new byte[] {0x01, 0x7F});
        BsonBinary binHigh = new BsonBinary(new byte[] {0x01, (byte) 0x80}); // 0x80 unsigned is 128 > 127
        assertThat(BSON_VALUE_COMPARATOR.compare(binLow, binHigh)).isNegative();
    }

    @Test
    public void testStringAndSymbolComparison()
    {
        BsonString str = new BsonString("test");
        BsonSymbol sym = new BsonSymbol("test");
        assertThat(BSON_VALUE_COMPARATOR.compare(str, sym)).isZero();

        BsonString lower = new BsonString("alpha");
        BsonSymbol higher = new BsonSymbol("beta");
        assertThat(BSON_VALUE_COMPARATOR.compare(lower, higher)).isNegative();
    }

    @Test
    public void testDocumentAndArrayComparison()
    {
        BsonDocument doc1 = new BsonDocument("a", new BsonInt32(1));
        BsonDocument doc2 = new BsonDocument("a", new BsonInt32(2));
        BsonDocument doc3 = new BsonDocument("b", new BsonInt32(1));
        assertThat(BSON_VALUE_COMPARATOR.compare(doc1, doc2)).isNegative();
        assertThat(BSON_VALUE_COMPARATOR.compare(doc1, doc3)).isNegative();

        BsonArray arr1 = new BsonArray(List.of(new BsonInt32(1)));
        BsonArray arr2 = new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2)));
        BsonArray arr3 = new BsonArray(List.of(new BsonInt32(2)));
        assertThat(BSON_VALUE_COMPARATOR.compare(arr1, arr2)).isNegative();
        assertThat(BSON_VALUE_COMPARATOR.compare(arr1, arr3)).isNegative();
    }

    @Test
    public void testTimestampAndDateComparison()
    {
        BsonDateTime date1 = new BsonDateTime(1000L);
        BsonDateTime date2 = new BsonDateTime(2000L);
        assertThat(BSON_VALUE_COMPARATOR.compare(date1, date2)).isNegative();

        BsonTimestamp ts1 = new BsonTimestamp(100, 1);
        BsonTimestamp ts2 = new BsonTimestamp(100, 2);
        BsonTimestamp ts3 = new BsonTimestamp(101, 1);
        assertThat(BSON_VALUE_COMPARATOR.compare(ts1, ts2)).isNegative();
        assertThat(BSON_VALUE_COMPARATOR.compare(ts1, ts3)).isNegative();
    }
}
