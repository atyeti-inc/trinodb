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
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBsonValueComparator
{
    private static final BsonValueComparator COMPARATOR = BsonValueComparator.INSTANCE;

    @Test
    public void testCanonicalTypeOrdering()
    {
        List<BsonValue> orderedRepresentatives = List.of(
                new BsonMinKey(),
                new BsonNull(),
                new BsonInt32(42),
                new BsonString("hello"),
                new BsonDocument("k", new BsonString("v")),
                new BsonArray(List.of(new BsonInt32(1))),
                new BsonBinary(new byte[] {1, 2}),
                new BsonObjectId(new ObjectId("507f191e810c19729de860ea")),
                new BsonBoolean(true),
                new BsonDateTime(123456789L),
                new BsonTimestamp(1000, 1),
                new BsonRegularExpression("abc", "i"),
                new BsonJavaScript("function() { return 1; }"),
                new BsonMaxKey());

        for (int i = 0; i < orderedRepresentatives.size(); i++) {
            BsonValue current = orderedRepresentatives.get(i);
            assertThat(COMPARATOR.compare(current, current)).isEqualTo(0);

            for (int j = i + 1; j < orderedRepresentatives.size(); j++) {
                BsonValue next = orderedRepresentatives.get(j);
                assertThat(COMPARATOR.compare(current, next))
                        .as("Expected %s < %s", current.getBsonType(), next.getBsonType())
                        .isLessThan(0);
                assertThat(COMPARATOR.compare(next, current))
                        .as("Expected %s > %s", next.getBsonType(), current.getBsonType())
                        .isGreaterThan(0);
            }
        }
    }

    @Test
    public void testNullAndUndefined()
    {
        BsonNull bsonNull = new BsonNull();
        BsonUndefined bsonUndefined = new BsonUndefined();

        assertThat(COMPARATOR.compare(bsonNull, bsonNull)).isEqualTo(0);
        assertThat(COMPARATOR.compare(bsonUndefined, bsonUndefined)).isEqualTo(0);
        assertThat(COMPARATOR.compare(bsonNull, bsonUndefined)).isEqualTo(0);
        assertThat(COMPARATOR.compare(bsonUndefined, bsonNull)).isEqualTo(0);
    }

    @Test
    public void testCrossNumericComparisons()
    {
        BsonDouble d1 = new BsonDouble(1.0);
        BsonInt32 i1 = new BsonInt32(1);
        BsonInt64 l1 = new BsonInt64(1L);
        BsonDecimal128 dec1 = new BsonDecimal128(Decimal128.parse("1.00"));

        assertThat(COMPARATOR.compare(d1, i1)).isEqualTo(0);
        assertThat(COMPARATOR.compare(d1, l1)).isEqualTo(0);
        assertThat(COMPARATOR.compare(d1, dec1)).isEqualTo(0);
        assertThat(COMPARATOR.compare(i1, l1)).isEqualTo(0);
        assertThat(COMPARATOR.compare(i1, dec1)).isEqualTo(0);
        assertThat(COMPARATOR.compare(l1, dec1)).isEqualTo(0);

        BsonInt32 i2 = new BsonInt32(2);
        BsonDouble d19 = new BsonDouble(1.9);
        assertThat(COMPARATOR.compare(i2, d19)).isGreaterThan(0);
        assertThat(COMPARATOR.compare(d19, i2)).isLessThan(0);

        BsonInt64 l10 = new BsonInt64(10L);
        BsonDouble d101 = new BsonDouble(10.1);
        assertThat(COMPARATOR.compare(l10, d101)).isLessThan(0);
        assertThat(COMPARATOR.compare(d101, l10)).isGreaterThan(0);
    }

    @Test
    public void testNumericNaNAndInfinities()
    {
        BsonDouble doubleNaN = new BsonDouble(Double.NaN);
        BsonDecimal128 decimalNaN = new BsonDecimal128(Decimal128.NaN);
        BsonDouble doubleNegInf = new BsonDouble(Double.NEGATIVE_INFINITY);
        BsonDecimal128 decimalNegInf = new BsonDecimal128(Decimal128.NEGATIVE_INFINITY);
        BsonDouble doublePosInf = new BsonDouble(Double.POSITIVE_INFINITY);
        BsonDecimal128 decimalPosInf = new BsonDecimal128(Decimal128.POSITIVE_INFINITY);
        BsonInt32 zero = new BsonInt32(0);

        // NaN comparisons
        assertThat(COMPARATOR.compare(doubleNaN, doubleNaN)).isEqualTo(0);
        assertThat(COMPARATOR.compare(decimalNaN, decimalNaN)).isEqualTo(0);
        assertThat(COMPARATOR.compare(doubleNaN, decimalNaN)).isEqualTo(0);
        assertThat(COMPARATOR.compare(doubleNaN, doubleNegInf)).isLessThan(0);
        assertThat(COMPARATOR.compare(doubleNaN, zero)).isLessThan(0);
        assertThat(COMPARATOR.compare(doubleNaN, doublePosInf)).isLessThan(0);

        // Negative infinity
        assertThat(COMPARATOR.compare(doubleNegInf, doubleNegInf)).isEqualTo(0);
        assertThat(COMPARATOR.compare(doubleNegInf, decimalNegInf)).isEqualTo(0);
        assertThat(COMPARATOR.compare(doubleNegInf, zero)).isLessThan(0);
        assertThat(COMPARATOR.compare(doubleNegInf, doublePosInf)).isLessThan(0);

        // Positive infinity
        assertThat(COMPARATOR.compare(doublePosInf, doublePosInf)).isEqualTo(0);
        assertThat(COMPARATOR.compare(doublePosInf, decimalPosInf)).isEqualTo(0);
        assertThat(COMPARATOR.compare(doublePosInf, zero)).isGreaterThan(0);
    }

    @Test
    public void testStringsAndSymbols()
    {
        BsonString s1 = new BsonString("apple");
        BsonString s2 = new BsonString("banana");
        BsonSymbol sym1 = new BsonSymbol("apple");

        assertThat(COMPARATOR.compare(s1, s2)).isLessThan(0);
        assertThat(COMPARATOR.compare(s2, s1)).isGreaterThan(0);
        assertThat(COMPARATOR.compare(s1, sym1)).isEqualTo(0);
        assertThat(COMPARATOR.compare(sym1, s1)).isEqualTo(0);
    }

    @Test
    public void testDocuments()
    {
        BsonDocument d1 = new BsonDocument("a", new BsonInt32(1));
        BsonDocument d2 = new BsonDocument("a", new BsonInt32(2));
        BsonDocument d3 = new BsonDocument("b", new BsonInt32(1));
        BsonDocument d4 = new BsonDocument("a", new BsonInt32(1)).append("b", new BsonInt32(2));

        assertThat(COMPARATOR.compare(d1, d1)).isEqualTo(0);
        assertThat(COMPARATOR.compare(d1, d2)).isLessThan(0);
        assertThat(COMPARATOR.compare(d1, d3)).isLessThan(0);
        assertThat(COMPARATOR.compare(d1, d4)).isLessThan(0);
    }

    @Test
    public void testArrays()
    {
        BsonArray a1 = new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2)));
        BsonArray a2 = new BsonArray(List.of(new BsonInt32(1), new BsonInt32(3)));
        BsonArray a3 = new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2), new BsonInt32(0)));

        assertThat(COMPARATOR.compare(a1, a1)).isEqualTo(0);
        assertThat(COMPARATOR.compare(a1, a2)).isLessThan(0);
        assertThat(COMPARATOR.compare(a1, a3)).isLessThan(0);
    }

    @Test
    public void testBinary()
    {
        BsonBinary b1 = new BsonBinary((byte) 0, new byte[] {1});
        BsonBinary b2 = new BsonBinary((byte) 0, new byte[] {1, 2});
        BsonBinary b3 = new BsonBinary((byte) 1, new byte[] {1, 2});
        BsonBinary b4 = new BsonBinary((byte) 0, new byte[] {1, 3});

        assertThat(COMPARATOR.compare(b1, b2)).isLessThan(0); // length
        assertThat(COMPARATOR.compare(b2, b3)).isLessThan(0); // subtype
        assertThat(COMPARATOR.compare(b2, b4)).isLessThan(0); // data
    }

    @Test
    public void testObjectId()
    {
        BsonObjectId o1 = new BsonObjectId(new ObjectId("507f191e810c19729de860ea"));
        BsonObjectId o2 = new BsonObjectId(new ObjectId("507f191e810c19729de860eb"));

        assertThat(COMPARATOR.compare(o1, o1)).isEqualTo(0);
        assertThat(COMPARATOR.compare(o1, o2)).isLessThan(0);
    }

    @Test
    public void testTimestamps()
    {
        BsonTimestamp t1 = new BsonTimestamp(100, 1);
        BsonTimestamp t2 = new BsonTimestamp(100, 2);
        BsonTimestamp t3 = new BsonTimestamp(101, 1);

        assertThat(COMPARATOR.compare(t1, t2)).isLessThan(0);
        assertThat(COMPARATOR.compare(t2, t3)).isLessThan(0);
    }

    @Test
    public void testRegularExpressions()
    {
        BsonRegularExpression r1 = new BsonRegularExpression("abc", "i");
        BsonRegularExpression r2 = new BsonRegularExpression("abd", "i");
        BsonRegularExpression r3 = new BsonRegularExpression("abc", "m");

        assertThat(COMPARATOR.compare(r1, r2)).isLessThan(0);
        assertThat(COMPARATOR.compare(r1, r3)).isLessThan(0);
    }

    @Test
    public void testJavaScript()
    {
        BsonJavaScript js1 = new BsonJavaScript("var x = 1;");
        BsonJavaScript js2 = new BsonJavaScript("var x = 2;");
        BsonJavaScriptWithScope jsw1 = new BsonJavaScriptWithScope("var x = 1;", new BsonDocument("a", new BsonInt32(1)));
        BsonJavaScriptWithScope jsw2 = new BsonJavaScriptWithScope("var x = 1;", new BsonDocument("a", new BsonInt32(2)));

        assertThat(COMPARATOR.compare(js1, js2)).isLessThan(0);
        assertThat(COMPARATOR.compare(jsw1, jsw2)).isLessThan(0);
    }
}
