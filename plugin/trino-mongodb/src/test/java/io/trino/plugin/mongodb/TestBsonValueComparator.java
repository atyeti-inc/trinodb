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
import org.bson.BsonUndefined;
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBsonValueComparator
{
    private final BsonValueComparator comparator = BsonValueComparator.INSTANCE;

    @Test
    public void testTypeOrder()
    {
        // Canonical BSON type comparison ordering:
        // MinKey < Null/Undefined < Numbers < Symbol/String < Document < Array < Binary < ObjectId < Boolean < Date < Timestamp < Regex < JavaScript < MaxKey
        List<BsonValue> samplePerType = List.of(
                new BsonMinKey(),
                new BsonNull(),
                new BsonInt32(10),
                new BsonString("hello"),
                new BsonDocument("k", new BsonInt32(1)),
                new BsonArray(List.of(new BsonInt32(1))),
                new BsonBinary(new byte[] {1, 2, 3}),
                new BsonObjectId(new ObjectId("507f1f77bcf86cd799439011")),
                new BsonBoolean(true),
                new BsonDateTime(1000L),
                new BsonTimestamp(1000, 1),
                new BsonRegularExpression("abc"),
                new BsonJavaScript("function() {}"),
                new BsonMaxKey());

        for (int i = 0; i < samplePerType.size(); i++) {
            for (int j = 0; j < samplePerType.size(); j++) {
                int cmp = comparator.compare(samplePerType.get(i), samplePerType.get(j));
                if (i < j) {
                    assertThat(cmp).as("Expected element %d < element %d", i, j).isLessThan(0);
                }
                else if (i > j) {
                    assertThat(cmp).as("Expected element %d > element %d", i, j).isGreaterThan(0);
                }
                else {
                    assertThat(cmp).as("Expected element %d == element %d", i, j).isEqualTo(0);
                }
            }
        }
    }

    @Test
    public void testNumericComparisons()
    {
        // Equal numbers across different BSON types
        assertThat(comparator.compare(new BsonInt32(42), new BsonInt64(42L))).isEqualTo(0);
        assertThat(comparator.compare(new BsonInt32(42), new BsonDouble(42.0))).isEqualTo(0);
        assertThat(comparator.compare(new BsonInt32(42), new BsonDecimal128(new Decimal128(new BigDecimal("42.000"))))).isEqualTo(0);
        assertThat(comparator.compare(new BsonDouble(42.0), new BsonInt64(42L))).isEqualTo(0);

        // Unequal numbers across different BSON types
        assertThat(comparator.compare(new BsonInt32(10), new BsonDouble(10.5))).isLessThan(0);
        assertThat(comparator.compare(new BsonDouble(10.5), new BsonInt32(10))).isGreaterThan(0);
        assertThat(comparator.compare(new BsonInt64(100L), new BsonDecimal128(new Decimal128(new BigDecimal("99.99"))))).isGreaterThan(0);
        assertThat(comparator.compare(new BsonInt32(-5), new BsonInt64(-4L))).isLessThan(0);

        // Double NaN and Decimal128 NaN
        BsonDouble doubleNan = new BsonDouble(Double.NaN);
        BsonDecimal128 decimalNan = new BsonDecimal128(Decimal128.NaN);
        assertThat(comparator.compare(doubleNan, doubleNan)).isEqualTo(0);
        assertThat(comparator.compare(doubleNan, decimalNan)).isEqualTo(0);
        assertThat(comparator.compare(decimalNan, doubleNan)).isEqualTo(0);

        // NaN is less than any other number in MongoDB
        assertThat(comparator.compare(doubleNan, new BsonDouble(Double.NEGATIVE_INFINITY))).isLessThan(0);
        assertThat(comparator.compare(doubleNan, new BsonInt32(-1000000))).isLessThan(0);
        assertThat(comparator.compare(doubleNan, new BsonDouble(0.0))).isLessThan(0);
        assertThat(comparator.compare(decimalNan, new BsonDouble(-1.0))).isLessThan(0);

        // Infinities
        BsonDouble posInf = new BsonDouble(Double.POSITIVE_INFINITY);
        BsonDouble negInf = new BsonDouble(Double.NEGATIVE_INFINITY);
        BsonDecimal128 decimalPosInf = new BsonDecimal128(Decimal128.POSITIVE_INFINITY);
        BsonDecimal128 decimalNegInf = new BsonDecimal128(Decimal128.NEGATIVE_INFINITY);

        assertThat(comparator.compare(negInf, posInf)).isLessThan(0);
        assertThat(comparator.compare(negInf, new BsonInt64(-999999999999L))).isLessThan(0);
        assertThat(comparator.compare(posInf, new BsonInt64(999999999999L))).isGreaterThan(0);
        assertThat(comparator.compare(posInf, decimalPosInf)).isEqualTo(0);
        assertThat(comparator.compare(negInf, decimalNegInf)).isEqualTo(0);
    }

    @Test
    public void testStringAndSymbolComparison()
    {
        assertThat(comparator.compare(new BsonString("apple"), new BsonString("banana"))).isLessThan(0);
        assertThat(comparator.compare(new BsonString("banana"), new BsonString("apple"))).isGreaterThan(0);
        assertThat(comparator.compare(new BsonString("apple"), new BsonString("apple"))).isEqualTo(0);
        assertThat(comparator.compare(new BsonSymbol("apple"), new BsonSymbol("banana"))).isLessThan(0);
    }

    @Test
    public void testNullAndUndefined()
    {
        assertThat(comparator.compare(new BsonNull(), new BsonUndefined())).isEqualTo(0);
        assertThat(comparator.compare(new BsonUndefined(), new BsonNull())).isEqualTo(0);
        assertThat(comparator.compare(new BsonNull(), new BsonMinKey())).isGreaterThan(0);
        assertThat(comparator.compare(new BsonNull(), new BsonInt32(0))).isLessThan(0);
    }

    @Test
    public void testObjectIdComparison()
    {
        ObjectId id1 = new ObjectId("507f1f77bcf86cd799439011");
        ObjectId id2 = new ObjectId("507f1f77bcf86cd799439012");
        assertThat(comparator.compare(new BsonObjectId(id1), new BsonObjectId(id2))).isLessThan(0);
        assertThat(comparator.compare(new BsonObjectId(id2), new BsonObjectId(id1))).isGreaterThan(0);
        assertThat(comparator.compare(new BsonObjectId(id1), new BsonObjectId(id1))).isEqualTo(0);
    }

    @Test
    public void testTimestampComparison()
    {
        BsonTimestamp t1 = new BsonTimestamp(100, 1);
        BsonTimestamp t2 = new BsonTimestamp(100, 2);
        BsonTimestamp t3 = new BsonTimestamp(101, 1);

        assertThat(comparator.compare(t1, t2)).isLessThan(0);
        assertThat(comparator.compare(t2, t3)).isLessThan(0);
        assertThat(comparator.compare(t3, t1)).isGreaterThan(0);
    }

    @Test
    public void testBinaryComparison()
    {
        BsonBinary b1 = new BsonBinary(new byte[] {1, 2});
        BsonBinary b2 = new BsonBinary(new byte[] {1, 2, 3});

        // Length differs: smaller length is smaller
        assertThat(comparator.compare(b1, b2)).isLessThan(0);
        // Same length: byte comparison
        assertThat(comparator.compare(new BsonBinary(new byte[] {1, 2}), new BsonBinary(new byte[] {1, 3}))).isLessThan(0);
    }

    @Test
    public void testArrayAndDocumentComparison()
    {
        BsonArray a1 = new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2)));
        BsonArray a2 = new BsonArray(List.of(new BsonInt32(1), new BsonInt32(3)));
        BsonArray a3 = new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2), new BsonInt32(0)));

        assertThat(comparator.compare(a1, a2)).isLessThan(0);
        assertThat(comparator.compare(a1, a3)).isLessThan(0);

        BsonDocument d1 = new BsonDocument("a", new BsonInt32(1));
        BsonDocument d2 = new BsonDocument("a", new BsonInt32(2));
        BsonDocument d3 = new BsonDocument("b", new BsonInt32(1));

        assertThat(comparator.compare(d1, d2)).isLessThan(0);
        assertThat(comparator.compare(d1, d3)).isLessThan(0);
    }
}
