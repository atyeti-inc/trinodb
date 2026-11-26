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

import static io.trino.plugin.mongodb.BsonValueComparator.BSON_VALUE_COMPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBsonValueComparator
{
    @Test
    public void testCanonicalBsonTypeOrdering()
    {
        List<BsonValue> orderedValues = List.of(
                new BsonMinKey(),
                new BsonNull(),
                new BsonInt32(5),
                new BsonString("5"),
                new BsonDocument("a", new BsonInt32(1)),
                new BsonArray(List.of(new BsonInt32(1))),
                new BsonBinary(new byte[] {0x01}),
                new BsonObjectId(new ObjectId("000000000000000000000001")),
                new BsonBoolean(true),
                new BsonDateTime(1000L),
                new BsonTimestamp(1000L),
                new BsonRegularExpression("abc"),
                new BsonMaxKey());

        for (int i = 0; i < orderedValues.size(); i++) {
            BsonValue current = orderedValues.get(i);
            assertThat(BSON_VALUE_COMPARATOR.compare(current, current)).isEqualTo(0);
            for (int j = i + 1; j < orderedValues.size(); j++) {
                BsonValue next = orderedValues.get(j);
                assertThat(BSON_VALUE_COMPARATOR.compare(current, next))
                        .as("%s should be less than %s", current.getBsonType(), next.getBsonType())
                        .isLessThan(0);
                assertThat(BSON_VALUE_COMPARATOR.compare(next, current))
                        .as("%s should be greater than %s", next.getBsonType(), current.getBsonType())
                        .isGreaterThan(0);
            }
        }
    }

    @Test
    public void testNumericComparisons()
    {
        // Equal numbers across types
        assertThat(BSON_VALUE_COMPARATOR.compare(new BsonInt32(42), new BsonInt64(42L))).isEqualTo(0);
        assertThat(BSON_VALUE_COMPARATOR.compare(new BsonInt32(42), new BsonDouble(42.0))).isEqualTo(0);
        assertThat(BSON_VALUE_COMPARATOR.compare(new BsonInt32(42), new BsonDecimal128(Decimal128.parse("42")))).isEqualTo(0);
        assertThat(BSON_VALUE_COMPARATOR.compare(new BsonInt64(42L), new BsonDouble(42.0))).isEqualTo(0);
        assertThat(BSON_VALUE_COMPARATOR.compare(new BsonDouble(42.0), new BsonDecimal128(Decimal128.parse("42.00")))).isEqualTo(0);

        // Unequal numbers across types
        assertThat(BSON_VALUE_COMPARATOR.compare(new BsonInt32(10), new BsonInt64(20L))).isLessThan(0);
        assertThat(BSON_VALUE_COMPARATOR.compare(new BsonInt64(20L), new BsonInt32(10))).isGreaterThan(0);
        assertThat(BSON_VALUE_COMPARATOR.compare(new BsonInt32(10), new BsonDouble(10.5))).isLessThan(0);
        assertThat(BSON_VALUE_COMPARATOR.compare(new BsonDouble(10.5), new BsonInt32(10))).isGreaterThan(0);

        // High precision 64-bit int vs double
        long largeLong = (1L << 54) + 1L;
        double largeDouble = (double) (1L << 54);
        assertThat(BSON_VALUE_COMPARATOR.compare(new BsonInt64(largeLong), new BsonDouble(largeDouble))).isGreaterThan(0);

        // Special floating point values
        assertThat(BSON_VALUE_COMPARATOR.compare(
                new BsonDouble(Double.POSITIVE_INFINITY),
                new BsonInt64(Long.MAX_VALUE))).isGreaterThan(0);
        assertThat(BSON_VALUE_COMPARATOR.compare(
                new BsonDouble(Double.NEGATIVE_INFINITY),
                new BsonInt64(Long.MIN_VALUE))).isLessThan(0);
        assertThat(BSON_VALUE_COMPARATOR.compare(
                new BsonDouble(Double.NaN),
                new BsonDouble(Double.NaN))).isEqualTo(0);
    }

    @Test
    public void testStringAndSymbolComparisons()
    {
        assertThat(BSON_VALUE_COMPARATOR.compare(new BsonString("apple"), new BsonString("banana"))).isLessThan(0);
        assertThat(BSON_VALUE_COMPARATOR.compare(new BsonString("apple"), new BsonSymbol("apple"))).isEqualTo(0);
        assertThat(BSON_VALUE_COMPARATOR.compare(new BsonSymbol("apple"), new BsonString("banana"))).isLessThan(0);
    }

    @Test
    public void testDocumentComparisons()
    {
        BsonDocument doc1 = new BsonDocument("a", new BsonInt32(1));
        BsonDocument doc2 = new BsonDocument("a", new BsonInt32(2));
        BsonDocument doc3 = new BsonDocument("b", new BsonInt32(1));
        BsonDocument doc4 = new BsonDocument("a", new BsonInt32(1)).append("b", new BsonInt32(2));

        assertThat(BSON_VALUE_COMPARATOR.compare(doc1, doc2)).isLessThan(0);
        assertThat(BSON_VALUE_COMPARATOR.compare(doc1, doc3)).isLessThan(0);
        assertThat(BSON_VALUE_COMPARATOR.compare(doc1, doc4)).isLessThan(0);
        assertThat(BSON_VALUE_COMPARATOR.compare(doc4, doc1)).isGreaterThan(0);
        assertThat(BSON_VALUE_COMPARATOR.compare(new BsonDocument(), new BsonDocument())).isEqualTo(0);
    }

    @Test
    public void testArrayComparisons()
    {
        BsonArray arr1 = new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2)));
        BsonArray arr2 = new BsonArray(List.of(new BsonInt32(1), new BsonInt32(3)));
        BsonArray arr3 = new BsonArray(List.of(new BsonInt32(1)));

        assertThat(BSON_VALUE_COMPARATOR.compare(arr1, arr2)).isLessThan(0);
        assertThat(BSON_VALUE_COMPARATOR.compare(arr3, arr1)).isLessThan(0);
        assertThat(BSON_VALUE_COMPARATOR.compare(arr1, arr3)).isGreaterThan(0);
        assertThat(BSON_VALUE_COMPARATOR.compare(arr1, arr1)).isEqualTo(0);
    }

    @Test
    public void testBinaryComparisons()
    {
        BsonBinary bin1 = new BsonBinary(new byte[] {0x01, 0x02});
        BsonBinary bin2 = new BsonBinary(new byte[] {0x01, 0x03});
        BsonBinary binUnsigned = new BsonBinary(new byte[] {(byte) 0xFF});
        BsonBinary binLow = new BsonBinary(new byte[] {0x00});

        assertThat(BSON_VALUE_COMPARATOR.compare(bin1, bin2)).isLessThan(0);
        assertThat(BSON_VALUE_COMPARATOR.compare(binLow, binUnsigned)).isLessThan(0);
        assertThat(BSON_VALUE_COMPARATOR.compare(binUnsigned, binLow)).isGreaterThan(0);
    }

    @Test
    public void testOtherTypes()
    {
        // ObjectId
        ObjectId oid1 = new ObjectId("000000000000000000000001");
        ObjectId oid2 = new ObjectId("000000000000000000000002");
        assertThat(BSON_VALUE_COMPARATOR.compare(new BsonObjectId(oid1), new BsonObjectId(oid2))).isLessThan(0);

        // Boolean
        assertThat(BSON_VALUE_COMPARATOR.compare(new BsonBoolean(false), new BsonBoolean(true))).isLessThan(0);

        // DateTime
        assertThat(BSON_VALUE_COMPARATOR.compare(new BsonDateTime(100L), new BsonDateTime(200L))).isLessThan(0);

        // Timestamp
        assertThat(BSON_VALUE_COMPARATOR.compare(new BsonTimestamp(100L), new BsonTimestamp(200L))).isLessThan(0);

        // Regular Expression
        assertThat(BSON_VALUE_COMPARATOR.compare(new BsonRegularExpression("abc"), new BsonRegularExpression("abd"))).isLessThan(0);

        // Null and Undefined
        assertThat(BSON_VALUE_COMPARATOR.compare(new BsonNull(), new BsonUndefined())).isEqualTo(0);
    }
}
