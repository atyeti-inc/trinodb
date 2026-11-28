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

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.mongodb.TypeUtils.isNumericType;
import static io.trino.plugin.mongodb.TypeUtils.isPushdownSupportedDomain;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTypeUtils
{
    @Test
    public void testIsNumericType()
    {
        assertThat(isNumericType(BIGINT)).isTrue();
        assertThat(isNumericType(INTEGER)).isTrue();
        assertThat(isNumericType(SMALLINT)).isTrue();
        assertThat(isNumericType(TINYINT)).isTrue();
        assertThat(isNumericType(REAL)).isTrue();
        assertThat(isNumericType(DOUBLE)).isTrue();
        assertThat(isNumericType(DecimalType.createDecimalType(10, 2))).isTrue();
        assertThat(isNumericType(DecimalType.createDecimalType(38, 10))).isTrue();

        assertThat(isNumericType(VARCHAR)).isFalse();
        assertThat(isNumericType(BOOLEAN)).isFalse();
        assertThat(isNumericType(DATE)).isFalse();
        assertThat(isNumericType(ObjectIdType.OBJECT_ID)).isFalse();
    }

    @Test
    public void testNaNDomainAfterCompaction()
    {
        Domain mixed = Domain.create(ValueSet.of(DOUBLE, 1.0, 3.0, Double.NaN), false);
        assertThat(isPushdownSupportedDomain(mixed)).isFalse();
        assertThat(isPushdownSupportedDomain(mixed.simplify(1))).isFalse();
        assertThat(isPushdownSupportedDomain(Domain.notNull(DOUBLE))).isTrue();
        assertThat(isPushdownSupportedDomain(Domain.onlyNull(DOUBLE))).isTrue();
        assertThat(isPushdownSupportedDomain(Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, 1.0)), false))).isTrue();
    }
}
