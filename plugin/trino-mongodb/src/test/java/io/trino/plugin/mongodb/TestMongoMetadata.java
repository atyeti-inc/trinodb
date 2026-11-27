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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoMetadata
{
    private static final SchemaTableName TABLE_NAME = new SchemaTableName("test_schema", "test_table");
    private static final RemoteTableName REMOTE_TABLE_NAME = new RemoteTableName("test_schema", "test_table");

    private final MongoMetadata metadata;

    public TestMongoMetadata()
    {
        MongoSession session = new MongoSession(
                TESTING_TYPE_MANAGER,
                () -> { throw new UnsupportedOperationException(); },
                new MongoClientConfig());
        metadata = new MongoMetadata(session);
    }

    private static MongoTableHandle createTableHandle()
    {
        return new MongoTableHandle(
                TABLE_NAME,
                REMOTE_TABLE_NAME,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(),
                ImmutableList.of(),
                OptionalInt.empty());
    }

    private static MongoColumnHandle createColumnHandle(String name)
    {
        return new MongoColumnHandle(name, ImmutableList.of(), BIGINT, false, false, Optional.empty());
    }

    private static MongoColumnHandle createNestedColumnHandle(String baseName, String... dereferences)
    {
        return new MongoColumnHandle(baseName, ImmutableList.copyOf(dereferences), VARCHAR, false, false, Optional.empty());
    }

    @Test
    public void testApplyTopNSuccess()
    {
        MongoTableHandle handle = createTableHandle();
        MongoColumnHandle col1 = createColumnHandle("col1");
        MongoColumnHandle col2 = createColumnHandle("col2");
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col1", col1, "col2", col2);
        List<SortItem> sortItems = ImmutableList.of(
                new SortItem("col1", ASC_NULLS_FIRST),
                new SortItem("col2", DESC_NULLS_LAST));

        Optional<TopNApplicationResult<ConnectorTableHandle>> result = metadata.applyTopN(SESSION, handle, 10, sortItems, assignments);

        assertThat(result).isPresent();
        TopNApplicationResult<ConnectorTableHandle> applicationResult = result.get();
        assertThat(applicationResult.isTopNGuaranteed()).isTrue();
        assertThat(applicationResult.isPrecalculateStatistics()).isFalse();

        MongoTableHandle newHandle = (MongoTableHandle) applicationResult.getHandle();
        assertThat(newHandle.limit()).isEqualTo(OptionalInt.of(10));
        assertThat(newHandle.sortOrder()).containsExactly(
                new MongoSortItem(col1, ASC_NULLS_FIRST),
                new MongoSortItem(col2, DESC_NULLS_LAST));
    }

    @Test
    public void testApplyTopNRejectsUnsupportedSortOrder()
    {
        MongoTableHandle handle = createTableHandle();
        MongoColumnHandle col = createColumnHandle("col");
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", col);

        assertThat(metadata.applyTopN(SESSION, handle, 10, ImmutableList.of(new SortItem("col", ASC_NULLS_LAST)), assignments))
                .isEmpty();
        assertThat(metadata.applyTopN(SESSION, handle, 10, ImmutableList.of(new SortItem("col", DESC_NULLS_FIRST)), assignments))
                .isEmpty();
    }

    @Test
    public void testApplyTopNRejectsInvalidSortItemsSize()
    {
        MongoTableHandle handle = createTableHandle();
        MongoColumnHandle col = createColumnHandle("col");
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", col);

        // Empty sort items
        assertThat(metadata.applyTopN(SESSION, handle, 10, ImmutableList.of(), assignments))
                .isEmpty();

        // 32 sort items - allowed boundary
        List<SortItem> thirtyTwoItems = new ArrayList<>();
        ImmutableMap.Builder<String, ColumnHandle> thirtyTwoAssignments = ImmutableMap.builder();
        for (int i = 0; i < 32; i++) {
            String name = "col" + i;
            thirtyTwoItems.add(new SortItem(name, ASC_NULLS_FIRST));
            thirtyTwoAssignments.put(name, createColumnHandle(name));
        }
        assertThat(metadata.applyTopN(SESSION, handle, 10, thirtyTwoItems, thirtyTwoAssignments.buildOrThrow()))
                .isPresent();

        // 33 sort items - exceeds maximum 32
        List<SortItem> thirtyThreeItems = new ArrayList<>(thirtyTwoItems);
        thirtyThreeItems.add(new SortItem("col32", ASC_NULLS_FIRST));
        ImmutableMap.Builder<String, ColumnHandle> thirtyThreeAssignments = ImmutableMap.<String, ColumnHandle>builder()
                .putAll(thirtyTwoAssignments.buildOrThrow())
                .put("col32", createColumnHandle("col32"));
        assertThat(metadata.applyTopN(SESSION, handle, 10, thirtyThreeItems, thirtyThreeAssignments.buildOrThrow()))
                .isEmpty();
    }

    @Test
    public void testApplyTopNRejectsInvalidLimit()
    {
        MongoTableHandle handle = createTableHandle();
        MongoColumnHandle col = createColumnHandle("col");
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", col);
        List<SortItem> sortItems = ImmutableList.of(new SortItem("col", ASC_NULLS_FIRST));

        // Limit <= 0
        assertThat(metadata.applyTopN(SESSION, handle, 0, sortItems, assignments)).isEmpty();
        assertThat(metadata.applyTopN(SESSION, handle, -1, sortItems, assignments)).isEmpty();

        // Limit > Integer.MAX_VALUE
        assertThat(metadata.applyTopN(SESSION, handle, ((long) Integer.MAX_VALUE) + 1, sortItems, assignments)).isEmpty();

        // Limit == Integer.MAX_VALUE
        Optional<TopNApplicationResult<ConnectorTableHandle>> result = metadata.applyTopN(
                SESSION,
                handle,
                Integer.MAX_VALUE,
                sortItems,
                assignments);
        assertThat(result).isPresent();
        assertThat(((MongoTableHandle) result.get().getHandle()).limit()).isEqualTo(OptionalInt.of(Integer.MAX_VALUE));
    }

    @Test
    public void testApplyTopNRejectsAlreadyLimitedOrSorted()
    {
        MongoColumnHandle col = createColumnHandle("col");
        Map<String, ColumnHandle> assignments = ImmutableMap.of("col", col);
        List<SortItem> sortItems = ImmutableList.of(new SortItem("col", ASC_NULLS_FIRST));

        // Already limited
        MongoTableHandle limitedHandle = new MongoTableHandle(
                TABLE_NAME,
                REMOTE_TABLE_NAME,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(),
                ImmutableList.of(),
                OptionalInt.of(5));
        assertThat(metadata.applyTopN(SESSION, limitedHandle, 10, sortItems, assignments)).isEmpty();

        // Already sorted
        MongoTableHandle sortedHandle = new MongoTableHandle(
                TABLE_NAME,
                REMOTE_TABLE_NAME,
                Optional.empty(),
                TupleDomain.all(),
                ImmutableSet.of(),
                ImmutableList.of(new MongoSortItem(col, ASC_NULLS_FIRST)),
                OptionalInt.empty());
        assertThat(metadata.applyTopN(SESSION, sortedHandle, 10, sortItems, assignments)).isEmpty();
    }

    @Test
    public void testApplyTopNColumnHandleResolution()
    {
        MongoTableHandle handle = createTableHandle();
        MongoColumnHandle nestedCol = createNestedColumnHandle("a", "b", "c");
        Map<String, ColumnHandle> assignments = ImmutableMap.of("nested", nestedCol);
        List<SortItem> sortItems = ImmutableList.of(new SortItem("nested", DESC_NULLS_LAST));

        Optional<TopNApplicationResult<ConnectorTableHandle>> result = metadata.applyTopN(SESSION, handle, 5, sortItems, assignments);
        assertThat(result).isPresent();
        MongoTableHandle newHandle = (MongoTableHandle) result.get().getHandle();
        assertThat(newHandle.sortOrder()).containsExactly(new MongoSortItem(nestedCol, DESC_NULLS_LAST));
        assertThat(newHandle.sortOrder().get(0).column().getQualifiedName()).isEqualTo("a.b.c");

        // Missing column in assignments
        assertThat(metadata.applyTopN(SESSION, handle, 5, ImmutableList.of(new SortItem("missing", DESC_NULLS_LAST)), assignments))
                .isEmpty();

        // Non-MongoColumnHandle in assignments
        ColumnHandle foreignColumnHandle = new ColumnHandle() {};
        assertThat(metadata.applyTopN(
                SESSION,
                handle,
                5,
                ImmutableList.of(new SortItem("foreign", DESC_NULLS_LAST)),
                ImmutableMap.of("foreign", foreignColumnHandle)))
                .isEmpty();
    }
}
