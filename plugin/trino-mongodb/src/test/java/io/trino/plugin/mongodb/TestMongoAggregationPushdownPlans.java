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
import com.google.common.io.Closer;
import com.mongodb.client.MongoClient;
import io.trino.Session;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.assertions.BasePushdownPlanTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.mongodb.MongoQueryRunner.createMongoClient;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestMongoAggregationPushdownPlans
        extends BasePushdownPlanTest
{
    private static final String CATALOG = "mongodb";
    private static final String SCHEMA = "test";

    private final Closer closer = Closer.create();

    @Override
    protected PlanTester createPlanTester()
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .build();

        PlanTester planTester = PlanTester.create(session);

        MongoServer server = closer.register(new MongoServer());
        MongoClient client = closer.register(createMongoClient(server));

        try {
            planTester.installPlugin(new MongoPlugin());
            planTester.createCatalog(
                    CATALOG,
                    "mongodb",
                    ImmutableMap.of("mongodb.connection-url", server.getConnectionString().toString()));
            // Put a dummy schema collection because MongoDB doesn't support a database without collections
            client.getDatabase(SCHEMA).createCollection("dummy");
        }
        catch (Throwable e) {
            closeAllSuppress(e, planTester);
            throw e;
        }
        return planTester;
    }

    @AfterAll
    public final void destroy()
            throws Exception
    {
        closer.close();
    }

    @Test
    public void testGlobalAggregationPushdown()
    {
        String tableName = "test_global_aggregation_" + randomNameSuffix();
        getPlanTester().executeStatement("CREATE TABLE " + tableName + " AS SELECT BIGINT '10' AS val");

        assertPlan(
                "SELECT count(*), sum(val) FROM " + tableName,
                anyTree(
                        tableScan(
                                table -> {
                                    MongoTableHandle actual = (MongoTableHandle) table;
                                    return actual.aggregation().isPresent()
                                            && actual.aggregation().get().groupingSets().equals(ImmutableList.of(ImmutableList.of()))
                                            && actual.aggregation().get().accumulators().size() == 2;
                                },
                                TupleDomain.all(),
                                ImmutableMap.of())));
    }

    @Test
    public void testGroupingAggregationPushdown()
    {
        String tableName = "test_grouping_aggregation_" + randomNameSuffix();
        getPlanTester().executeStatement("CREATE TABLE " + tableName + " AS SELECT VARCHAR 'APAC' AS region, BIGINT '10' AS val");

        assertPlan(
                "SELECT region, count(*), avg(val) FROM " + tableName + " GROUP BY region",
                anyTree(
                        tableScan(
                                table -> {
                                    MongoTableHandle actual = (MongoTableHandle) table;
                                    return actual.aggregation().isPresent()
                                            && actual.aggregation().get().groupingSets().size() == 1
                                            && actual.aggregation().get().groupingSets().get(0).size() == 1
                                            && actual.aggregation().get().groupingSets().get(0).get(0).baseName().equals("region")
                                            && actual.aggregation().get().accumulators().size() == 2;
                                },
                                TupleDomain.all(),
                                ImmutableMap.of())));
    }

    @Test
    public void testGroupByWithoutAggregatesPushdown()
    {
        String tableName = "test_group_by_no_agg_" + randomNameSuffix();
        getPlanTester().executeStatement("CREATE TABLE " + tableName + " AS SELECT VARCHAR 'APAC' AS region");

        assertPlan(
                "SELECT region FROM " + tableName + " GROUP BY region",
                anyTree(
                        tableScan(
                                table -> {
                                    MongoTableHandle actual = (MongoTableHandle) table;
                                    return actual.aggregation().isPresent()
                                            && actual.aggregation().get().groupingSets().size() == 1
                                            && actual.aggregation().get().groupingSets().get(0).size() == 1
                                            && actual.aggregation().get().groupingSets().get(0).get(0).baseName().equals("region")
                                            && actual.aggregation().get().accumulators().isEmpty();
                                },
                                TupleDomain.all(),
                                ImmutableMap.of())));
    }

    @Test
    public void testAggregationPushdownDisabled()
    {
        String tableName = "test_pushdown_disabled_" + randomNameSuffix();
        getPlanTester().executeStatement("CREATE TABLE " + tableName + " AS SELECT BIGINT '10' AS val");

        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setCatalogSessionProperty(CATALOG, "aggregation_pushdown_enabled", "false")
                .build();

        assertPlan(
                "SELECT count(*), sum(val) FROM " + tableName,
                session,
                anyTree(
                        node(
                                AggregationNode.class,
                                tableScan(tableName, ImmutableMap.of("val", "val")))));
    }

    @Test
    public void testMultiGroupingSetsNotPushedDown()
    {
        String tableName = "test_cube_" + randomNameSuffix();
        getPlanTester().executeStatement("CREATE TABLE " + tableName + " AS SELECT VARCHAR 'APAC' AS a, BIGINT '10' AS b");

        assertPlan(
                "SELECT a, b, count(*) FROM " + tableName + " GROUP BY CUBE(a, b)",
                anyTree(
                        node(
                                AggregationNode.class,
                                anyTree(tableScan(tableName, ImmutableMap.of("a", "a", "b", "b"))))));
    }

    @Test
    public void testDistinctAggregationNotPushedDown()
    {
        String tableName = "test_distinct_" + randomNameSuffix();
        getPlanTester().executeStatement("CREATE TABLE " + tableName + " AS SELECT VARCHAR 'APAC' AS a");

        assertPlan(
                "SELECT count(DISTINCT a) FROM " + tableName,
                anyTree(
                        node(
                                AggregationNode.class,
                                anyTree(tableScan(tableName, ImmutableMap.of("a", "a"))))));
    }
}
