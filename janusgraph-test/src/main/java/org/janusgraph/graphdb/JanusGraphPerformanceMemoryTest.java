// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.TestCategory;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.util.Profiler;
import org.janusgraph.testutil.JUnitBenchmarkProvider;
import org.janusgraph.testutil.MemoryAssess;
import org.junit.Rule;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.rules.TestRule;

import static org.janusgraph.testutil.JanusGraphAssert.assertCount;
import static org.junit.jupiter.api.Assertions.assertTrue;
/**
 * These tests focus on the in-memory data structures of individual transactions and how they hold up to high memory pressure
 */
@Tag(TestCategory.MEMORY_TESTS)
public abstract class JanusGraphPerformanceMemoryTest extends JanusGraphBaseTest {

    @Rule
    public TestRule benchmark = JUnitBenchmarkProvider.get();

    @Test
    public void testMemoryLeakage() {
        long memoryBaseline = 0;
        SummaryStatistics stats = new SummaryStatistics();
        int numRuns = 25;
        for (int r = 0; r < numRuns; r++) {
            if (r == 1 || r == (numRuns - 1)) {
                memoryBaseline = MemoryAssess.getMemoryUse();
                stats.addValue(memoryBaseline);
                //System.out.println("Memory before run "+(r+1)+": " + memoryBaseline / 1024 + " KB");
            }
            for (int t = 0; t < 1000; t++) {
                graph.addVertex();
                graph.tx().rollback();
                JanusGraphTransaction tx = graph.newTransaction();
                tx.addVertex();
                tx.rollback();
            }
            if (r == 1 || r == (numRuns - 1)) {
                memoryBaseline = MemoryAssess.getMemoryUse();
                stats.addValue(memoryBaseline);
                //System.out.println("Memory after run " + (r + 1) + ": " + memoryBaseline / 1024 + " KB");
            }
            clopen();
        }
        System.out.println("Average: " + stats.getMean() + " Std. Dev: " + stats.getStandardDeviation());
        assertTrue(stats.getStandardDeviation() < stats.getMin());
    }

    @Test
    public void testTransactionalMemory() throws Exception {
        makeVertexIndexedUniqueKey("uid",Long.class);
        makeKey("name",String.class);

        PropertyKey time = makeKey("time",Integer.class);
        mgmt.makeEdgeLabel("friend").signature(time).directed().make();
        finishSchema();

        final Random random = new Random();
        final int rounds = 10;
        final int commitSize = 2000;
        final int threads = 1;
        final AtomicInteger uidCounter = new AtomicInteger(0);

        long start = System.currentTimeMillis();
        System.out.println("statrting writes");


        Thread[] writeThreads = new Thread[threads];
        for (int t = 0; t < writeThreads.length; t++) {
            writeThreads[t] = new Thread(() -> {
                for (int r = 0; r < rounds; r++) {
                    JanusGraphTransaction tx = graph.newTransaction();
                    JanusGraphVertex previous = null;
                    for (int c = 0; c < commitSize; c++) {
                        JanusGraphVertex v = tx.addVertex();
                        long uid = uidCounter.incrementAndGet();
                        v.property(VertexProperty.Cardinality.single, "uid",  uid);
                        v.property(VertexProperty.Cardinality.single, "name",  "user" + uid);
                        if (previous != null) {
                            v.addEdge("friend", previous, "time", Math.abs(random.nextInt()));
                        }
                        previous = v;
                    }
                    tx.commit();
                }
            });
            writeThreads[t].start();
        }
        for (final Thread writeThread : writeThreads) {
            writeThread.join();
        }


        for (int r = 0; r < rounds; r++) {
                    JanusGraphTransaction tx = graph.newTransaction();
                    JanusGraphVertex previous = null;
                    for (int c = 0; c < commitSize; c++) {
                        JanusGraphVertex v = tx.addVertex();
                        long uid = uidCounter.incrementAndGet();
                        v.property(VertexProperty.Cardinality.single, "uid",  uid);
                        v.property(VertexProperty.Cardinality.single, "name",  "user" + uid);
                        if (previous != null) {
                            v.addEdge("friend", previous, "time", Math.abs(random.nextInt()));
                        }
                        previous = v;
                    }
                    tx.commit();
        }

        System.out.println("Write time for " + (rounds * commitSize * threads) + " vertices & edges: " + (System.currentTimeMillis() - start));
        Profiler.printTimes();
        Profiler.clear();


        final int maxUID = uidCounter.get();
        final int trials = 20000;
        final String fixedName = "john";


        JanusGraphTransaction tx = graph.newTransaction();



        start = System.currentTimeMillis();
        long randomUniqueId = random.nextInt(maxUID) + 1;
        getVertex(tx,"uid", randomUniqueId).property(VertexProperty.Cardinality.single, "name",  fixedName);
        Profiler.updateFromCurrentTime("readTest::updateVertexProperty", start);
        for (int t1 = 1; t1 <= trials; t1++) {
            long start2 = System.currentTimeMillis();
            //System.out.println("fetching vertex");
            //JanusGraphVertex v = getVertex(tx,"uid", random.nextInt(maxUID) + 1);
            Vertex v = Iterables.getOnlyElement(() -> tx.traversal().V().has("uid", random.nextInt(maxUID) + 1));
            Profiler.updateFromCurrentTime("readTest::fetchVertex", start2);

            //System.out.println("fetching properties");
            long start3 = System.currentTimeMillis();
            assertCount(2, v.properties());
            Profiler.updateFromCurrentTime("readTest::fetchVertexProperties", start3);

            int count = 0;
            long start4 = System.currentTimeMillis();

/*
            List<Edge> collect = Stream.concat(
                    tx.traversal().V(v.id()).inE().toStream(),
                    tx.traversal().V(v.id()).outE().toStream())
                    .collect(Collectors.toList());
            Profiler.updateFromCurrentTime("readTest::edgeQuery", start4);

 */

            //System.out.println("fetching edges");
            //Iterable<JanusGraphEdge> edgeIter = v.query().direction(Direction.BOTH).edges();
            Iterator<Edge> edgeIter = v.edges(Direction.BOTH);
            Profiler.updateFromCurrentTime("readTest::buildVertexQuery", start4);
            Set<Edge> edges = Sets.newHashSet(edgeIter);
            Profiler.updateFromCurrentTime("readTest::executeVertexQuery", start4);
            for (Object e : edges) {
                count++;
                long start5 = System.currentTimeMillis();
                //System.out.println("fetching edge values");
                assertTrue(((JanusGraphEdge) e).<Integer>value("time") >= 0);
                Profiler.updateFromCurrentTime("readTest::fetchEdgeValue", start5);
            }
            //System.out.println();
            assertTrue(count <= 2);
        }


        /*
        Thread[] readThreads = new Thread[threads];
        for (int t = 0; t < readThreads.length; t++) {
            readThreads[t] = new Thread(() -> {
                JanusGraphTransaction tx = graph.newTransaction();
                long randomUniqueId = random.nextInt(maxUID) + 1;
                getVertex(tx,"uid", randomUniqueId).property(VertexProperty.Cardinality.single, "name",  fixedName);
                for (int t1 = 1; t1 <= trials; t1++) {
                    JanusGraphVertex v = getVertex(tx,"uid", random.nextInt(maxUID) + 1);
                    assertCount(2, v.properties());
                    int count = 0;
                    for (Object e : v.query().direction(Direction.BOTH).edges()) {
                        count++;
                        assertTrue(((JanusGraphEdge) e).<Integer>value("time") >= 0);
                    }
                    assertTrue(count <= 2);
//                        if (t%(trials/10)==0) System.out.println(t);

                }
                assertEquals(getVertex(tx,"uid", randomUniqueId).value("name"), fixedName);
                tx.commit();
            });
            readThreads[t].start();
        }
        for (final Thread readThread : readThreads) {
            readThread.join();
        }
        */

        System.out.println("Read time for " + (trials * threads) + " vertex lookups: " + (System.currentTimeMillis() - start));
        Profiler.printTimes();



    }

    @Test
    public void testGraknAccessPatterns() throws Exception {
        makeVertexIndexedUniqueKey("uid",Long.class);
        makeKey("name",String.class);

        PropertyKey time = makeKey("time",Integer.class);
        mgmt.makeEdgeLabel("friend").signature(time).directed().make();
        finishSchema();

        final Random random = new Random();
        final int rounds = 10;
        final int commitSize = 2000;
        final int threads = 1;
        final AtomicInteger uidCounter = new AtomicInteger(0);

        List<Object> ids = new ArrayList<>();
        long start = System.currentTimeMillis();
        for (int r = 0; r < rounds; r++) {
            JanusGraphTransaction tx = graph.newTransaction();
            for (int c = 0; c < commitSize; c++) {
                JanusGraphVertex vertex = tx.addVertex("vertexType");
                long uid = uidCounter.incrementAndGet();
                vertex.property("uid", uid);
                vertex.property("index", "value");
            }
            tx.commit();
        }

        System.out.println("Write time for " + (rounds * commitSize * threads) + " vertices & edges: " + (System.currentTimeMillis() - start));
        Profiler.printTimes();
        Profiler.clear();

        final int maxUID = uidCounter.get();
        final int trials = 20000;
        final String fixedName = "john";

        JanusGraphTransaction tx = graph.newTransaction();

        start = System.currentTimeMillis();
        for (int t1 = 1; t1 <= trials; t1++) {
            long start2 = System.currentTimeMillis();
            JanusGraphVertex v = getVertex(tx,"uid", random.nextInt(maxUID) + 1);
            Profiler.updateFromCurrentTime("readTest::fetchVertex", start2);

            long start3 = System.currentTimeMillis();
            assertCount(2, v.properties());
            Profiler.updateFromCurrentTime("readTest::fetchVertexProperties", start3);

            int count = 0;
            long start4 = System.currentTimeMillis();
            Set<JanusGraphEdge> edges = Sets.newHashSet(v.query().direction(Direction.BOTH).edges());
            Profiler.updateFromCurrentTime("readTest::fetchEdges", start4);
            for (Object e : edges) {
                count++;
                long start5 = System.currentTimeMillis();
                assertTrue(((JanusGraphEdge) e).<Integer>value("time") >= 0);
                Profiler.updateFromCurrentTime("readTest::fetchEdgeValue", start5);
            }
            assertTrue(count <= 2);
        }

        System.out.println("Read time for " + (trials * threads) + " vertex lookups: " + (System.currentTimeMillis() - start));
        Profiler.printTimes();

    }
}


