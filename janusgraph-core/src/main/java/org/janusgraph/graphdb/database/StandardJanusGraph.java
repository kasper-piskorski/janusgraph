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

package org.janusgraph.graphdb.database;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.cache.KCVSCache;
import org.janusgraph.diskstorage.log.Log;
import org.janusgraph.diskstorage.log.Message;
import org.janusgraph.diskstorage.log.ReadMarker;
import org.janusgraph.diskstorage.log.kcvs.KCVSLog;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.cache.SchemaCache;
import org.janusgraph.graphdb.database.idassigner.VertexIDAssigner;
import org.janusgraph.graphdb.database.idhandling.IDHandler;
import org.janusgraph.graphdb.database.log.LogTxStatus;
import org.janusgraph.graphdb.database.log.TransactionLogHeader;
import org.janusgraph.graphdb.database.management.ManagementLogger;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.internal.InternalVertexLabel;
import org.janusgraph.graphdb.query.QueryUtil;
import org.janusgraph.graphdb.relations.EdgeDirection;
import org.janusgraph.graphdb.tinkerpop.JanusGraphFeatures;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistryV1d0;
import org.janusgraph.graphdb.tinkerpop.JanusGraphVariables;
import org.janusgraph.graphdb.tinkerpop.optimize.AdjacentVertexFilterOptimizerStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphIoRegistrationStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphLocalQueryOptimizerStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphStepStrategy;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.transaction.StandardTransactionBuilder;
import org.janusgraph.graphdb.transaction.TransactionConfiguration;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.BaseRelationType;
import org.janusgraph.graphdb.types.vertices.JanusGraphSchemaVertex;
import org.janusgraph.util.system.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REGISTRATION_TIME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REPLACE_INSTANCE_IF_EXISTS;

public class StandardJanusGraph implements JanusGraph {

    private static final Logger LOG = LoggerFactory.getLogger(StandardJanusGraph.class);

    // Filters used to retain Schema vertices(SCHEMA_FILTER), NOT Schema vertices(NO_SCHEMA_FILTER) and to not filter anything (NO_FILTER).
    private static final Predicate<InternalRelation> SCHEMA_FILTER = internalRelation -> internalRelation.getType() instanceof BaseRelationType && internalRelation.getVertex(0) instanceof JanusGraphSchemaVertex;
    private static final Predicate<InternalRelation> NO_SCHEMA_FILTER = internalRelation -> !SCHEMA_FILTER.test(internalRelation);
    private static final Predicate<InternalRelation> NO_FILTER = internalRelation -> true;

    static {
        TraversalStrategies graphStrategies = TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone()
                .addStrategies(AdjacentVertexFilterOptimizerStrategy.instance(),
                        JanusGraphLocalQueryOptimizerStrategy.instance(), JanusGraphStepStrategy.instance(),
                        JanusGraphIoRegistrationStrategy.instance());

        //Register with cache
        TraversalStrategies.GlobalCache.registerStrategies(StandardJanusGraph.class, graphStrategies);
        TraversalStrategies.GlobalCache.registerStrategies(StandardJanusGraphTx.class, graphStrategies);
    }

    private final GraphDatabaseConfiguration config;
    private final Backend backend;
    private final IDManager idManager;
    private final VertexIDAssigner idAssigner;
    private final TimestampProvider timestampProvider;

    //Serializers
    private final IndexSerializer indexSerializer;
    private final EdgeSerializer edgeSerializer;
    protected final Serializer serializer;

    //Caches
    public final SliceQuery vertexExistenceQuery;
    private final RelationQueryCache queryCache;
    private final SchemaCache schemaCache;

    //Log
    private final ManagementLogger managementLogger;

    private volatile boolean isOpen;
    private final AtomicLong txCounter; // used to generate unique transaction IDs

    private final Set<StandardJanusGraphTx> openTransactions;

    private final String name;
    private final Thread shutdownThread;

    private final AutomaticLocalTinkerTransaction tinkerTransaction = new AutomaticLocalTinkerTransaction();

    public StandardJanusGraph(GraphDatabaseConfiguration configuration, Backend backend) {

        this.config = configuration;
        this.name = configuration.getGraphName();
        this.isOpen = true;
        this.txCounter = new AtomicLong(0);
        this.openTransactions = Collections.newSetFromMap(new ConcurrentHashMap<>(100, 0.75f, 1));

        // Collaborators:
        this.backend = backend;
        this.idAssigner = new VertexIDAssigner(config.getConfiguration(), backend.getStoreManager(), backend.getStoreFeatures());
        this.idManager = idAssigner.getIDManager();
        this.timestampProvider = configuration.getTimestampProvider();


        // Collaborators (Serializers)
        this.serializer = config.getSerializer();
        StoreFeatures storeFeatures = backend.getStoreFeatures();
        this.indexSerializer = new IndexSerializer(configuration.getConfiguration(), this.serializer, this.backend.getIndexInformation(), storeFeatures.isDistributed() && storeFeatures.isKeyOrdered());
        this.edgeSerializer = new EdgeSerializer(this.serializer);
        this.vertexExistenceQuery = edgeSerializer.getQuery(BaseKey.VertexExists, Direction.OUT, new EdgeSerializer.TypedInterval[0]).setLimit(1);

        // Collaborators (Caches)
        this.queryCache = new RelationQueryCache(this.edgeSerializer);
        this.schemaCache = configuration.getTypeCache(typeCacheRetrieval);


        // Transaction(?) Log Manager
        Log managementLog = backend.getSystemMgmtLog();
        this.managementLogger = new ManagementLogger(this, managementLog, schemaCache, this.timestampProvider);
        managementLog.registerReader(ReadMarker.fromNow(), this.managementLogger);


        //Register instance and ensure uniqueness
        String uniqueInstanceId = configuration.getUniqueGraphId();
        ModifiableConfiguration globalConfig = getGlobalSystemConfig(backend);
        boolean instanceExists = globalConfig.has(REGISTRATION_TIME, uniqueInstanceId);
        boolean replaceExistingInstance = configuration.getConfiguration().get(REPLACE_INSTANCE_IF_EXISTS);
        if (instanceExists) {
            if (!replaceExistingInstance) {
                throw new JanusGraphException(String.format("A JanusGraph graph with the same instance id [%s] is already open. Might required forced shutdown.", uniqueInstanceId));
            } else {
                LOG.debug(String.format("Instance [%s] already exists. Opening the graph per " + REPLACE_INSTANCE_IF_EXISTS.getName() + " configuration.", uniqueInstanceId));
            }
        }
        globalConfig.set(REGISTRATION_TIME, timestampProvider.getTime(), uniqueInstanceId);

        shutdownThread = new Thread(this::closeInternal, "StandardJanusGraph-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownThread);
    }

    // Get JanusTransaction which is wrapped inside the TinkerTransaction
    // it opens the JanusTransaction if not initialised yet
    private StandardJanusGraphTx getLocalJanusTransaction() {
        if (!tinkerTransaction.isOpen()) {
            tinkerTransaction.readWrite(); // creates a new JanusGraphTransaction internally (inside AutomaticLocalTinkerTransaction)
        }
        StandardJanusGraphTx tx = tinkerTransaction.getJanusTransaction();
        Preconditions.checkNotNull(tx, "Invalid read-write behavior configured: Should either open transaction or throw exception.");
        return tx;
    }

    //This returns the JanusTransaction contained inside tinkerTransaction -> it opens/creates a new one if the previous one is closed!
    public JanusGraphTransaction getCurrentThreadTx() {
        return getLocalJanusTransaction();
    }

    @Override
    public Transaction tx() {
        return tinkerTransaction;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, getConfiguration().getBackendDescription());
    }

    public Variables variables() {
        return new JanusGraphVariables(getBackend().getUserConfiguration());
    }

    @Override
    public org.apache.commons.configuration.Configuration configuration() {
        return getConfiguration().getConfigurationAtOpen();
    }

    @Override
    public <I extends Io> I io(Io.Builder<I> builder) {
        if (builder.requiresVersion(GryoVersion.V1_0) || builder.requiresVersion(GraphSONVersion.V1_0)) {
            return (I) builder.graph(this).onMapper(mapper -> mapper.addRegistry(JanusGraphIoRegistryV1d0.getInstance())).create();
        } else if (builder.requiresVersion(GraphSONVersion.V2_0)) {
            return (I) builder.graph(this).onMapper(mapper -> mapper.addRegistry(JanusGraphIoRegistry.getInstance())).create();
        } else {
            return (I) builder.graph(this).onMapper(mapper -> mapper.addRegistry(JanusGraphIoRegistry.getInstance())).create();
        }
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        return null; //TODO, yeah...
    }

    // ########## TRANSACTIONAL FORWARDING ###########################

    @Override
    public JanusGraphVertex addVertex(Object... keyValues) {
        return getLocalJanusTransaction().addVertex(keyValues);
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        return getLocalJanusTransaction().vertices(vertexIds);
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        return getLocalJanusTransaction().edges(edgeIds);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return null; // TODO possibly think about this in the future.
    }

    @Override
    public JanusGraphVertex addVertex(String vertexLabel) {
        return getLocalJanusTransaction().addVertex(vertexLabel);
    }

    // Tinkerpop wrapper around StandardJanusGraph transaction, automatic transaction used by the graph
    // when user cannot be bother to use manual/explicit transactions,
    // i.e. when user wants to do graph.addVertex(); graph.tx().commit();
    // rather than tx = graph.newTransaction(); tx.addVertex(); tx.commit();
    // This wrapper internally uses the thread-local Janusgraph transaction.

    //This transaction is AUTOmatic in so that it does not need to be explicitly open -> it will invoke readWrite which triggers tx.open() if not open yet
    // It is also automatic because it is possible to invoke tx.close() directly without first explicitly invoking commit or rollback,
    // when invoking close directly, the tx will internally trigger tx.rollback.
    // Also this is public just so that we can use it in tests
    public class AutomaticLocalTinkerTransaction extends AbstractThreadLocalTransaction {

        private ThreadLocal<StandardJanusGraphTx> localJanusTransaction = ThreadLocal.withInitial(() -> null);

        AutomaticLocalTinkerTransaction() {
            super(StandardJanusGraph.this);
        }

        public StandardJanusGraphTx getJanusTransaction() {
            return localJanusTransaction.get();
        }

        @Override
        public void doOpen() {
            StandardJanusGraphTx tx = localJanusTransaction.get();
            if (tx != null && tx.isOpen()) throw Transaction.Exceptions.transactionAlreadyOpen();
            tx = (StandardJanusGraphTx) newThreadBoundTransaction();
            localJanusTransaction.set(tx);
        }

        @Override
        public void doCommit() {
            localJanusTransaction.get().commit();
        }

        @Override
        public void doRollback() {
            localJanusTransaction.get().rollback();
        }

        @Override
        public JanusGraphTransaction createThreadedTx() {
            return newTransaction();
        }

        @Override
        public boolean isOpen() {
            if (StandardJanusGraph.this.isClosed()) {
                // Graph has been closed
                return false;
            }
            StandardJanusGraphTx tx = localJanusTransaction.get();
            return tx != null && tx.isOpen();
        }

        @Override
        protected void doClose() {
            super.doClose();
            transactionListeners.remove();
            localJanusTransaction.remove();
        }

        @Override
        public Transaction onReadWrite(Consumer<Transaction> transactionConsumer) {
            if (StandardJanusGraph.this.isClosed()) { //cannot start a transaction if the enclosing graph is closed
                throw new IllegalStateException("Graph has been closed");
            }
            Preconditions.checkArgument(transactionConsumer instanceof READ_WRITE_BEHAVIOR, "Only READ_WRITE_BEHAVIOR instances are accepted argument, got: %s", transactionConsumer);
            return super.onReadWrite(transactionConsumer);
        }

        @Override
        public Transaction onClose(Consumer<Transaction> transactionConsumer) {
            Preconditions.checkArgument(transactionConsumer instanceof CLOSE_BEHAVIOR, "Only CLOSE_BEHAVIOR instances are accepted argument, got: %s", transactionConsumer);
            return super.onClose(transactionConsumer);
        }
    }

    public String getGraphName() {
        return this.name;
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public boolean isClosed() {
        return !isOpen();
    }

    @Override
    public synchronized void close() throws JanusGraphException {
        Runtime.getRuntime().removeShutdownHook(shutdownThread);
        closeInternal();
    }

    private synchronized void closeInternal() {

        if (!isOpen) return;

        Map<JanusGraphTransaction, RuntimeException> txCloseExceptions = new HashMap<>();

        try {
            //Unregister instance
            String uniqueId = null;
            try {
                uniqueId = config.getUniqueGraphId();
                ModifiableConfiguration globalConfig = getGlobalSystemConfig(backend);
                globalConfig.remove(REGISTRATION_TIME, uniqueId);
            } catch (Exception e) {
                LOG.warn("Unable to remove graph instance uniqueid {}", uniqueId, e);
            }

            /* Assuming a couple of properties about openTransactions:
             * 1. no concurrent modifications during graph shutdown
             * 2. all contained localJanusTransaction are open
             */
            for (StandardJanusGraphTx otx : openTransactions) {
                try {
                    otx.rollback();
                    otx.close();
                } catch (RuntimeException e) {
                    // Catch and store these exceptions, but proceed with the loop
                    // Any remaining localJanusTransaction on the iterator should get a chance to close before we throw up
                    LOG.warn("Unable to close transaction {}", otx, e);
                    txCloseExceptions.put(otx, e);
                }
            }
            tinkerTransaction.close();

            IOUtils.closeQuietly(idAssigner);
            IOUtils.closeQuietly(backend);
            IOUtils.closeQuietly(queryCache);
            IOUtils.closeQuietly(serializer);
        } finally {
            isOpen = false;
        }

        // Throw an exception if at least one transaction failed to close
        if (1 == txCloseExceptions.size()) {
            // TP3's test suite requires that this be of type ISE
            throw new IllegalStateException("Unable to close transaction", Iterables.getOnlyElement(txCloseExceptions.values()));
        } else if (1 < txCloseExceptions.size()) {
            throw new IllegalStateException(String.format("Unable to close %s transactions (see warnings in log output for details)",
                    txCloseExceptions.size()));
        }
    }

    // ################### Simple Getters #########################

    @Override
    public Features features() {
        return JanusGraphFeatures.getFeatures(this, backend.getStoreFeatures());
    }

    public IndexSerializer getIndexSerializer() {
        return indexSerializer;
    }

    public Backend getBackend() {
        return backend;
    }

    public IDManager getIDManager() {
        return idManager;
    }

    public EdgeSerializer getEdgeSerializer() {
        return edgeSerializer;
    }

    public Serializer getDataSerializer() {
        return serializer;
    }

    public SchemaCache getSchemaCache() {
        return schemaCache;
    }

    public GraphDatabaseConfiguration getConfiguration() {
        return config;
    }

    @Override
    public JanusGraphManagement openManagement() {
        return new ManagementSystem(this, backend.getGlobalSystemConfig(), backend.getSystemMgmtLog(), managementLogger, schemaCache);
    }

    public Set<? extends JanusGraphTransaction> getOpenTransactions() {
        return Sets.newHashSet(openTransactions);
    }

    // ################### TRANSACTIONS #########################

    @Override
    public JanusGraphTransaction newTransaction() {
        return buildTransaction()/*.consistencyChecks(false)*/.start();
    }

    @Override
    public StandardTransactionBuilder buildTransaction() {
        return new StandardTransactionBuilder(getConfiguration(), this);
    }

    public StandardJanusGraphTx newThreadBoundTransaction() {
        return buildTransaction().threadBound().start();
    }

    public StandardJanusGraphTx newTransaction(TransactionConfiguration configuration) {
        if (!isOpen) {
            throw new IllegalStateException("Graph has been shut down");
        }
        StandardJanusGraphTx tx = new StandardJanusGraphTx(this, configuration);
        openTransactions.add(tx);
        return tx;
    }

    // This in only used from StandardJanusGraphTx (that's why public when it's really a private method) for an awkward initialisation that should be fixed in the future
    public BackendTransaction openBackendTransaction(StandardJanusGraphTx tx) {
        try {
            IndexSerializer.IndexInfoRetriever retriever = indexSerializer.getIndexInfoRetriever(tx);
            return backend.beginTransaction(tx.getConfiguration(), retriever);
        } catch (BackendException e) {
            throw new JanusGraphException("Could not start new transaction", e);
        }
    }

    public void closeTransaction(StandardJanusGraphTx tx) {
        openTransactions.remove(tx);
    }

    // ################### READ #########################

    private final SchemaCache.StoreRetrieval typeCacheRetrieval = new SchemaCache.StoreRetrieval() {

        @Override
        public Long retrieveSchemaByName(String typeName) {
            // Get a consistent tx
            Configuration customTxOptions = backend.getStoreFeatures().getKeyConsistentTxConfig();
            StandardJanusGraphTx consistentTx = null;
            try {
                consistentTx = StandardJanusGraph.this.newTransaction(new StandardTransactionBuilder(getConfiguration(),
                        StandardJanusGraph.this, customTxOptions).groupName(GraphDatabaseConfiguration.METRICS_SCHEMA_PREFIX_DEFAULT));
                consistentTx.getBackendTransaction().disableCache();
                JanusGraphVertex v = Iterables.getOnlyElement(QueryUtil.getVertices(consistentTx, BaseKey.SchemaName, typeName), null);
                return v != null ? v.longId() : null;
            } finally {
                try {
                    if (consistentTx != null) {
                        consistentTx.rollback();
                    }
                } catch (Throwable t) {
                    LOG.warn("Unable to rollback transaction", t);
                }
            }
        }

        @Override
        public EntryList retrieveSchemaRelations(long schemaId, BaseRelationType type, Direction dir) {
            SliceQuery query = queryCache.getQuery(type, dir);
            Configuration customTxOptions = backend.getStoreFeatures().getKeyConsistentTxConfig();
            StandardJanusGraphTx consistentTx = null;
            try {
                consistentTx = StandardJanusGraph.this.newTransaction(new StandardTransactionBuilder(getConfiguration(),
                        StandardJanusGraph.this, customTxOptions).groupName(GraphDatabaseConfiguration.METRICS_SCHEMA_PREFIX_DEFAULT));
                consistentTx.getBackendTransaction().disableCache();
                return edgeQuery(schemaId, query, consistentTx.getBackendTransaction());
            } finally {
                try {
                    if (consistentTx != null) {
                        consistentTx.rollback();
                    }
                } catch (Throwable t) {
                    LOG.warn("Unable to rollback transaction", t);
                }
            }
        }

    };

    public RecordIterator<Long> getVertexIDs(BackendTransaction tx) {
        Preconditions.checkArgument(backend.getStoreFeatures().hasOrderedScan() || backend.getStoreFeatures().hasUnorderedScan(),
                "The configured storage backend does not support global graph operations - use Faunus instead");

        KeyIterator keyIterator;
        if (backend.getStoreFeatures().hasUnorderedScan()) {
            keyIterator = tx.edgeStoreKeys(vertexExistenceQuery);
        } else {
            keyIterator = tx.edgeStoreKeys(new KeyRangeQuery(IDHandler.MIN_KEY, IDHandler.MAX_KEY, vertexExistenceQuery));
        }

        return new RecordIterator<Long>() {

            @Override
            public boolean hasNext() {
                return keyIterator.hasNext();
            }

            @Override
            public Long next() {
                return idManager.getKeyID(keyIterator.next());
            }

            @Override
            public void close() throws IOException {
                keyIterator.close();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Removal not supported");
            }
        };
    }

    public EntryList edgeQuery(long vid, SliceQuery query, BackendTransaction tx) {
        Preconditions.checkArgument(vid > 0);
        return tx.edgeStoreQuery(new KeySliceQuery(idManager.getKey(vid), query));
    }

    public List<EntryList> edgeMultiQuery(LongArrayList vertexIdsAsLongs, SliceQuery query, BackendTransaction tx) {
        Preconditions.checkArgument(vertexIdsAsLongs != null && !vertexIdsAsLongs.isEmpty());
        List<StaticBuffer> vertexIds = new ArrayList<>(vertexIdsAsLongs.size());
        for (int i = 0; i < vertexIdsAsLongs.size(); i++) {
            Preconditions.checkArgument(vertexIdsAsLongs.get(i) > 0);
            vertexIds.add(idManager.getKey(vertexIdsAsLongs.get(i)));
        }
        Map<StaticBuffer, EntryList> result = tx.edgeStoreMultiQuery(vertexIds, query);
        List<EntryList> resultList = new ArrayList<>(result.size());
        for (StaticBuffer v : vertexIds) resultList.add(result.get(v));
        return resultList;
    }

    private ModifiableConfiguration getGlobalSystemConfig(Backend backend) {
        return new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                backend.getGlobalSystemConfig(), BasicConfiguration.Restriction.GLOBAL);
    }

    // ################### WRITE #########################

    public void assignID(InternalRelation relation) {
        idAssigner.assignID(relation);
    }

    public void assignID(InternalVertex vertex, VertexLabel label) {
        idAssigner.assignID(vertex, label);
    }

    /**
     * The TTL of a relation (edge or property) is the minimum of:
     * 1) The TTL configured of the relation type (if exists)
     * 2) The TTL configured for the label any of the relation end point vertices (if exists)
     *
     * @param rel relation to determine the TTL for
     * @return
     */
    public static int getTTL(InternalRelation rel) {
        assert rel.isNew();
        InternalRelationType baseType = (InternalRelationType) rel.getType();
        assert baseType.getBaseType() == null;
        int ttl = 0;
        Integer ettl = baseType.getTTL();
        if (ettl > 0) ttl = ettl;
        for (int i = 0; i < rel.getArity(); i++) {
            int vttl = getTTL(rel.getVertex(i));
            if (vttl > 0 && (vttl < ttl || ttl <= 0)) ttl = vttl;
        }
        return ttl;
    }

    public static int getTTL(InternalVertex v) {
        assert v.hasId();
        if (IDManager.VertexIDType.UnmodifiableVertex.is(v.longId())) {
            assert v.isNew() : "Should not be able to add relations to existing static vertices: " + v;
            return ((InternalVertexLabel) v.vertexLabel()).getTTL();
        } else return 0;
    }

    private static class ModificationSummary {

        final boolean hasModifications;
        final boolean has2iModifications;

        private ModificationSummary(boolean hasModifications, boolean has2iModifications) {
            this.hasModifications = hasModifications;
            this.has2iModifications = has2iModifications;
        }
    }

    private ModificationSummary prepareCommit(Collection<InternalRelation> addedRelations, Collection<InternalRelation> deletedRelations,
                                              Predicate<InternalRelation> filter, BackendTransaction mutator, StandardJanusGraphTx tx) throws BackendException {
        ListMultimap<Long, InternalRelation> mutations = ArrayListMultimap.create();
        ListMultimap<InternalVertex, InternalRelation> mutatedProperties = ArrayListMultimap.create();
        List<IndexSerializer.IndexUpdate> indexUpdates = Lists.newArrayList();
        //1) Collect deleted edges and their index updates and acquire edge locks
        for (InternalRelation del : Iterables.filter(deletedRelations, filter::test)) {
            Preconditions.checkArgument(del.isRemoved());
            for (int pos = 0; pos < del.getLen(); pos++) {
                InternalVertex vertex = del.getVertex(pos);
                if (pos == 0 || !del.isLoop()) {
                    if (del.isProperty()) mutatedProperties.put(vertex, del);
                    mutations.put(vertex.longId(), del);
                }
            }
            indexUpdates.addAll(indexSerializer.getIndexUpdates(del));
        }

        //2) Collect added edges and their index updates and acquire edge locks
        for (InternalRelation add : Iterables.filter(addedRelations, filter::test)) {
            Preconditions.checkArgument(add.isNew());

            for (int pos = 0; pos < add.getLen(); pos++) {
                InternalVertex vertex = add.getVertex(pos);
                if (pos == 0 || !add.isLoop()) {
                    if (add.isProperty()) mutatedProperties.put(vertex, add);
                    mutations.put(vertex.longId(), add);
                }
            }
            indexUpdates.addAll(indexSerializer.getIndexUpdates(add));
        }

        //3) Collect all index update for vertices
        for (InternalVertex v : mutatedProperties.keySet()) {
            indexUpdates.addAll(indexSerializer.getIndexUpdates(v, mutatedProperties.get(v)));
        }
        //4) Acquire index locks (deletions first)
        for (IndexSerializer.IndexUpdate update : indexUpdates) {
            if (!update.isCompositeIndex() || !update.isDeletion()) continue;
            CompositeIndexType iIndex = (CompositeIndexType) update.getIndex();
        }
        for (IndexSerializer.IndexUpdate update : indexUpdates) {
            if (!update.isCompositeIndex() || !update.isAddition()) continue;
            CompositeIndexType iIndex = (CompositeIndexType) update.getIndex();
        }

        System.out.println("mutations: ");
        mutations.entries().forEach(System.out::println);
        System.out.println();

        System.out.println("indexUpdates: ");
        indexUpdates.stream().map(IndexSerializer.IndexUpdate::getEntry).forEach(System.out::println);
        System.out.println();

        //5) Add relation mutations
        for (Long vertexId : mutations.keySet()) {
            Preconditions.checkArgument(vertexId > 0, "Vertex has no id: %s", vertexId);
            List<InternalRelation> edges = mutations.get(vertexId);
            List<Entry> additions = new ArrayList<>(edges.size());
            List<Entry> deletions = new ArrayList<>(Math.max(10, edges.size() / 10));
            for (InternalRelation edge : edges) {
                InternalRelationType baseType = (InternalRelationType) edge.getType();
                assert baseType.getBaseType() == null;

                for (InternalRelationType type : baseType.getRelationIndexes()) {
                    if (type.getStatus() == SchemaStatus.DISABLED) continue;
                    for (int pos = 0; pos < edge.getArity(); pos++) {
                        if (!type.isUnidirected(Direction.BOTH) && !type.isUnidirected(EdgeDirection.fromPosition(pos)))
                            continue; //Directionality is not covered
                        if (edge.getVertex(pos).longId() == vertexId) {
                            StaticArrayEntry entry = edgeSerializer.writeRelation(edge, type, pos, tx);
                            if (edge.isRemoved()) {
                                deletions.add(entry);
                            } else {
                                Preconditions.checkArgument(edge.isNew());
                                int ttl = getTTL(edge);
                                if (ttl > 0) {
                                    entry.setMetaData(EntryMetaData.TTL, ttl);
                                }
                                additions.add(entry);
                            }
                        }
                    }
                }
            }

            StaticBuffer vertexKey = idManager.getKey(vertexId);
            mutator.mutateEdges(vertexKey, additions, deletions);
        }

        //6) Add index updates
        boolean has2iMods = false;
        for (IndexSerializer.IndexUpdate indexUpdate : indexUpdates) {
            assert indexUpdate.isAddition() || indexUpdate.isDeletion();
            if (indexUpdate.isCompositeIndex()) {
                IndexSerializer.IndexUpdate<StaticBuffer, Entry> update = indexUpdate;
                if (update.isAddition())
                    mutator.mutateIndex(update.getKey(), Lists.newArrayList(update.getEntry()), KCVSCache.NO_DELETIONS);
                else
                    mutator.mutateIndex(update.getKey(), KeyColumnValueStore.NO_ADDITIONS, Lists.newArrayList(update.getEntry()));
            } else {
                IndexSerializer.IndexUpdate<String, IndexEntry> update = indexUpdate;
                has2iMods = true;
                IndexTransaction itx = mutator.getIndexTransaction(update.getIndex().getBackingIndexName());
                String indexStore = ((MixedIndexType) update.getIndex()).getStoreName();
                if (update.isAddition())
                    itx.add(indexStore, update.getKey(), update.getEntry(), update.getElement().isNew());
                else
                    itx.delete(indexStore, update.getKey(), update.getEntry().field, update.getEntry().value, update.getElement().isRemoved());
            }
        }
        return new ModificationSummary(!mutations.isEmpty(), has2iMods);
    }

    public void commit(Collection<InternalRelation> addedRelations, Collection<InternalRelation> deletedRelations, StandardJanusGraphTx tx) {
        if (addedRelations.isEmpty() && deletedRelations.isEmpty()) {
            return;
        }
        //1. Finalise transaction
        LOG.debug("Saving transaction. Added {}, removed {}", addedRelations.size(), deletedRelations.size());
        if (!tx.getConfiguration().hasCommitTime()) tx.getConfiguration().setCommitTime(timestampProvider.getTime());
        Instant txTimestamp = tx.getConfiguration().getCommitTime();
        long transactionId = txCounter.incrementAndGet();

        //2. Assign JanusGraphVertex IDs
        if (!tx.getConfiguration().hasAssignIDsImmediately()) {
            idAssigner.assignIDs(addedRelations);
        }

        //3. Commit
        BackendTransaction mutator = tx.getBackendTransaction();
        boolean hasTxIsolation = backend.getStoreFeatures().hasTxIsolation();
        boolean logTransaction = config.hasLogTransactions() && !tx.getConfiguration().hasEnabledBatchLoading();
        KCVSLog txLog = logTransaction ? backend.getSystemTxLog() : null;
        TransactionLogHeader txLogHeader = new TransactionLogHeader(transactionId, txTimestamp, timestampProvider);
        ModificationSummary commitSummary;

        try {
            //3.1 Log transaction (write-ahead log) if enabled
            if (logTransaction) {
                //[FAILURE] Inability to log transaction fails the transaction by escalation since it's likely due to unavailability of primary
                //storage backend.
                Preconditions.checkNotNull(txLog, "Transaction log is null");
                txLog.add(txLogHeader.serializeModifications(serializer, LogTxStatus.PRECOMMIT, tx, addedRelations, deletedRelations), txLogHeader.getLogKey());
            }

            //3.2 Commit schema elements and their associated relations in a separate transaction if backend does not support
            //    transactional isolation
            boolean hasSchemaElements = !Iterables.isEmpty(Iterables.filter(deletedRelations, SCHEMA_FILTER::test)) || !Iterables.isEmpty(Iterables.filter(addedRelations, SCHEMA_FILTER::test));

            if (hasSchemaElements && !hasTxIsolation) {
                /*
                 * On storage without transactional isolation, create separate
                 * backend transaction for schema aspects to make sure that
                 * those are persisted prior to and independently of other
                 * mutations in the tx. If the storage supports transactional
                 * isolation, then don't create a separate tx.
                 */
                BackendTransaction schemaMutator = openBackendTransaction(tx);

                try {
                    //[FAILURE] If the preparation throws an exception abort directly - nothing persisted since batch-loading cannot be enabled for schema elements
                    commitSummary = prepareCommit(addedRelations, deletedRelations, SCHEMA_FILTER, schemaMutator, tx);
                    //System.out.println("#############SCHEMA COMMIT#################");
                    assert commitSummary.hasModifications && !commitSummary.has2iModifications;
                } catch (Throwable e) {
                    //Roll back schema tx and escalate exception
                    schemaMutator.rollback();
                    throw e;
                }

                try {
                    schemaMutator.commit();
                } catch (Throwable e) {
                    //[FAILURE] Primary persistence failed => abort and escalate exception, nothing should have been persisted
                    LOG.error("Could not commit transaction [" + transactionId + "] due to storage exception in system-commit", e);
                    throw e;
                }
            }

            //[FAILURE] Exceptions during preparation here cause the entire transaction to fail on transactional systems
            //or just the non-system part on others. Nothing has been persisted unless batch-loading
            commitSummary = prepareCommit(addedRelations, deletedRelations, hasTxIsolation ? NO_FILTER : NO_SCHEMA_FILTER, mutator, tx);
            System.out.println("#############INSTANCE COMMIT#################");
            if (commitSummary.hasModifications) {
                String logTxIdentifier = tx.getConfiguration().getLogIdentifier();
                boolean hasSecondaryPersistence = logTxIdentifier != null || commitSummary.has2iModifications;

                //1. Commit storage - failures lead to immediate abort

                //1a. Add success message to tx log which will be committed atomically with all transactional changes so that we can recover secondary failures
                //    This should not throw an exception since the mutations are just cached. If it does, it will be escalated since its critical
                if (logTransaction) {
                    txLog.add(txLogHeader.serializePrimary(serializer,
                            hasSecondaryPersistence ? LogTxStatus.PRIMARY_SUCCESS : LogTxStatus.COMPLETE_SUCCESS),
                            txLogHeader.getLogKey(), mutator.getTxLogPersistor());
                }

                try {
                    mutator.commitStorage();
                } catch (Throwable e) {
                    //[FAILURE] If primary storage persistence fails abort directly (only schema could have been persisted)
                    LOG.error("Could not commit transaction [" + transactionId + "] due to storage exception in commit", e);
                    throw e;
                }

                if (hasSecondaryPersistence) {
                    LogTxStatus status = LogTxStatus.SECONDARY_SUCCESS;
                    Map<String, Throwable> indexFailures = ImmutableMap.of();
                    boolean userlogSuccess = true;

                    try {
                        //2. Commit indexes - [FAILURE] all exceptions are collected and logged but nothing is aborted
                        indexFailures = mutator.commitIndexes();
                        if (!indexFailures.isEmpty()) {
                            status = LogTxStatus.SECONDARY_FAILURE;
                            for (Map.Entry<String, Throwable> entry : indexFailures.entrySet()) {
                                LOG.error("Error while committing index mutations for transaction [" + transactionId + "] on index: " + entry.getKey(), entry.getValue());
                            }
                        }
                        //3. Log transaction if configured - [FAILURE] is recorded but does not cause exception
                        if (logTxIdentifier != null) {
                            try {
                                userlogSuccess = false;
                                final Log userLog = backend.getUserLog(logTxIdentifier);
                                Future<Message> env = userLog.add(txLogHeader.serializeModifications(serializer, LogTxStatus.USER_LOG, tx, addedRelations, deletedRelations));
                                if (env.isDone()) {
                                    try {
                                        env.get();
                                    } catch (ExecutionException ex) {
                                        throw ex.getCause();
                                    }
                                }
                                userlogSuccess = true;
                            } catch (Throwable e) {
                                status = LogTxStatus.SECONDARY_FAILURE;
                                LOG.error("Could not user-log committed transaction [" + transactionId + "] to " + logTxIdentifier, e);
                            }
                        }
                    } finally {
                        if (logTransaction) {
                            //[FAILURE] An exception here will be logged and not escalated; tx considered success and
                            // needs to be cleaned up later
                            try {
                                txLog.add(txLogHeader.serializeSecondary(serializer, status, indexFailures, userlogSuccess), txLogHeader.getLogKey());
                            } catch (Throwable e) {
                                LOG.error("Could not tx-log secondary persistence status on transaction [" + transactionId + "]", e);
                            }
                        }
                    }
                } else {
                    //This just closes the transaction since there are no modifications
                    mutator.commitIndexes();
                }
            } else { //Just commit everything at once
                //[FAILURE] This case only happens when there are no non-system mutations in which case all changes
                //are already flushed. Hence, an exception here is unlikely and should abort
                mutator.commit();
            }
        } catch (Throwable e) {
            LOG.error("Could not commit transaction [" + transactionId + "] due to exception", e);
            try {
                //Clean up any left-over transaction handles
                mutator.rollback();
            } catch (Throwable e2) {
                LOG.error("Could not roll-back transaction [" + transactionId + "] after failure due to exception", e2);
            }
            if (e instanceof RuntimeException) throw (RuntimeException) e;
            else throw new JanusGraphException("Unexpected exception", e);
        }
    }
}
