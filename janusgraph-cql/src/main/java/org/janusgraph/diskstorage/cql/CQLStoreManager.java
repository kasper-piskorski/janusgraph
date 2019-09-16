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

package org.janusgraph.diskstorage.cql;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import com.datastax.oss.driver.internal.core.ssl.DefaultSslEngineFactory;
import com.google.common.annotations.VisibleForTesting;
import io.vavr.Tuple;
import io.vavr.collection.Array;
import io.vavr.collection.HashMap;
import io.vavr.collection.Iterator;
import io.vavr.collection.Seq;
import io.vavr.concurrent.Future;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData.Container;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.util.system.NetworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.truncate;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.dropKeyspace;
import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.ATOMIC_BATCH_MUTATE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.BATCH_STATEMENT_SIZE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.KEYSPACE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.LOCAL_DATACENTER;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.LOCAL_MAX_CONNECTIONS_PER_HOST;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.MAX_REQUESTS_PER_CONNECTION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.ONLY_USE_LOCAL_CONSISTENCY_FOR_SYSTEM_OPERATIONS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.PROTOCOL_VERSION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.READ_CONSISTENCY;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REMOTE_MAX_CONNECTIONS_PER_HOST;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REPLICATION_FACTOR;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REPLICATION_OPTIONS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REPLICATION_STRATEGY;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SESSION_NAME;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_TRUSTSTORE_LOCATION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_TRUSTSTORE_PASSWORD;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.USE_EXTERNAL_LOCKING;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.WRITE_CONSISTENCY;
import static org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore.EXCEPTION_MAPPER;
import static org.janusgraph.diskstorage.cql.CQLTransaction.getTransaction;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.AUTH_PASSWORD;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.AUTH_USERNAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DROP_ON_CLEAR;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.METRICS_PREFIX;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_PORT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.buildGraphConfiguration;

/**
 * This class creates see {@link CQLKeyColumnValueStore CQLKeyColumnValueStores} and handles Cassandra-backed allocation of vertex IDs for JanusGraph (when so
 * configured).
 */
public class CQLStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(CQLStoreManager.class);

    private static final String CONSISTENCY_LOCAL_QUORUM = "LOCAL_QUORUM";
    static final String CONSISTENCY_QUORUM = "QUORUM";
    private static final int DEFAULT_PORT = 9042;

    private final String keyspace;
    private final int batchSize;
    private final boolean atomicBatch;

    @Resource
    private CqlSession session;
    private final StoreFeatures storeFeatures;
    private final Map<String, CQLKeyColumnValueStore> openStores;
    private final Deployment deployment;

    /**
     * Constructor for the {@link CQLStoreManager} given a JanusGraph {@link Configuration}.
     *
     * @param configuration
     */
    public CQLStoreManager(Configuration configuration) throws PermanentBackendException {
        super(configuration, DEFAULT_PORT);
        this.keyspace = determineKeyspaceName(configuration);
        this.batchSize = configuration.get(BATCH_STATEMENT_SIZE);
        this.atomicBatch = configuration.get(ATOMIC_BATCH_MUTATE);
        this.session = initialiseSession();
        initialiseKeyspace();

        Configuration global = buildGraphConfiguration()
                .set(READ_CONSISTENCY, CONSISTENCY_QUORUM)
                .set(WRITE_CONSISTENCY, CONSISTENCY_QUORUM)
                .set(METRICS_PREFIX, METRICS_SYSTEM_PREFIX_DEFAULT);

        Configuration local = buildGraphConfiguration()
                .set(READ_CONSISTENCY, CONSISTENCY_LOCAL_QUORUM)
                .set(WRITE_CONSISTENCY, CONSISTENCY_LOCAL_QUORUM)
                .set(METRICS_PREFIX, METRICS_SYSTEM_PREFIX_DEFAULT);

        Boolean onlyUseLocalConsistency = configuration.get(ONLY_USE_LOCAL_CONSISTENCY_FOR_SYSTEM_OPERATIONS);

        Boolean useExternalLocking = configuration.get(USE_EXTERNAL_LOCKING);

        StandardStoreFeatures.Builder fb = new StandardStoreFeatures.Builder();

        fb.batchMutation(true).distributed(true);
        fb.timestamps(true).cellTTL(true);
        fb.keyConsistent((onlyUseLocalConsistency ? local : global), local);
        fb.locking(useExternalLocking);
        fb.optimisticLocking(true);
        fb.multiQuery(false);

        String partitioner = this.session.getMetadata().getTokenMap().get().getPartitionerName();
        switch (partitioner.substring(partitioner.lastIndexOf('.') + 1)) {
            case "RandomPartitioner":
            case "Murmur3Partitioner": {
                fb.keyOrdered(false).orderedScan(false).unorderedScan(true);
                deployment = Deployment.REMOTE;
                break;
            }
            case "ByteOrderedPartitioner": {
                fb.keyOrdered(true).orderedScan(true).unorderedScan(false);
                deployment = (hostnames.length == 1)// mark deployment as local only in case we have byte ordered partitioner and local
                        // connection
                        ? (NetworkUtil.isLocalConnection(hostnames[0])) ? Deployment.LOCAL : Deployment.REMOTE
                        : Deployment.REMOTE;
                break;
            }
            default: {
                throw new IllegalArgumentException("Unrecognized partitioner: " + partitioner);
            }
        }
        this.storeFeatures = fb.build();
        this.openStores = new ConcurrentHashMap<>();
    }

    private CqlSession initialiseSession() throws PermanentBackendException {
        Configuration configuration = getStorageConfig();
        List<InetSocketAddress> contactPoints;
        //TODO the following 2 variables are duplicated in DistributedStoreManager
        String[] hostnames = configuration.get(STORAGE_HOSTS);
        int port = configuration.has(STORAGE_PORT) ? configuration.get(STORAGE_PORT) : 9042;
        try {
            contactPoints = Array.of(hostnames)
                    .map(hostName -> hostName.split(":"))
                    .map(array -> Tuple.of(array[0], array.length == 2 ? Integer.parseInt(array[1]) : port))
                    .map(tuple -> new InetSocketAddress(tuple._1, tuple._2))
                    .toJavaList();
        } catch (SecurityException | ArrayIndexOutOfBoundsException | NumberFormatException e) {
            throw new PermanentBackendException("Error initialising cluster contact points", e);
        }

        final CqlSessionBuilder builder = CqlSession.builder()
                .addContactPoints(contactPoints)
                .withLocalDatacenter(configuration.get(LOCAL_DATACENTER));

        ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder = DriverConfigLoader.programmaticBuilder();
        configLoaderBuilder.withString(DefaultDriverOption.SESSION_NAME, configuration.get(SESSION_NAME));

        if (configuration.get(PROTOCOL_VERSION) != 0) {
            configLoaderBuilder.withInt(DefaultDriverOption.PROTOCOL_VERSION, configuration.get(PROTOCOL_VERSION));
        }

        if (configuration.has(AUTH_USERNAME) && configuration.has(AUTH_PASSWORD)) {
            configLoaderBuilder
                    .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class)
                    .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, configuration.get(AUTH_USERNAME))
                    .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, configuration.get(AUTH_PASSWORD));
        }

        if (configuration.get(SSL_ENABLED)) {
            configLoaderBuilder
                    .withClass(DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS, DefaultSslEngineFactory.class)
                    .withString(DefaultDriverOption.SSL_TRUSTSTORE_PATH, configuration.get(SSL_TRUSTSTORE_LOCATION))
                    .withString(DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD, configuration.get(SSL_TRUSTSTORE_PASSWORD));
        }

        configLoaderBuilder.withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, configuration.get(LOCAL_MAX_CONNECTIONS_PER_HOST));
        configLoaderBuilder.withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, configuration.get(REMOTE_MAX_CONNECTIONS_PER_HOST));
        configLoaderBuilder.withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, configuration.get(MAX_REQUESTS_PER_CONNECTION));

        // Keep to 0 for the time being: https://groups.google.com/a/lists.datastax.com/forum/#!topic/java-driver-user/Bc0gQuOVVL0
        // Ideally we want to batch all tables initialisations to happen together when opening a new keyspace
        configLoaderBuilder.withInt(DefaultDriverOption.METADATA_SCHEMA_WINDOW, 0);
        //The following sets the size of Netty ThreadPool executor used by Cassandra driver:
        //https://docs.datastax.com/en/developer/java-driver/4.0/manual/core/async/#threading-model
        configLoaderBuilder.withInt(DefaultDriverOption.NETTY_IO_SIZE, 0); // size of threadpool scales with number of available CPUs when set to 0
        configLoaderBuilder.withInt(DefaultDriverOption.NETTY_ADMIN_SIZE, 0); // size of threadpool scales with number of available CPUs when set to 0

        configLoaderBuilder.withBoolean(DefaultDriverOption.PREPARE_ON_ALL_NODES, false);
        configLoaderBuilder.withInt(DefaultDriverOption.NETTY_TIMER_TICK_DURATION, 300);

        builder.withConfigLoader(configLoaderBuilder.build());
        return builder.build();
    }

    void initialiseKeyspace() {
        // if the keyspace already exists, just return
        if (this.session.getMetadata().getKeyspace(this.keyspace).isPresent()) {
            return;
        }

        final Configuration configuration = getStorageConfig();

        // Setting replication strategy based on value reading from the configuration: either "SimpleStrategy" or "NetworkTopologyStrategy"
        final Map<String, Object> replication = Match(configuration.get(REPLICATION_STRATEGY)).of(
                Case($("SimpleStrategy"), strategy -> HashMap.<String, Object>of("class", strategy, "replication_factor", configuration.get(REPLICATION_FACTOR))),
                Case($("NetworkTopologyStrategy"),
                        strategy -> HashMap.<String, Object>of("class", strategy)
                                .merge(Array.of(configuration.get(REPLICATION_OPTIONS))
                                        .grouped(2)
                                        .toMap(array -> Tuple.of(array.get(0), Integer.parseInt(array.get(1)))))))
                .toJavaMap();

        session.execute(createKeyspace(this.keyspace)
                .ifNotExists()
                .withReplicationOptions(replication)
                .build());
    }

    CqlSession getSession() {
        return this.session;
    }

    String getKeyspaceName() {
        return this.keyspace;
    }

    @VisibleForTesting
    Map<String, String> getCompressionOptions(final String name) throws BackendException {
        KeyspaceMetadata keyspaceMetadata1 = this.session.getMetadata().getKeyspace(this.keyspace)
                .orElseThrow(() -> new PermanentBackendException(String.format("Unknown keyspace '%s'", this.keyspace)));

        TableMetadata tableMetadata = keyspaceMetadata1.getTable(name)
                .orElseThrow(() -> new PermanentBackendException(String.format("Unknown table '%s'", name)));

        Object compressionOptions = tableMetadata.getOptions().get(CqlIdentifier.fromCql("compression"));

        return (Map<String, String>) compressionOptions;
    }

    @VisibleForTesting
    TableMetadata getTableMetadata(final String name) throws BackendException {
        final KeyspaceMetadata keyspaceMetadata = (this.session.getMetadata().getKeyspace(this.keyspace))
                .orElseThrow(() -> new PermanentBackendException(String.format("Unknown keyspace '%s'", this.keyspace)));
        return keyspaceMetadata.getTable(name)
                .orElseThrow(() -> new PermanentBackendException(String.format("Unknown table '%s'", name)));
    }

    @Override
    public void close() {
        this.session.closeAsync();
    }

    @Override
    public String getName() {
        return String.format("%s.%s", getClass().getSimpleName(), this.keyspace);
    }

    @Override
    public Deployment getDeployment() {
        return this.deployment;
    }

    @Override
    public StoreFeatures getFeatures() {
        return this.storeFeatures;
    }

    @Override
    public KeyColumnValueStore openDatabase(final String name, final Container metaData) throws BackendException {
        return this.openStores.computeIfAbsent(name, n -> new CQLKeyColumnValueStore(this, n, getStorageConfig(), () -> this.openStores.remove(n)));
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig config) throws BackendException {
        return new CQLTransaction(config);
    }

    @Override
    public void clearStorage() throws BackendException {
        if (this.storageConfig.get(DROP_ON_CLEAR)) {
            this.session.execute(dropKeyspace(this.keyspace).build());
        } else if (this.exists()) {
            final Future<Seq<AsyncResultSet>> result = Future.sequence(
                    Iterator.ofAll(this.session.getMetadata().getKeyspace(this.keyspace).get().getTables().values())
                            .map(table -> Future.fromJavaFuture(this.session.executeAsync(truncate(this.keyspace, table.getName().toString()).build())
                                    .toCompletableFuture())));
            result.await();
        } else {
            LOGGER.info("Keyspace {} does not exist in the cluster", this.keyspace);
        }
    }

    @Override
    public boolean exists() throws BackendException {
        return session.getMetadata().getKeyspace(this.keyspace).isPresent();
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void mutateMany(final Map<String, Map<StaticBuffer, KCVMutation>> mutations, final StoreTransaction txh) throws BackendException {
        if (this.atomicBatch) {
            mutateManyLogged(mutations, txh);
        } else {
            mutateManyUnlogged(mutations, txh);
        }
    }

    // Use a single logged batch
    private void mutateManyLogged(final Map<String, Map<StaticBuffer, KCVMutation>> mutations, final StoreTransaction txh) throws BackendException {
        MaskedTimestamp commitTime = new MaskedTimestamp(txh);
        long deletionTime = commitTime.getDeletionTime(this.times);
        long additionTime = commitTime.getAdditionTime(this.times);

        BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.LOGGED);
        builder.setConsistencyLevel(getTransaction(txh).getWriteConsistencyLevel());
        builder.addStatements(Iterator.ofAll(mutations.entrySet()).flatMap(tableNameAndMutations -> {
            String tableName = tableNameAndMutations.getKey();
            Map<StaticBuffer, KCVMutation> tableMutations = tableNameAndMutations.getValue();

            CQLKeyColumnValueStore columnValueStore = this.openStores.get(tableName);
            return Iterator.ofAll(tableMutations.entrySet()).flatMap(keyAndMutations -> {
                StaticBuffer key = keyAndMutations.getKey();
                KCVMutation keyMutations = keyAndMutations.getValue();

                Iterator<BatchableStatement<BoundStatement>> deletions = Iterator.ofAll(keyMutations.getDeletions()).map(deletion -> columnValueStore.deleteColumn(key, deletion, deletionTime));
                Iterator<BatchableStatement<BoundStatement>> additions = Iterator.ofAll(keyMutations.getAdditions()).map(addition -> columnValueStore.insertColumn(key, addition, additionTime));

                return Iterator.concat(deletions, additions);
            });
        }));
        CompletableFuture<AsyncResultSet> result = this.session.executeAsync(builder.build()).toCompletableFuture();

        try {
            result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw EXCEPTION_MAPPER.apply(e);
        } catch (ExecutionException e) {
            throw EXCEPTION_MAPPER.apply(e);
        }
        sleepAfterWrite(txh, commitTime);
    }

    // Create an async un-logged batch per partition key
    private void mutateManyUnlogged(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        MaskedTimestamp commitTime = new MaskedTimestamp(txh);
        long deletionTime = commitTime.getDeletionTime(this.times);
        long additionTime = commitTime.getAdditionTime(this.times);
        List<CompletableFuture<AsyncResultSet>> executionFutures = new ArrayList<>();
        ConsistencyLevel consistencyLevel = getTransaction(txh).getWriteConsistencyLevel();

        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> tableNameAndMutations : mutations.entrySet()) {
            String tableName = tableNameAndMutations.getKey();
            Map<StaticBuffer, KCVMutation> tableMutations = tableNameAndMutations.getValue();
            CQLKeyColumnValueStore columnValueStore = this.openStores.get(tableName);

            // Prepare one BatchStatement containing all the statements concerning the same partitioning key.
            // For correctness we must batch statements based on partition key otherwise we might end-up having batch of statements
            // containing queries that have to be executed on different cluster nodes.
            for (Map.Entry<StaticBuffer, KCVMutation> keyAndMutations : tableMutations.entrySet()) {
                StaticBuffer key = keyAndMutations.getKey();
                KCVMutation keyMutations = keyAndMutations.getValue();

                //Concatenate additions and deletions and map them to async session executions (completable futures) of resulting batch statement
                executionFutures.addAll(
                        Stream.concat(
                                keyMutations.getDeletions().stream().map(deletion -> columnValueStore.deleteColumn(key, deletion, deletionTime)),
                                keyMutations.getAdditions().stream().map(addition -> columnValueStore.insertColumn(key, addition, additionTime))
                        ).map(st -> this.session.executeAsync(
                                BatchStatement.newInstance(DefaultBatchType.UNLOGGED)
                                        .add(st)
                                        .setConsistencyLevel(consistencyLevel)
                                ).toCompletableFuture()
                        ).collect(Collectors.toList())
                );
            }
        }

        try {
            CompletableFuture.allOf(executionFutures.toArray(new CompletableFuture[]{})).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw EXCEPTION_MAPPER.apply(e);
        } catch (ExecutionException e) {
            throw EXCEPTION_MAPPER.apply(e);
        }
        sleepAfterWrite(txh, commitTime);
    }

    private String determineKeyspaceName(Configuration config) {
        if ((!config.has(KEYSPACE) && (config.has(GRAPH_NAME)))) return config.get(GRAPH_NAME);
        return config.get(KEYSPACE);
    }
}
