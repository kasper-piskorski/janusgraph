// Copyright 2018 JanusGraph Authors
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

package org.janusgraph.diskstorage.configuration.builder;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.*;
import org.janusgraph.diskstorage.configuration.backend.KCVSConfiguration;
import org.janusgraph.diskstorage.configuration.backend.builder.KCVSConfigurationBuilder;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.BackendOperation;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.JanusGraphConstants;
import org.janusgraph.util.system.LoggerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * Builder to build {@link ReadConfiguration} instance of global configuration
 */
public class ReadConfigurationBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(ReadConfigurationBuilder.class);
    private static final String BACKLEVEL_STORAGE_VERSION_EXCEPTION = "The storage version on the client or server is lower than the storage version of the graph: graph storage version %s vs. client storage version %s when opening graph %s.";
    private static final String INCOMPATIBLE_STORAGE_VERSION_EXCEPTION = "Storage version is incompatible with current client: graph storage version %s vs. client storage version %s when opening graph %s.";

    public static ReadConfiguration buildGlobalConfiguration(BasicConfiguration localBasicConfiguration,
                                                             KeyColumnValueStoreManager storeManager,
                                                             KCVSConfigurationBuilder kcvsConfigurationBuilder) {


        BackendOperation.TransactionalProvider transactionalProvider = new BackendOperation.TransactionalProvider() {
            @Override
            public StoreTransaction openTx() throws BackendException {
                return storeManager.beginTransaction(StandardBaseTransactionConfig.of(localBasicConfiguration.get(TIMESTAMP_PROVIDER), storeManager.getFeatures().getKeyConsistentTxConfig()));
            }

            @Override
            public void close() {
                // do nothing
            }
        };
        KeyColumnValueStore systemPropertiesStore;
        try {
            systemPropertiesStore = storeManager.openDatabase(SYSTEM_PROPERTIES_STORE_NAME);
        } catch (BackendException e) {
            throw new JanusGraphException("Could not open 'system_properties' store: ", e);
        }

        //Read  Global Configuration (from 'system_properties' store, everything associated to 'configuration' key)
        try (KCVSConfiguration keyColumnValueStoreConfiguration = kcvsConfigurationBuilder.buildGlobalConfiguration(transactionalProvider, systemPropertiesStore, localBasicConfiguration)) {

            //Freeze global configuration if not already frozen!
            ModifiableConfiguration globalWrite = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS, keyColumnValueStoreConfiguration, BasicConfiguration.Restriction.GLOBAL);

            if (!globalWrite.isFrozen()) {
                //Copy over global configurations
                globalWrite.setAll(getGlobalSubset(localBasicConfiguration.getAll()));

                setupJanusGraphVersion(globalWrite);
                setupStorageVersion(globalWrite);
                setupTimestampProvider(globalWrite, localBasicConfiguration, storeManager);

                globalWrite.freezeConfiguration();
            } else {
                String graphName = localBasicConfiguration.getConfiguration().get(GRAPH_NAME.toStringWithoutRoot(), String.class);
                final boolean upgradeAllowed = isUpgradeAllowed(globalWrite, localBasicConfiguration);

                if (upgradeAllowed) {
                    setupUpgradeConfiguration(graphName, globalWrite);
                } else {
                    checkJanusGraphStorageVersionEquality(globalWrite, graphName);
                }

//                checkOptionsWithDiscrepancies(globalWrite, localBasicConfiguration, overwrite);
            }
            return keyColumnValueStoreConfiguration.asReadConfiguration();
        }
    }

    private static void setupUpgradeConfiguration(String graphName, ModifiableConfiguration globalWrite) {
        // If the graph doesn't have a storage version set it and update version
        if (!globalWrite.has(INITIAL_STORAGE_VERSION)) {
            janusGraphVersionsWithDisallowedUpgrade(globalWrite);
            LOG.info("graph.storage-version has been upgraded from 1 to {} and graph.janusgraph-version has been upgraded from {} to {} on graph {}",
                    JanusGraphConstants.STORAGE_VERSION, globalWrite.get(INITIAL_JANUSGRAPH_VERSION), JanusGraphConstants.VERSION, graphName);
            return;
        }
        int storageVersion = Integer.parseInt(JanusGraphConstants.STORAGE_VERSION);
        int initialStorageVersion = Integer.parseInt(globalWrite.get(INITIAL_STORAGE_VERSION));
        // If the storage version of the client or server opening the graph is lower than the graph's storage version throw an exception
        if (initialStorageVersion > storageVersion) {
            throw new JanusGraphException(String.format(BACKLEVEL_STORAGE_VERSION_EXCEPTION, globalWrite.get(INITIAL_STORAGE_VERSION), JanusGraphConstants.STORAGE_VERSION, graphName));
        }
        // If the graph has a storage version, but it's lower than the client or server opening the graph upgrade the version and storage version
        if (initialStorageVersion < storageVersion) {
            janusGraphVersionsWithDisallowedUpgrade(globalWrite);
            LOG.info("graph.storage-version has been upgraded from {} to {} and graph.janusgraph-version has been upgraded from {} to {} on graph {}",
                    globalWrite.get(INITIAL_STORAGE_VERSION), JanusGraphConstants.STORAGE_VERSION, globalWrite.get(INITIAL_JANUSGRAPH_VERSION), JanusGraphConstants.VERSION, graphName);
        } else {
            LOG.warn("Warning graph.allow-upgrade is currently set to true on graph {}. Please set graph.allow-upgrade to false in your properties file.", graphName);
        }
    }

    private static void janusGraphVersionsWithDisallowedUpgrade(ModifiableConfiguration globalWrite) {
        globalWrite.set(INITIAL_JANUSGRAPH_VERSION, JanusGraphConstants.VERSION);
        globalWrite.set(INITIAL_STORAGE_VERSION, JanusGraphConstants.STORAGE_VERSION);
        globalWrite.set(ALLOW_UPGRADE, false);
    }

    private static void setupJanusGraphVersion(ModifiableConfiguration globalWrite) {
        Preconditions.checkArgument(!globalWrite.has(INITIAL_JANUSGRAPH_VERSION), "Database has already been initialized but not frozen");
        globalWrite.set(INITIAL_JANUSGRAPH_VERSION, JanusGraphConstants.VERSION);
    }

    private static void setupStorageVersion(ModifiableConfiguration globalWrite) {
        Preconditions.checkArgument(!globalWrite.has(INITIAL_STORAGE_VERSION), "Database has already been initialized but not frozen");
        globalWrite.set(INITIAL_STORAGE_VERSION, JanusGraphConstants.STORAGE_VERSION);
    }

    private static void setupTimestampProvider(ModifiableConfiguration globalWrite, BasicConfiguration localBasicConfiguration, KeyColumnValueStoreManager storeManager) {
        /* If the configuration does not explicitly set a timestamp provider and
         * the storage backend both supports timestamps and has a preference for
         * a specific timestamp provider, then apply the backend's preference.
         */
        if (!localBasicConfiguration.has(TIMESTAMP_PROVIDER)) {
            StoreFeatures f = storeManager.getFeatures();
            final TimestampProviders backendPreference;
            if (f.hasTimestamps() && null != (backendPreference = f.getPreferredTimestamps())) {
                globalWrite.set(TIMESTAMP_PROVIDER, backendPreference);
                LOG.debug("Set timestamps to {} according to storage backend preference",
                        LoggerUtil.sanitizeAndLaunder(globalWrite.get(TIMESTAMP_PROVIDER)));
            } else {
                globalWrite.set(TIMESTAMP_PROVIDER, TIMESTAMP_PROVIDER.getDefaultValue());
                LOG.debug("Set default timestamp provider {}", LoggerUtil.sanitizeAndLaunder(globalWrite.get(TIMESTAMP_PROVIDER)));
            }
        } else {
            LOG.debug("Using configured timestamp provider {}", localBasicConfiguration.get(TIMESTAMP_PROVIDER));
        }
    }

    private static Map<ConfigElement.PathIdentifier, Object> getGlobalSubset(Map<ConfigElement.PathIdentifier, Object> m) {
        return Maps.filterEntries(m, entry -> ((ConfigOption) entry.getKey().element).isGlobal());
    }

    private static Map<ConfigElement.PathIdentifier, Object> getManagedSubset(Map<ConfigElement.PathIdentifier, Object> m) {
        return Maps.filterEntries(m, entry -> ((ConfigOption) entry.getKey().element).isManaged());
    }

    private static void checkJanusGraphStorageVersionEquality(ModifiableConfiguration globalWrite, String graphName) {
        if (!Objects.equals(globalWrite.get(INITIAL_STORAGE_VERSION), JanusGraphConstants.STORAGE_VERSION)) {
            String storageVersion = (globalWrite.has(INITIAL_STORAGE_VERSION)) ? globalWrite.get(INITIAL_STORAGE_VERSION) : "1";
            throw new JanusGraphException(String.format(INCOMPATIBLE_STORAGE_VERSION_EXCEPTION, storageVersion, JanusGraphConstants.STORAGE_VERSION, graphName));
        }
    }

    private static boolean isUpgradeAllowed(ModifiableConfiguration globalWrite, BasicConfiguration localBasicConfiguration) {
        if (localBasicConfiguration.has(ALLOW_UPGRADE)) {
            return localBasicConfiguration.get(ALLOW_UPGRADE);
        } else if (globalWrite.has(ALLOW_UPGRADE)) {
            return globalWrite.get(ALLOW_UPGRADE);
        }
        return ALLOW_UPGRADE.getDefaultValue();
    }

//    private static void checkOptionsWithDiscrepancies(ModifiableConfiguration globalWrite, BasicConfiguration localBasicConfiguration,
//                                               ModifiableConfiguration overwrite) {
//        final boolean managedOverridesAllowed = isManagedOverwritesAllowed(globalWrite, localBasicConfiguration);
//        Set<String> optionsWithDiscrepancies = getOptionsWithDiscrepancies(globalWrite, localBasicConfiguration, overwrite, managedOverridesAllowed);
//
//        if (optionsWithDiscrepancies.size() > 0 && !managedOverridesAllowed) {
//            final String template = "Local settings present for one or more globally managed options: [%s].  These options are controlled through the %s interface; local settings have no effect.";
//            throw new JanusGraphConfigurationException(String.format(template, Joiner.on(", ").join(optionsWithDiscrepancies), ManagementSystem.class.getSimpleName()));
//        }
//    }
//
//    private static boolean isManagedOverwritesAllowed(ModifiableConfiguration globalWrite, BasicConfiguration localBasicConfiguration) {
//        if (localBasicConfiguration.has(ALLOW_STALE_CONFIG)) {
//            return localBasicConfiguration.get(ALLOW_STALE_CONFIG);
//        } else if (globalWrite.has(ALLOW_STALE_CONFIG)) {
//            return globalWrite.get(ALLOW_STALE_CONFIG);
//        }
//        return ALLOW_STALE_CONFIG.getDefaultValue();
//    }

//    /**
//     * Check for disagreement between local and backend values for GLOBAL(_OFFLINE) and FIXED options
//     * The point of this check is to find edits to the local config which have no effect (and therefore likely indicate misconfiguration)
//     *
//     * @return Options with discrepancies
//     */
//    private static Set<String> getOptionsWithDiscrepancies(ModifiableConfiguration globalWrite, BasicConfiguration localBasicConfiguration,
//                                                    ModifiableConfiguration overwrite, boolean managedOverridesAllowed) {
//        Set<String> optionsWithDiscrepancies = Sets.newHashSet();
//
//        for (Map.Entry<ConfigElement.PathIdentifier, Object> entry : getManagedSubset(localBasicConfiguration.getAll()).entrySet()) {
//            ConfigElement.PathIdentifier pathId = entry.getKey();
//            ConfigOption<?> configOption = (ConfigOption<?>) pathId.element;
//            Object localValue = entry.getValue();
//
//            // Get the storage backend's setting and compare with localValue
//            Object storeValue = globalWrite.get(configOption, pathId.umbrellaElements);
//
//            // Check if the value is to be overwritten
//            if (overwrite.has(configOption, pathId.umbrellaElements)) {
//                storeValue = overwrite.get(configOption, pathId.umbrellaElements);
//            }
//
//            // Most validation predicate implementations disallow null, but we can't assume that here
//            final boolean match = Objects.equals(localValue, storeValue);
//
//            // Log each option with value disagreement between local and backend configs
//            if (!match) {
//                final String fullOptionName = ConfigElement.getPath(pathId.element, pathId.umbrellaElements);
//                final String template = "Local setting {}={} (Type: {}) is overridden by globally managed value ({}).  Use the {} interface instead of the local configuration to control this setting.";
//                Object[] replacements = new Object[]{fullOptionName, localValue, configOption.getType(), storeValue, ManagementSystem.class.getSimpleName()};
//                if (managedOverridesAllowed) { // Lower LOG severity when this is enabled
//                    LOG.warn(template, replacements);
//                } else {
//                    LOG.error(template, replacements);
//                }
//                optionsWithDiscrepancies.add(fullOptionName);
//            }
//        }
//        return optionsWithDiscrepancies;
//    }

}
