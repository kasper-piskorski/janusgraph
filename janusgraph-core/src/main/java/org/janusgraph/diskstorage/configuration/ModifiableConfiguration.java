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

package org.janusgraph.diskstorage.configuration;

import static org.janusgraph.graphdb.configuration.JanusGraphConstants.UPGRADEABLE_FIXED;

import com.google.common.base.Preconditions;

import java.util.Map;

/**
 * This configuration extends BasicConfiguration, adding 'set' and 'remove' capabilities to it.
 * It is also possible to Freeze this Configuration, in order to make it read-only again.
 *
 */
public class ModifiableConfiguration extends BasicConfiguration {

    private static final String FROZEN_KEY = "hidden.frozen";
    private final WriteConfiguration config;
    private Boolean isFrozen;


    public ModifiableConfiguration(ConfigNamespace root, WriteConfiguration config, Restriction restriction) {
        super(root, config, restriction);
        Preconditions.checkNotNull(config);
        this.config = config;
    }

    public <O> ModifiableConfiguration set(ConfigOption<O> option, O value, String... umbrellaElements) {
        verifyOption(option);
        Preconditions.checkArgument(hasUpgradeableFixed(option.getName()) ||
                !option.isFixed() || !isFrozen(), "Cannot change configuration option: %s", option);
        String key = super.getPath(option, umbrellaElements);
        value = option.verify(value);
        config.set(key, value);
        return this;
    }

    public void setAll(Map<ConfigElement.PathIdentifier, Object> options) {
        for (Map.Entry<ConfigElement.PathIdentifier, Object> entry : options.entrySet()) {
            Preconditions.checkArgument(entry.getKey().element.isOption());
            set((ConfigOption) entry.getKey().element, entry.getValue(), entry.getKey().umbrellaElements);
        }
    }

    public <O> void remove(ConfigOption<O> option, String... umbrellaElements) {
        verifyOption(option);
        Preconditions.checkArgument(!option.isFixed() || !isFrozen(), "Cannot change configuration option: %s", option);
        String key = super.getPath(option, umbrellaElements);
        config.remove(key);
    }

    public void freezeConfiguration() {
        config.set(FROZEN_KEY, Boolean.TRUE);
        if (!isFrozen()) setFrozen();
    }

    private boolean hasUpgradeableFixed(String name) {
        return UPGRADEABLE_FIXED.contains(name);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return config;
    }

    public boolean isFrozen() {
        if (null == isFrozen) {
            Boolean frozen = config.get(FROZEN_KEY, Boolean.class);
            isFrozen = (null == frozen) ? false : frozen;
        }
        return isFrozen;
    }

    private void setFrozen() {
        isFrozen = true;
    }
}
