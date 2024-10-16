/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.connect.paimon.sink;

import io.connect.paimon.coordinator.Coordinator;
import io.connect.paimon.coordinator.CoordinatorTask;
import io.connect.paimon.coordinator.OrphanFilesCleanHandler;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Paimon sink connector. */
public class PaimonSinkConnector extends SinkConnector {

    private Map<String, String> config;

    @Override
    public void start(Map<String, String> config) {
        this.config = config;
        PaimonSinkConfig sinkConfig = new PaimonSinkConfig(config);
        if (sinkConfig.isEnabledOrphanFilesClean()) {
            CoordinatorTask task =
                    new CoordinatorTask(
                            new OrphanFilesCleanHandler(sinkConfig),
                            sinkConfig.getOrphanFilesCleanIntervalMs());
            Coordinator.registry(task);
        }
        Coordinator.start();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PaimonSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int max) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < max; i++) {
            configs.add(new HashMap<>(config));
        }
        return configs;
    }

    @Override
    public void stop() {
        // No-op
    }

    @Override
    public ConfigDef config() {
        return PaimonSinkConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return PaimonSinkConfig.version();
    }
}
