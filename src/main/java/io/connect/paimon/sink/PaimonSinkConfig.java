/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.connect.paimon.sink;

import io.connect.paimon.common.PropertyUtil;
import io.connect.paimon.sink.naming.DebeziumTableNamingStrategy;
import io.connect.paimon.sink.naming.TableNamingStrategy;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.paimon.shade.guava30.com.google.common.annotations.VisibleForTesting;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;


/**
 * Paimon sink config
 */
public class PaimonSinkConfig extends AbstractConfig {

    // config options
    private static final String TABLE_PROP_PREFIX = "table.config.";
    private static final String CATALOG_PROP_PREFIX = "catalog.config.";
    private static final String HADOOP_PROP_PREFIX = "hadoop.config.";

    // Auto  create table
    public static final String AUTO_CREATE = "auto.create";
    private static final String AUTO_CREATE_DEFAULT = "false";
    private static final String AUTO_CREATE_DOC =
            "Whether to automatically create the destination table based on record schema if it is "
                    + "found to be missing by issuing ``CREATE``.";

    // schema evolution
    public static final String AUTO_EVOLVE = "auto.evolve";
    private static final String AUTO_EVOLVE_DEFAULT = "false";
    private static final String AUTO_EVOLVE_DOC =
            "Whether to automatically add columns in the table schema when found to be missing relative "
                    + "to the record schema by issuing ``ALTER``.";

    // Table name format
    public static final String TABLE_NAME_FORMAT_FIELD = "table.name.format";
    private static final String TABLE_NAME_FORMAT_FIELD_DEFAULT = "${topic}";
    private static final String TABLE_NAME_FORMAT_FIELD_DOC = "A format string for the table, which may contain '${topic}' as a placeholder for the original topic name.";

    // Table naming strategy
    public static final String TABLE_NAMING_STRATEGY_FIELD = "table.naming.strategy";
    public static final String TABLE_NAMING_STRATEGY_FIELD_DEFAULT = DebeziumTableNamingStrategy.class.getName();
    public static final String TABLE_NAMING_STRATEGY_FIELD_DOC = "Name of the strategy class that implements the TablingNamingStrategy interface";


    // table case sensitive
    public static final String TABLE_CASE_SENSITIVE = "case.sensitive";
    private static final boolean TABLE_CASE_SENSITIVE_DEFAULT = true;
    private static final String TABLE_CASE_SENSITIVE_DOC = "Table case sensitive config.";

    // commit interval ms
    private static final String COMMIT_INTERVAL_MS_PROP = "control.commit.interval-ms";
    private static final int COMMIT_INTERVAL_MS_DEFAULT = 300_000;


    // primary keys
    private static final String PRIMARY_KEYS = "table.default-primary-keys";
    // partition keys
    private static final String PARTITION_KEYS = "table.default-partition-keys";


    //parameter
    public static final ConfigDef CONFIG_DEF = newConfigDef();
    private final Map<String, String> tableProps;
    private final Map<String, String> catalogProps;
    private final Map<String, String> hadoopProps;

    // table naming strategy
    private final TableNamingStrategy tableNamingStrategy;
    // auto create table
    private final boolean autoCreate;
    // schema evolution
    private final boolean autoEvolve;
    // table name format
    private final String tableNameFormat;
    // case sensitive
    private final boolean caseSensitive;

    //  commit interval ms
    private final int commitIntervalMs;

    public static String version() {
        String version = PaimonSinkConfig.class.getPackage().getImplementationVersion();
        if (version == null) {
            version = "unknown";
        }
        return version;
    }

    public PaimonSinkConfig(Map<String, String> originalProps) {
        super(CONFIG_DEF, originalProps);
        // Table naming strategy
        this.tableNamingStrategy = getConfiguredInstance(TABLE_NAMING_STRATEGY_FIELD, TableNamingStrategy.class);
        // table props
        this.tableProps = PropertyUtil.propertiesWithPrefix(originalProps, TABLE_PROP_PREFIX);
        // Catalog props
        this.catalogProps = PropertyUtil.propertiesWithPrefix(originalProps, CATALOG_PROP_PREFIX);
        this.hadoopProps = PropertyUtil.propertiesWithPrefix(originalProps, HADOOP_PROP_PREFIX);
        // auto create table
        this.autoCreate = getBoolean(AUTO_CREATE);
        // schema evolution
        this.autoEvolve = getBoolean(AUTO_EVOLVE);
        this.tableNameFormat = this.getString(TABLE_NAME_FORMAT_FIELD);
        this.caseSensitive = this.getBoolean(TABLE_CASE_SENSITIVE);
        this.commitIntervalMs = this.getInt(COMMIT_INTERVAL_MS_PROP);
    }

    private static ConfigDef newConfigDef() {
        ConfigDef configDef = new ConfigDef();

        // table name strategy
        configDef.define(
                TABLE_NAMING_STRATEGY_FIELD,
                ConfigDef.Type.CLASS,
                TABLE_NAMING_STRATEGY_FIELD_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                TABLE_NAMING_STRATEGY_FIELD_DOC
        ).define(
                AUTO_EVOLVE,
                ConfigDef.Type.BOOLEAN,
                AUTO_EVOLVE_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                AUTO_EVOLVE_DOC
        ).define(
                AUTO_CREATE,
                ConfigDef.Type.BOOLEAN,
                AUTO_CREATE_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                AUTO_CREATE_DOC
        ).define(
                TABLE_NAME_FORMAT_FIELD,
                ConfigDef.Type.STRING,
                TABLE_NAME_FORMAT_FIELD_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                TABLE_NAME_FORMAT_FIELD_DOC
        ).define(
                PRIMARY_KEYS,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.MEDIUM,
                "Default ID columns for tables, comma-separated"
        ).define(
                PARTITION_KEYS,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.MEDIUM,
                "Default partition spec to use when creating table, comma-separated"
        ).define(
                TABLE_CASE_SENSITIVE,
                ConfigDef.Type.STRING,
                TABLE_CASE_SENSITIVE_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                TABLE_CASE_SENSITIVE_DOC)
        .define(
                COMMIT_INTERVAL_MS_PROP,
                ConfigDef.Type.INT,
                COMMIT_INTERVAL_MS_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                "Coordinator interval for performing Iceberg table commits, in millis");;
        // Config options
        return configDef;
    }

    public List<String> partitionKeys() {
        return stringToList(getString(PARTITION_KEYS), ",");
    }

    public List<String> primaryKeys() {
        return stringToList(getString(PRIMARY_KEYS), ",");
    }

    public Map<String, String> getTableProps() {
        return tableProps;
    }

    public Map<String, String> catalogProps() {
        return catalogProps;
    }

    public Map<String, String> hadoopProps() {
        return hadoopProps;
    }

    public TableNamingStrategy getTableNamingStrategy() {
        return tableNamingStrategy;
    }

    public String getTableNameFormat() {
        return tableNameFormat;
    }

    public boolean isAutoCreate() {
        return autoCreate;
    }

    public boolean isAutoEvolve() {
        return autoEvolve;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public int getCommitIntervalMs() {
        return commitIntervalMs;
    }

    @VisibleForTesting
    static List<String> stringToList(String value, String regex) {
        if (value == null || value.isEmpty()) {
            return ImmutableList.of();
        }

        return Arrays.stream(value.split(regex)).map(String::trim).collect(toList());
    }
}
