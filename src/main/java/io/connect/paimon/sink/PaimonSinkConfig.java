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

import org.apache.paimon.shade.guava30.com.google.common.annotations.VisibleForTesting;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import io.connect.paimon.sink.naming.DebeziumTableNamingStrategy;
import io.connect.paimon.sink.naming.TableNamingStrategy;
import io.connect.paimon.utils.PropertyUtil;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static java.util.stream.Collectors.toList;

/** Paimon sink config. */
public class PaimonSinkConfig extends AbstractConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(PaimonSinkConfig.class);

    // Refrence org.apache.paimon.CoreOptions
    private static final String TABLE_PROP_PREFIX = "table.config.";
    private static final String CATALOG_PROP_PREFIX = "catalog.config.";
    private static final String HADOOP_PROP_PREFIX = "hadoop.config.";

    // Auto  create table
    public static final String AUTO_CREATE = "auto.create";
    private static final boolean AUTO_CREATE_DEFAULT = false;
    private static final String AUTO_CREATE_DOC =
            "Whether to automatically create the destination table based on record schema if it is "
                    + "found to be missing by issuing ``CREATE``.";

    // schema evolution
    public static final String AUTO_EVOLVE = "auto.evolve";
    private static final boolean AUTO_EVOLVE_DEFAULT = false;
    private static final String AUTO_EVOLVE_DOC =
            "Whether to automatically add columns in the table schema when found to be missing relative "
                    + "to the record schema by issuing ``ALTER``.";

    // Write database name
    public static final String WRITE_DATABASE_NAME = "database.name.format";
    private static final String WRITE_DATABASE_NAME_DEFAULT = "default";
    private static final String WRITE_DATABASE_NAME_DOC =
            "A format string for the database, which may contain '${topic}' as a placeholder for the original topic name.";

    // Table name format
    public static final String TABLE_NAME_FORMAT_FIELD = "table.name.format";
    private static final String TABLE_NAME_FORMAT_FIELD_DEFAULT = "${topic}";
    private static final String TABLE_NAME_FORMAT_FIELD_DOC =
            "A format string for the table, which may contain '${topic}' as a placeholder for the original topic name.";

    // Table naming strategy
    public static final String TABLE_NAMING_STRATEGY_FIELD = "table.naming.strategy";
    public static final String TABLE_NAMING_STRATEGY_FIELD_DEFAULT =
            DebeziumTableNamingStrategy.class.getName();
    public static final String TABLE_NAMING_STRATEGY_FIELD_DOC =
            "Name of the strategy class that implements the TablingNamingStrategy interface";

    // table case sensitive
    public static final String TABLE_CASE_SENSITIVE = "case.sensitive";
    private static final boolean TABLE_CASE_SENSITIVE_DEFAULT = true;
    private static final String TABLE_CASE_SENSITIVE_DOC = "Table case sensitive config.";

    // primary keys
    private static final String PRIMARY_KEYS = "table.default-primary-keys";
    // partition keys
    private static final String PARTITION_KEYS = "table.default-partition-keys";

    private static final String ENABLED_ORPHAN_FILES_CLEAN = "enabled.orphan.files.clean";
    // commit interval ms
    private static final String ORPHAN_FILES_CLEAN_INTERVAL_MS = "orphan.files.clean.interval-ms";
    private static final int ORPHAN_FILES_CLEAN_INTERVAL_MS_DEFAULT = 1000 * 60 * 60;
    // older than
    private static final String ORPHAN_FILES_CLEAN_OLDER_THAN = "orphan.files.clean.older-than";
    // clean orphan parallelism
    private static final String ORPHAN_FILES_CLEAN_PARALLELISM = "orphan.files.clean.parallelism";
    private static final int ORPHAN_FILES_CLEAN_PARALLELISM_DEFAULT = 1;
    // clean orphan parallelism
    private static final String ORPHAN_FILES_CLEAN_DATABASE = "orphan.files.clean.database";

    // parameter
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
    private final String writeDatabaseName;
    // table name format
    private final String tableNameFormat;
    // case sensitive
    private final boolean caseSensitive;
    // enabled orphan files clean
    private final boolean enabledOrphanFilesClean;
    //  commit interval ms
    private final int orphanFilesCleanIntervalMs;
    private final String orphanFilesCleanOlderThan;
    private final int orphanFilesCleanParallelism;
    private final String orphanFilesCleanDatabase;

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
        this.tableNamingStrategy = loadTableNamingStrategy();
        // table props
        this.tableProps = PropertyUtil.propertiesWithPrefix(originalProps, TABLE_PROP_PREFIX);
        // Catalog props
        this.catalogProps = PropertyUtil.propertiesWithPrefix(originalProps, CATALOG_PROP_PREFIX);
        this.hadoopProps = PropertyUtil.propertiesWithPrefix(originalProps, HADOOP_PROP_PREFIX);
        // auto create table
        this.autoCreate = getBoolean(AUTO_CREATE);
        // schema evolution
        this.autoEvolve = getBoolean(AUTO_EVOLVE);
        this.writeDatabaseName = getString(WRITE_DATABASE_NAME);
        this.tableNameFormat = getString(TABLE_NAME_FORMAT_FIELD);
        this.caseSensitive = getBoolean(TABLE_CASE_SENSITIVE);

        // orphan files clean
        this.enabledOrphanFilesClean = getBoolean(ENABLED_ORPHAN_FILES_CLEAN);
        this.orphanFilesCleanIntervalMs = getInt(ORPHAN_FILES_CLEAN_INTERVAL_MS);
        this.orphanFilesCleanOlderThan = getString(ORPHAN_FILES_CLEAN_OLDER_THAN);
        this.orphanFilesCleanParallelism = getInt(ORPHAN_FILES_CLEAN_PARALLELISM);
        this.orphanFilesCleanDatabase = getString(ORPHAN_FILES_CLEAN_DATABASE);
    }

    private TableNamingStrategy loadTableNamingStrategy() {
        ServiceLoader<TableNamingStrategy> loadedDialects =
                ServiceLoader.load(TableNamingStrategy.class);
        Iterator<TableNamingStrategy> dialectIterator = loadedDialects.iterator();
        try {
            while (dialectIterator.hasNext()) {
                TableNamingStrategy strategy = dialectIterator.next();
                if (strategy.getClass().getName().equals(getString(TABLE_NAMING_STRATEGY_FIELD))) {
                    return strategy;
                }
            }
        } catch (Throwable t) {
            LOGGER.debug("Error loading table naming strategy.", t);
        }
        throw new RuntimeException("Please specify table naming strategy");
    }

    private static ConfigDef newConfigDef() {
        ConfigDef configDef = new ConfigDef();

        // table name strategy
        configDef
                .define(
                        WRITE_DATABASE_NAME,
                        ConfigDef.Type.STRING,
                        WRITE_DATABASE_NAME_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        WRITE_DATABASE_NAME_DOC)
                .define(
                        TABLE_NAMING_STRATEGY_FIELD,
                        ConfigDef.Type.STRING,
                        TABLE_NAMING_STRATEGY_FIELD_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        TABLE_NAMING_STRATEGY_FIELD_DOC)
                .define(
                        AUTO_EVOLVE,
                        ConfigDef.Type.BOOLEAN,
                        AUTO_EVOLVE_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        AUTO_EVOLVE_DOC)
                .define(
                        AUTO_CREATE,
                        ConfigDef.Type.BOOLEAN,
                        AUTO_CREATE_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        AUTO_CREATE_DOC)
                .define(
                        TABLE_NAME_FORMAT_FIELD,
                        ConfigDef.Type.STRING,
                        TABLE_NAME_FORMAT_FIELD_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        TABLE_NAME_FORMAT_FIELD_DOC)
                .define(
                        PRIMARY_KEYS,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        "Default ID columns for tables, comma-separated")
                .define(
                        PARTITION_KEYS,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        "Default partition spec to use when creating table, comma-separated")
                .define(
                        TABLE_CASE_SENSITIVE,
                        ConfigDef.Type.BOOLEAN,
                        TABLE_CASE_SENSITIVE_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        TABLE_CASE_SENSITIVE_DOC)
                .define(
                        ENABLED_ORPHAN_FILES_CLEAN,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.MEDIUM,
                        "Enabled clean orphan files.")
                .define(
                        ORPHAN_FILES_CLEAN_INTERVAL_MS,
                        ConfigDef.Type.INT,
                        ORPHAN_FILES_CLEAN_INTERVAL_MS_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        "Task for clean orphan files interval ms.")
                .define(
                        ORPHAN_FILES_CLEAN_OLDER_THAN,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        "Orphan files clean older-than.")
                .define(
                        ORPHAN_FILES_CLEAN_PARALLELISM,
                        ConfigDef.Type.INT,
                        ORPHAN_FILES_CLEAN_PARALLELISM_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        "Orphan files clean parallelism.")
                .define(
                        ORPHAN_FILES_CLEAN_DATABASE,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        "Orphan files clean database.");
        // Config options
        return configDef;
    }

    public List<String> partitionKeys() {
        return stringToList(getString(PARTITION_KEYS), ",");
    }

    public List<String> primaryKeys() {
        return stringToList(getString(PRIMARY_KEYS), ",");
    }

    public Map<String, String> tableProps() {
        return tableProps;
    }

    public Map<String, String> catalogProps() {
        return catalogProps;
    }

    public Map<String, String> hadoopProps() {
        return hadoopProps;
    }

    public TableNamingStrategy tableNamingStrategy() {
        return tableNamingStrategy;
    }

    public String getTableNameFormat() {
        return tableNameFormat;
    }

    public String getWriteDatabaseName() {
        return writeDatabaseName;
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

    public boolean isEnabledOrphanFilesClean() {
        return enabledOrphanFilesClean;
    }

    public int getOrphanFilesCleanIntervalMs() {
        return orphanFilesCleanIntervalMs;
    }

    public String getOrphanFilesCleanOlderThan() {
        return orphanFilesCleanOlderThan;
    }

    public int getOrphanFilesCleanParallelism() {
        return orphanFilesCleanParallelism;
    }

    public String getOrphanFilesCleanDatabase() {
        return orphanFilesCleanDatabase;
    }

    @VisibleForTesting
    static List<String> stringToList(String value, String regex) {
        if (value == null || value.isEmpty()) {
            return ImmutableList.of();
        }
        return Arrays.stream(value.split(regex)).map(String::trim).collect(toList());
    }
}
