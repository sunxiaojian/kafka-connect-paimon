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

package io.connect.paimon.coordinator;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.OrphanFilesClean;
import org.apache.paimon.utils.Preconditions;

import io.connect.paimon.operation.LocalOrphanFilesClean;
import io.connect.paimon.sink.PaimonSinkConfig;
import io.connect.paimon.utils.CatalogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

/** A handler for orphan files clean. */
public class OrphanFilesCleanHandler implements Handler {

    protected static final Logger LOG = LoggerFactory.getLogger(OrphanFilesCleanHandler.class);

    private final Catalog catalog;
    private final String database;
    private final long olderThanMillis;
    private final Consumer<Path> fileCleaner;
    private final Integer parallelism;

    public OrphanFilesCleanHandler(PaimonSinkConfig config) {
        this.catalog = CatalogUtils.createCataLog(config.catalogProps());
        this.database =
                Preconditions.checkNotNull(
                        config.getOrphanFilesCleanDatabase(),
                        "Orphan files clean database can not empty.");
        this.olderThanMillis =
                OrphanFilesClean.olderThanMillis(config.getOrphanFilesCleanOlderThan());
        this.fileCleaner =
                path -> {
                    try {
                        if (catalog.fileIO().isDir(path)) {
                            catalog.fileIO().deleteDirectoryQuietly(path);
                        } else {
                            catalog.fileIO().deleteQuietly(path);
                        }
                    } catch (IOException ignored) {
                    }
                };
        this.parallelism = config.getOrphanFilesCleanParallelism();
    }

    @Override
    public void handle() {
        if (catalog.databaseExists(database)) {
            try {
                List<String> tables = catalog.listTables(database);
                for (String table : tables) {
                    LocalOrphanFilesClean.executeDatabaseOrphanFiles(
                            catalog, database, table, olderThanMillis, fileCleaner, parallelism);
                }
            } catch (Catalog.DatabaseNotExistException databaseNotExistException) {
                LOG.error("Database {} is not exists.", database);
            } catch (Catalog.TableNotExistException tableNotExistException) {
                LOG.error("Table is not exists.", tableNotExistException);
            }
        }
    }
}
