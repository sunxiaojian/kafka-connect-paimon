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

package io.connect.paimon.sink.naming;

import org.apache.paimon.catalog.Identifier;

import io.connect.paimon.sink.PaimonSinkConfig;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Debezium table naming strategy. */
public class DebeziumTableNamingStrategy implements TableNamingStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumTableNamingStrategy.class);
    private final Pattern sourcePattern = Pattern.compile("\\$\\{(source\\.)(.*?)}");

    @Override
    public Identifier resolveTableName(PaimonSinkConfig config, SinkRecord record) {
        // Default behavior is to replace dots with underscores
        return Identifier.create(
                resolveNameBySource(config, record, config.getWriteDatabaseName()),
                resolveNameBySource(
                        config,
                        record,
                        config.getTableNameFormat().replace("${topic}", record.topic())));
    }

    private String resolveNameBySource(PaimonSinkConfig config, SinkRecord record, String format) {
        String nameFormat = format;
        if (nameFormat.contains("${source.")) {
            if (isTombstone(record)) {
                LOGGER.warn(
                        "Ignore this record because it seems to be a tombstone that doesn't have source field, then cannot resolve table name in topic '{}', partition '{}', offset '{}'",
                        record.topic(),
                        record.kafkaPartition(),
                        record.kafkaOffset());
                return null;
            }
            try {
                Struct source = ((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE);
                Matcher matcher = sourcePattern.matcher(nameFormat);
                while (matcher.find()) {
                    String target = matcher.group();
                    nameFormat = nameFormat.replace(target, source.getString(matcher.group(2)));
                }
            } catch (DataException e) {
                LOGGER.error(
                        "Failed to resolve table name with format '{}', check source field in topic '{}'",
                        config.getTableNameFormat(),
                        record.topic(),
                        e);
                throw e;
            }
        }
        return nameFormat;
    }

    private boolean isTombstone(SinkRecord record) {
        return record.value() == null;
    }
}
