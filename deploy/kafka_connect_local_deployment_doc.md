# Kafka Connect Local Deployment Document

## 1. Environment Setup

Before you begin, ensure that your environment meets the following requirements:

- **Operating System**: Linux or macOS.
- **Java Version**: Java 8 or later.
- **Kafka Version**: Apache Kafka 2.0 or later.
- **Zookeeper**: Zookeeper service must be running.
- **Maven**: For building the project.

## 2. Download and Install Apache Kafka

### Step 1: Download Apache Kafka

Download the latest version of Apache Kafka from the official website:

```bash
wget https://downloads.apache.org/kafka/2.8.0/kafka_2.13-2.8.0.tgz
```

### Step 2: Extract Apache Kafka

Extract the downloaded Kafka archive:

```bash
tar -xzf kafka_2.13-2.8.0.tgz
cd kafka_2.13-2.8.0
```

## 3. Start Zookeeper and Kafka

### Step 1: Start Zookeeper

Start the Zookeeper service:

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

### Step 2: Start Kafka

Start the Kafka service:

```bash
bin/kafka-server-start.sh config/server.properties
```

## 4. Clone and Build the Paimon Kafka Connect Project

### Step 1: Clone the Repository

Clone the Paimon Kafka Connect project from GitHub:

```bash
git clone https://github.com/sunxiaojian/paimon-kafka-connect.git
cd paimon-kafka-connect
```

If you encounter issues cloning the repository, please check the URL for correctness and ensure your network connection is stable. You may need to retry or check the repository's status.

### Step 2: Build the Project

Build the project using Maven:

```bash
mvn clean package -Dmaven.test.skip=true
```

After the build is complete, you will find the compiled JAR file in the `target` directory.

## 5. Configure Kafka Connect

### Step 1: Create a Plugins Directory

Create a `plugins` directory within your Kafka installation directory:

```bash
mkdir plugins
```

### Step 2: Copy the Compiled JAR

Copy the compiled Paimon Kafka Connect JAR file to the `plugins` directory:

```bash
cp target/paimon-kafka-connect-*.jar plugins/
```

### Step 3: Configure Kafka Connect

Create a `connect-distributed.properties` file with the necessary configurations to connect to your Kafka cluster and Zookeeper:

```properties
##
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##

# This file contains some of the configurations for the Kafka Connect distributed worker. This file is intended
# to be used with the examples, and some settings may differ from those used in a production system, especially
# the `bootstrap.servers` and those specifying replication factors.

# A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
bootstrap.servers=localhost:9092

# unique name for the cluster, used in forming the Connect cluster group. Note that this must not conflict with consumer group IDs
group.id=connect-cluster

# The converters specify the format of data in Kafka and how to translate it into Connect data. Every Connect user will
# need to configure these based on the format they want their data in when loaded from or stored into Kafka
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
# Converter-specific settings can be passed in by prefixing the Converter's setting with the converter we want to apply
# it to
key.converter.schemas.enable=true
value.converter.schemas.enable=true

# Topic to use for storing offsets. This topic should have many partitions and be replicated and compacted.
# Kafka Connect will attempt to create the topic automatically when needed, but you can always manually create
# the topic before starting Kafka Connect if a specific topic configuration is needed.
# Most users will want to use the built-in default replication factor of 3 or in some cases even specify a larger value.
# Since this means there must be at least as many brokers as the maximum replication factor used, we'd like to be able
# to run this example on a single-broker cluster and so here we instead set the replication factor to 1.
offset.storage.topic=connect-offsets
offset.storage.replication.factor=1
#offset.storage.partitions=25

max.request.size=10485760

topic.creation.enable=true
topic.creation.default.replication.factor=1
topic.creation.default.partitions=12

# Topic to use for storing connector and task configurations; note that this should be a single partition, highly replicated,
# and compacted topic. Kafka Connect will attempt to create the topic automatically when needed, but you can always manually create
# the topic before starting Kafka Connect if a specific topic configuration is needed.
# Most users will want to use the built-in default replication factor of 3 or in some cases even specify a larger value.
# Since this means there must be at least as many brokers as the maximum replication factor used, we'd like to be able
# to run this example on a single-broker cluster and so here we instead set the replication factor to 1.
config.storage.topic=connect-configs
config.storage.replication.factor=1

# Topic to use for storing statuses. This topic can have multiple partitions and should be replicated and compacted.
# Kafka Connect will attempt to create the topic automatically when needed, but you can always manually create
# the topic before starting Kafka Connect if a specific topic configuration is needed.
# Most users will want to use the built-in default replication factor of 3 or in some cases even specify a larger value.
# Since this means there must be at least as many brokers as the maximum replication factor used, we'd like to be able
# to run this example on a single-broker cluster and so here we instead set the replication factor to 1.
status.storage.topic=connect-status
status.storage.replication.factor=1
#status.storage.partitions=5

# Flush much faster than normal, which is useful for testing/debugging
offset.flush.interval.ms=10000

# These are provided to inform the user about the presence of the REST host and port configs 
# Hostname & Port for the REST API to listen on. If this is set, it will bind to the interface used to listen to requests.
#rest.host.name=
#rest.port=8083

topic.creation.default.replication.factor=1
topic.creation.default.partitions=12

offset.flush.interval.ms=10000
max.request.size=20971520
producer.max.request.size=20971520
consumer.max.request.size=20971520

# The Hostname & Port that will be given out to other workers to connect to i.e. URLs that are routable from other servers.
#rest.advertised.host.name=
#rest.advertised.port=

# Set to a list of filesystem paths separated by commas (,) to enable class loading isolation for plugins
# (connectors, converters, transformations). The list should consist of top level directories that include 
# any combination of: 
# a) directories immediately containing jars with plugins and their dependencies
# b) uber-jars with plugins and their dependencies
# c) directories immediately containing the package directory structure of classes of plugins and their dependencies
# Examples: 
# plugin.path=/usr/local/share/java,/usr/local/share/kafka/plugins,/opt/connectors,
plugin.path=plugins/

connector.client.config.override.policy=All
```

## 6. Start Kafka Connect

Start the Kafka Connect service with the `connect-distributed` script:

```bash
bin/connect-distributed.sh connect-distributed.properties
```

## 7. Create a Connector

Create a configuration file for your connector, for example, `paimon-sink-connector.properties`, and configure your Paimon Kafka Connect connector:

```json
{
  "name": "paimo-sink-test-07",
  "config": {
    "tasks.max": 1,
    "connector.class": "io.connect.paimon.sink.PaimonSinkConnector",
    "topics": "debezium-mysql-sourc-002.test_database.products_cdc",
    "database.name.format": "${source.db}",
    "table.name.format":"${source.table}",
    "auto.create": true,
    "auto.evolve": true,
    "table.config.file.format":"parquet",
    "table.default-primary-keys": "id",
    "table.config.bucket": 2,
    "table.config.bucket-key": "id",
    "catalog.config.warehouse":"/tmp/paimon",
    "enabled.orphan.files.clean": true,
    "orphan.files.clean.database": "default",
    "table.config.snapshot.num-retained.min": 10,
    "table.config.snapshot.num-retained.max": 100,
    "table.config.snapshot.time-retained": "1h"
  }
}
```

Use the `curl` command or Kafka Connect's REST API to create the connector:

```bash
curl -X POST -H "Content-Type: application/json" --data @paimon-sink-connector.properties http://localhost:8083/connectors
```

## 8. Verify Deployment

You can verify the connector's operation through Kafka Connect's REST API.

## 9. Monitoring and Logging

Monitor Kafka Connect's status and check the logs to ensure there are no errors.

## 10. Common Issues

- **Dependency Issues**: Ensure all dependencies are correctly installed.
- **Configuration Errors**: Verify the parameters in the configuration file.
- **Permission Issues**: Ensure Kafka Connect has the necessary permissions to access the Kafka cluster and Zookeeper service.

---

Please adjust the parameters and configurations in the above document according to your actual situation. If you encounter any issues or need further assistance, feel free to reach out!
