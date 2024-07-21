# Kafka Connect Docker Deployment Document

## 1. Environment Setup

Ensure your environment meets the following requirements:

- **Docker**: Docker installed and running.
- **Git**: Git installed for cloning the repository.
- **Java**: Java 8 or later for building the project (if building outside Docker).
- **Maven**: Maven for building the project (if building outside Docker).

## 2. Clone and Build the Paimon Kafka Connect Project

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
mvn clean package
```

After the build is complete, you will find the compiled JAR file in the `target` directory.

## 3. Prepare Docker Environment

### Step 1: Create a Dockerfile

Create a `Dockerfile` in the root directory of the Kafka Connect project:

```Dockerfile
FROM confluentinc/cp-kafka-connect-base:latest

# Copy the compiled connector JAR file into the plugins directory
COPY target/paimon-kafka-connect-*.jar /etc/kafka-connect/jars/

# Set the classpath to include the plugins directory
ENV CLASSPATH="/etc/kafka-connect/jars/*"

# Expose the ports for Kafka Connect
EXPOSE 8083
```

### Step 2: Create a Docker Compose File

Create a `docker-compose.yml` file to define the services:

```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-server:latest
    ports:
      - "9092:9092"
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_BROKER_ID: 1
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  connect:
    build: .
    ports:
      - "8083:8083"
    depends_on:
      - zookeeper
      - kafka
    environment:
      CONNECT_BOOTSTRAP_SERVERS: kafka:29092
      CONNECT_REST_PORT: 8083
      CONNECT_GROUP_ID: compose-connect-group
      CONNECT_CONFIG_STORAGE_TOPIC: deploy-connect-configs
      CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_OFFSET_FLUSH_INTERVAL_MS: 10000
      CONNECT_STATUS_STORAGE_TOPIC: deploy-connect-status
      CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_KEY_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_INTERNAL_KEY_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_INTERNAL_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_ZOOKEEPER_CONNECT: zookeeper:2181
```

## 4. Build and Run the Docker Containers

### Step 1: Build the Docker Image

Build the Docker image for Kafka Connect with the Paimon connector:

```bash
deploy build -t paimon-kafka-connect .
```

### Step 2: Start the Docker Containers

Start the Docker containers using Docker Compose:

```bash
deploy-compose up -d
```

This will start Zookeeper, Kafka, and Kafka Connect with the Paimon connector.

## 5. Verify Deployment

You can verify the connector's operation through Kafka Connect's REST API.

## 6. Monitoring and Logging

Monitor Kafka Connect's status and check the logs to ensure there are no errors:

```bash
deploy logs [container_id_or_name]
```

## 7. Common Issues

- **Docker Build Issues**: Ensure all necessary files are in place and the Dockerfile is correctly configured.
- **Network Connectivity**: Ensure the containers can communicate with each other.
- **Configuration Errors**: Verify the parameters in the `docker-compose.yml` and environment variables.

---

Please adjust the parameters and configurations in the above document according to your actual situation. If you encounter any issues or need further assistance, feel free to reach out!
