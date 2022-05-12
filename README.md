# KafkaTestClient

Simple (single executable jar) Kafka test client to produce one (or more) JSON messages (serialized as a String) to a Kafka topic.

## Clone

```
git clone https://github.com/dhoard/KafkaTestClient
```

## Build

```
cd KafkaTestClient
mvn clean package
```

## Usage

<b>Example 1:</b>

1. Create a Kafka topic `KafkaTestClient` with correct permissions.


2. Create a properties file (`test.properties`)

```
bootstrap.servers=<bootstrap url>
security.protocol=SASL_SSL
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='<username or api key>' password='<password or api secret>';
sasl.mechanism=PLAIN
client.dns.lookup=use_all_dns_ips
session.timeout.ms=45000
acks=all
```

3. Run the test client

```
java -jar target/KafkaTestClient-1.0.0.jar test.properties
```

<b>Example 2:</b>

1. Create a Kafka topic `TEST` with correct permissions.


2. Create a properties file (`test2.properties`)

```
bootstrap.servers=<bootstrap url>
security.protocol=SASL_SSL
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='<username or api key>' password='<password or api secret>';
sasl.mechanism=PLAIN
client.dns.lookup=use_all_dns_ips
session.timeout.ms=45000
acks=all

# Name of the test topic (default is "KafkaTestTopic")
topic.name=TEST

# Number of messages to produce (default is 1)
message.count=10
```

3. Run the test client

```
java -jar target/KafkaTestClient-1.0.0.jar test2.properties
```

# THIS CODE IS UNSUPPORTED

# USE AT YOUR OWN RISK
