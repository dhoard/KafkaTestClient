# KafkaTestClient

Simple (single executable jar) Kafka test client to publish a JSON message (as a String) to a topic.

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

Create a properties file...

Example `test.properties`:

```
topic.name=TEST
message.count=1
bootstrap.servers=<bootstrap url>
security.protocol=SASL_SSL
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='<username or api key>' password='<password or api secret>';
sasl.mechanism=PLAIN
client.dns.lookup=use_all_dns_ips
session.timeout.ms=45000
acks=all
```

## Run the test client

```
java -jar target/KafkaTestClient-1.0.0.jar test.properties
```

# THIS CODE IS UNSUPPORTED

# USE AT YOUR OWN RISK
