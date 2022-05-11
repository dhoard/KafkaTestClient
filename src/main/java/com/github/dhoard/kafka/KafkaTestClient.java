package com.github.dhoard.kafka;

import java.io.FileReader;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;

import com.google.gson.JsonObject;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTestClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTestClient.class);

    private static final Random RANDOM = new Random();

    public static void main(String[] args) throws Exception {
        new KafkaTestClient().execute(args);
    }

    public void execute(String[] args) throws Exception {
        if ((args == null) && (args.length != 1)) {
            LOGGER.info("Usage: java -jar <jar> <properties filename>");
            System.exit(1);
        }

        String filename = args[0];
        LOGGER.info("properties = [" + filename + "]");

        Properties properties = new Properties();
        properties.load(new FileReader(filename));

        String topicName = properties.getProperty("topic.name");
        LOGGER.info("topic.name = [" + topicName + "]");

        int messageCount = 1;

        if (properties.containsKey("message.count")) {
            try {
                messageCount = Integer.parseInt(properties.getProperty("message.count"));
            } catch (NumberFormatException e) {
                // DO NOTHING
            }
        }

        if (messageCount < 1) {
            messageCount = 1;
        }

        LOGGER.info("message.count = [" + messageCount + "]");

        properties.remove("topic.name");
        properties.remove("schema.registry.url");
        properties.remove("basic.auth.user.info");
        properties.remove("session.timeout.ms");

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        properties.setProperty("max.block.ms", "5000");

        KafkaProducer<String, String> kafkaProducer = null;

        try {
            kafkaProducer = new KafkaProducer<>(properties);

            for (int i = 0; i < messageCount; i++) {
                String id = UUID.randomUUID().toString();
                String data = randomString(10);
                String timestamp = toISOTimestamp(System.currentTimeMillis(), TimeZone.getDefault().getID());

                JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty("id", id);
                jsonObject.addProperty("source", KafkaTestClient.class.getName());
                jsonObject.addProperty("data", data);
                jsonObject.addProperty("timestamp", timestamp);

                LOGGER.info("message = [" + jsonObject.toString() + "]");

                ProducerRecord<String, String> producerRecord = new ProducerRecord(topicName, id, jsonObject.toString());
                kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
                    if (e != null) {
                        LOGGER.error("error producing message", e);
                    } else {
                        LOGGER.info("successfully produced message");
                    }
                }).get();

                if (messageCount > 1) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // DO NOTHING
                    }
                }
            }
        } catch (Throwable t) {
            //LOGGER.error("ERROR", t);
        } finally {
            if (null != kafkaProducer) {
                kafkaProducer.close();
            }
        }
    }

    private static String getISOTimestamp() {
        return toISOTimestamp(System.currentTimeMillis(), "America/New_York");
    }

    private static String toISOTimestamp(long milliseconds, String timeZoneId) {
        return Instant.ofEpochMilli(milliseconds).atZone(ZoneId.of(timeZoneId)).toString().replace("[" + timeZoneId + "]", "");
    }

    private String randomString(int length) {
        return RANDOM.ints(48, 122 + 1)
            .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
            .limit(length)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();
    }
}
