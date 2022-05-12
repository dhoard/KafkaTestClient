package com.github.dhoard.kafka;

import java.io.FileReader;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import com.google.gson.JsonObject;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
        if ((args == null) || (args.length != 1)) {
            System.out.println("Usage: java -jar <jar> <properties filename>");
            System.exit(1);
        }

        String filename = args[0];
        LOGGER.info("properties = [" + filename + "]");

        Properties properties = new Properties();
        properties.load(new FileReader(filename));

        String topicName = "KafkaTestClient";

        if (properties.containsKey("topic.name")) {
            topicName = properties.getProperty("topic.name");
        }

        LOGGER.info("topic.name = [" + topicName + "]");

        int messageCount = 1;

        if (properties.containsKey("message.count")) {
            try {
                messageCount = Integer.parseInt(properties.getProperty("message.count"));
            } catch (NumberFormatException e) {
                LOGGER.error("Invalid 'message.count'", e);
            }
        }

        if (messageCount < 1) {
            messageCount = 1;
        }

        LOGGER.info("message.count = [" + messageCount + "]");

        // Remove properties not used by the KafkaProducer
        properties.remove("topic.name");
        properties.remove("schema.registry.url");
        properties.remove("basic.auth.user.info");
        properties.remove("session.timeout.ms");
        properties.remove("message.count");

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        if (!properties.containsKey("max.block.ms")) {
            properties.setProperty("max.block.ms", "5000");
        }

        List<JsonObject> jsonObjectList = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            String id = UUID.randomUUID().toString();
            String data = randomString(10);
            String timestamp = toISOTimestamp(System.currentTimeMillis(), TimeZone.getDefault().getID());

            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("id", id);
            jsonObject.addProperty("source", KafkaTestClient.class.getName());
            jsonObject.addProperty("data", data);
            jsonObject.addProperty("timestamp", timestamp);

            jsonObjectList.add(jsonObject);
        }

        ExtendedCallback extendedCallback = new ExtendedCallback(messageCount);
        KafkaProducer<String, String> kafkaProducer = null;

        try {
            kafkaProducer = new KafkaProducer<>(properties);

            for (JsonObject jsonObject : jsonObjectList) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(
                                topicName,
                                jsonObject.get("id").getAsString(),
                                jsonObject.toString());

                LOGGER.info("Producing message = [" + jsonObject.toString() + "]");

                kafkaProducer.send(producerRecord, extendedCallback);
            }

            // Wait for all messages to be attempted to be produced
            extendedCallback.await();
        } catch (Throwable t) {
            LOGGER.error("Exception producing messages", t);
        } finally {
            if (kafkaProducer != null) {
                kafkaProducer.close();
            }

            LOGGER.info("Successfully produced " + extendedCallback.getSuccessCount() + " message(s)");

            if (extendedCallback.getSuccessCount() != messageCount) {
                System.exit(1);
            }
        }
    }

    class ExtendedCallback implements Callback {

        private CountDownLatch countDownLatch;
        private int successCount;

        public ExtendedCallback(int count) {
            this.countDownLatch = new CountDownLatch(count);
            this.successCount = 0;
        }

        public void await() throws InterruptedException {
            this.countDownLatch.await();
        }

        public int getSuccessCount() {
            return this.successCount;
        }

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                LOGGER.error("Exception producing message", e);
            } else {
                this.successCount++;
            }

            this.countDownLatch.countDown();
        }
    }

    class Holder<T> {

        public T value;

        public Holder(T value) {
            this.value = value;
        }

        public String toString() {
            if (this.value != null) {
                return this.value.toString();
            }

            return null;
        }
    }

    private static String toISOTimestamp(long milliseconds, String timeZoneId) {
        return Instant.ofEpochMilli(milliseconds).atZone(ZoneId.of(timeZoneId)).toString().replace("[" + timeZoneId + "]", "");
    }

    private static String randomString(int length) {
        return RANDOM.ints(48, 122 + 1)
            .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
            .limit(length)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();
    }
}
