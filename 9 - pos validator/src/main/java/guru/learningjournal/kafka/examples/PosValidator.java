package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.JsonDeserializer;
import guru.learningjournal.kafka.examples.serde.JsonSerializer;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * This program consist of a consumer that reads and validates PosInvoice data, and a producer
 * that publishes the data to a valid-pos and invalid-pos topic
 */
public class PosValidator {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        // Create consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        // Target class name for deserialised data
        consumerProps.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, PosInvoice.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, AppConfigs.groupID);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AppConfigs.offsetReset);

        KafkaConsumer<String, PosInvoice> consumer = new KafkaConsumer(consumerProps);

        // Subscribe to topic
        consumer.subscribe(Arrays.asList(AppConfigs.sourceTopicNames));

        // Create producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        KafkaProducer<String, PosInvoice> producer = new KafkaProducer(producerProps);

        // Poll messages from kafka and process them
        while (true) {
            ConsumerRecords<String, PosInvoice> records = consumer.poll(Duration.ofMillis(AppConfigs.pollTimeout));
            for (ConsumerRecord<String, PosInvoice> record: records) {
                PosInvoice recordValue = record.value();
                // invalid record
                if (recordValue.getDeliveryType().equals("HOME-DELIVERY") &&
                        recordValue.getDeliveryAddress().getContactNumber().equals("")) {
                    producer.send(new ProducerRecord<>(AppConfigs.invalidTopicName, recordValue.getStoreID(), recordValue));
                    logger.info("Invalid record: " + recordValue.getInvoiceNumber());
                } else {
                    producer.send(new ProducerRecord<>(AppConfigs.validTopicName, recordValue.getStoreID(), recordValue));
                    logger.info("Valid record: " + recordValue.getInvoiceNumber());
                }
            }
        }
    }
}
