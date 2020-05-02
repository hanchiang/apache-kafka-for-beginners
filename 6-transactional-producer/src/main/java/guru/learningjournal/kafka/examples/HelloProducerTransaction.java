package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.Properties;

/**
 * This program demonstrates producer transaction across two topics
 */
public class HelloProducerTransaction {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        logger.info("Creating Kafka Producer...");
        Properties props = new Properties();
        try {
            InputStream inputStream = new FileInputStream(AppConfigs.propertiesFile);
            props.load(inputStream);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            // Set a transaction id in order to use producer transactions
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, AppConfigs.transactionId);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();

        // First transaction
        logger.info("Starting transaction...");
        producer.beginTransaction();
        try {
            for (int i = 1; i <= AppConfigs.numEvents; i++) {
                producer.send(new ProducerRecord<>(AppConfigs.topicName1, i, "Simple Message-T1-" + i));
                producer.send(new ProducerRecord<>(AppConfigs.topicName2, i, "Simple Message-T1-" + i));
            }
            logger.info("Committing tranasction");
            producer.commitTransaction();
        } catch (Exception e) {
            logger.error(e.getStackTrace());
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }
        logger.info("Complete transaction. Closing kafka producer.");

        // Second transaction
        logger.info("Starting second transaction...");
        producer.beginTransaction();
        try {
            for (int i = 1; i <= AppConfigs.numEvents; i++) {
                producer.send(new ProducerRecord<>(AppConfigs.topicName1, i, "Simple Message-T2-" + i));
                producer.send(new ProducerRecord<>(AppConfigs.topicName2, i, "Simple Message-T2-" + i));
            }
            logger.info("Committing tranasction");
            producer.abortTransaction();
        } catch (Exception e) {
            logger.error(e.getStackTrace());
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }

        logger.info("Complete transaction. Closing kafka producer.");
        producer.close();

    }
}
