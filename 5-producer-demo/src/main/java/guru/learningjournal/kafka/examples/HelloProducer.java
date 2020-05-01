package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.Properties;

public class HelloProducer {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String [] args) {
        logger.info("Creating kafka producer...");

        // 1. Create the necessary producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        /*
            A kafka message should be a key-value pair, and must be serialised into bytes
            to be transmitted over the network.
            In real-world scenarios, serialisation of complex objects are expected, rather than plain integer and string
         */
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. Create a producer
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);

        // 3. Send message to kafka
        logger.info("Sending messages to kafka");
        for (int i = 0; i < AppConfigs.numEvents; i++) {
            producer.send(new ProducerRecord<>(AppConfigs.topicName, i, "Simple message: " + i));
        }

        logger.info("Finished sending messages to kafka");

        // 4. Close the producer
        producer.close();
    }
}
