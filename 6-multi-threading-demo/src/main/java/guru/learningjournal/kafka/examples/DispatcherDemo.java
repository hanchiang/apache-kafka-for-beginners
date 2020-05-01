package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.omg.SendingContext.RunTime;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This Dispatcher main program will create an array of threads to dispatch the send() operations
 */
public class DispatcherDemo {
    private static final Logger logger = LogManager.getLogger();
    public static void main(String[] args) {
        Properties props = new Properties();
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(AppConfigs.kafkaConfigFileLocation);
            props.load(inputStream);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        // 1. Create producer
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);

        // 2. Create threads
        Thread[] dispatchers = new Thread[AppConfigs.eventFiles.length];
        logger.info("Starting dispatcher threads...");
        for (int i = 0; i < AppConfigs.eventFiles.length; i++) {
            dispatchers[i] = new Thread(new Dispatcher(producer, AppConfigs.topicName, AppConfigs.eventFiles[i]));
            dispatchers[i].start();
        }

        // 3. Wait for the threads to complete
        for (Thread t : dispatchers) {
            try {
                t.join();
            } catch (InterruptedException e) {
                logger.error(e.getStackTrace());
                throw new RuntimeException(e);
            } finally {
                // 4. Close producer
                producer.close();
                logger.info("Finished Dispatcher Demo");
            }
        }
    }
}
