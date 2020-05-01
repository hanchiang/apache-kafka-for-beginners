package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * This Dispatcher reads data from file and sends it to kafka
 */
public class Dispatcher implements Runnable {
    private static final Logger logger = LogManager.getLogger();
    private String fileLocation;
    private String topicName;
    private KafkaProducer<Integer, String> producer;

    Dispatcher(KafkaProducer producer, String topicName, String filelocation) {
        this.producer = producer;
        this.topicName = topicName;
        this.fileLocation = filelocation;
    }
    @Override
    public void run() {
        logger.info("Start processing " + fileLocation);
        File file = new File(fileLocation);
        int lineCount = 0;

        try {
            Scanner scanner = new Scanner(file);
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                producer.send(new ProducerRecord<>(topicName, null, line));
                lineCount++;
            }
            logger.info("Finished sending " + lineCount + " messages from " + fileLocation);
        } catch (FileNotFoundException e) {
            logger.error(e.getStackTrace());
            throw new RuntimeException(e);
        }
    }
}
