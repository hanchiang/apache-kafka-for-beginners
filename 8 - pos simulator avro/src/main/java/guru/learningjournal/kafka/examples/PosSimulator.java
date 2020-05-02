package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.types.PosInvoice;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PosSimulator {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        // Confluent schema registry is required for avro serialiser
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfigs.schemaRegistryServers);

        KafkaProducer<String, PosInvoice> kafkaProducer = new KafkaProducer<>(properties);
        ExecutorService executor = Executors.newFixedThreadPool(AppConfigs.numThreads);
        final List<RunnableProducer> runnableProducers = new ArrayList<>();

        for (int i = 0; i < AppConfigs.numThreads; i++) {
            RunnableProducer runnableProducer = new RunnableProducer(i, kafkaProducer, AppConfigs.topicName, AppConfigs.produceDelay, AppConfigs.defaultProduceDelay);
            runnableProducers.add(runnableProducer);
            executor.submit(runnableProducer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (RunnableProducer p : runnableProducers) {
                p.shutdown();
            }
            executor.shutdown();
            logger.info("Closing Executor Service");
            try {
                executor.awaitTermination(AppConfigs.produceDelay * 2, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.error(e.getStackTrace());
                throw new RuntimeException(e);
            }
        }));

    }
}
