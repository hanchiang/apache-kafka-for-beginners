package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.datagenerator.InvoiceGenerator;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

public class RunnableProducer implements Runnable {
    private static final Logger logger = LogManager.getLogger();
    private boolean shouldStop = false;
    private KafkaProducer<String, PosInvoice> producer;
    private String topicName;
    private InvoiceGenerator invoiceGenerator;
    private int produceDelay;
    private int id;

    RunnableProducer(int id, KafkaProducer<String, PosInvoice> producer, String topicName, int produceDelay, int defaultProduceDelay) {
        this.id = id;
        this.producer = producer;
        this.topicName = topicName;
        this.produceDelay = produceDelay;
        if (this.produceDelay < defaultProduceDelay) {
            logger.info("Delay " + this.produceDelay + " is too low. Setting it to " + defaultProduceDelay);
            this.produceDelay = defaultProduceDelay;
        }
        this.invoiceGenerator = InvoiceGenerator.getInstance();
    }
    @Override
    public void run() {
        try {
            logger.info("Starting producer thread - " + id);
            while (!shouldStop) {
                PosInvoice posInvoice = invoiceGenerator.getNextInvoice();
                producer.send(new ProducerRecord<>(topicName, posInvoice.getStoreID().toString(), posInvoice));
                logger.info("Sleeping thread " + id + " for " + this.produceDelay + "ms");
                Thread.sleep(this.produceDelay);
            }

        } catch (Exception e) {
            logger.error("Exception in Producer thread - " + id);
            throw new RuntimeException(e);
        }

    }

    void shutdown() {
        logger.info("Shutting down producer thread - " + id);
        this.shouldStop = true;
    }
}
