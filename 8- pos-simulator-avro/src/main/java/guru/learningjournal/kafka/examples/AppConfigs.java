package guru.learningjournal.kafka.examples;

public class AppConfigs {
    public final static String applicationID = "PosSimulator";
    public final static String bootstrapServers = "localhost:9092,localhost:9093";
    public final static String schemaRegistryServers = "http://localhost:8081";
    public final static int numThreads = 3;
    public final static int produceDelay = 200;
    public final static int defaultProduceDelay= 200;
    public final static String topicName = "pos";
}
