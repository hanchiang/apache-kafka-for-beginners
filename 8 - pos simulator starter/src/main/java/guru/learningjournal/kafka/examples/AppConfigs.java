package guru.learningjournal.kafka.examples;

public class AppConfigs {
    final static int delayBetweenMessage = 500; // milliseconds
    final static int numThreads = 10;
    final static String bootstrapServers = "localhost:9092";
    final static String applicationId = "posJson";
    final static String topicName = "pos";
}
