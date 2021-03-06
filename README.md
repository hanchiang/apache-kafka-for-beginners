# Introduction

This project contains simple applications demonstrating the usage of the kafka producer consumer API from this [course](https://www.udemy.com/course/apache-kafka-for-beginners)

# Assumptions

* [Confluent kafka community edition](https://www.confluent.io/download) is installed
* [Intellij IDEA](https://www.jetbrains.com/idea/) is installed
* [JDK 1.8](https://www.oracle.com/java/technologies/javase-jdk8-downloads.html) is installed
* Kafka installation directory is set as `$KAFKA_HOME`
* Kafka `bin` directory is added to `$PATH`
* Log directory for zookeeper and kafka are set
  * kafka: `../tmp/<log-name>` in `etc/kafka/server.properties`
  * zookeeper: `../tmp/zookeeper` in `etc/kafka/zookeeper.properties`
