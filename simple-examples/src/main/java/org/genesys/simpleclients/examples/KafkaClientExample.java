package org.genesys.simpleclients.examples;

import lombok.extern.slf4j.Slf4j;
import org.genesys.simpleclients.kafka.admin.SimpleKafkaAdmin;
import org.genesys.simpleclients.kafka.common.FileBasedProperties;
import org.genesys.simpleclients.kafka.common.KafkaProperties;
import org.genesys.simpleclients.kafka.consumer.SimpleKafkaConsumer;
import org.genesys.simpleclients.kafka.producer.SimpleKafkaProducer;

@Slf4j
public class KafkaClientExample {

  public static void main(final String[] args) {

    final KafkaClientExample kafkaClientExample = new KafkaClientExample("test-topic");
    kafkaClientExample.createTopic();
    kafkaClientExample.sendMessages();
    kafkaClientExample.receiveMessages();
  }

  private final String topic;

  public KafkaClientExample(final String topic) {

    this.topic = topic;
  }

  private void createTopic() {

    final KafkaProperties adminProperties = new FileBasedProperties("admin.properties");
    try (final SimpleKafkaAdmin admin = new SimpleKafkaAdmin(adminProperties.get())) {
      admin.createTopic(topic, 3, (short) 3);
    }
  }

  private void sendMessages() {

    final KafkaProperties producerProperties = new FileBasedProperties("producer.properties");
    try (final SimpleKafkaProducer<String, String> producer = new SimpleKafkaProducer<>(
        producerProperties.get(), topic)) {
      for (int i = 0; i < 10; i++) {
        producer.send("Hello World!! " + i);
      }
    }
  }

  private void receiveMessages() {

    final KafkaProperties consumerProperties = new FileBasedProperties("consumer.properties");
    try (final SimpleKafkaConsumer<String, String> consumer = new SimpleKafkaConsumer<>(
        consumerProperties.get(), topic)) {
      consumer.receive();
    }
  }
}
