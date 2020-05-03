package org.genesys.simpleclients.examples;

import lombok.extern.slf4j.Slf4j;
import org.genesys.simpleclients.kafka.common.FileBasedProperties;
import org.genesys.simpleclients.kafka.common.KafkaProperties;
import org.genesys.simpleclients.kafka.consumer.SimpleKafkaConsumer;
import org.genesys.simpleclients.kafka.producer.SimpleKafkaProducer;

@Slf4j
public class KafkaClientExample {

  public static void main(final String[] args) {

    final String topic = "test-topic";

    final KafkaProperties properties = new FileBasedProperties("config.properties");
    
    try (final SimpleKafkaProducer<String, String> producer = new SimpleKafkaProducer<>(
        properties.get(), topic)) {
      for (int i = 0; i < 10; i++) {
        producer.send("Hello World!! " + i);
      }
    }

    try (final SimpleKafkaConsumer<String, String> consumer = new SimpleKafkaConsumer<>(
        properties.get(), topic)) {
      consumer.receive();
    }
  }
}
