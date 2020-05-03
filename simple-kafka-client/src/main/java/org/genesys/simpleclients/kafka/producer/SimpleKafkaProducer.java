package org.genesys.simpleclients.kafka.producer;

import java.util.Properties;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public final class SimpleKafkaProducer<K, V> implements AutoCloseable {

  private final KafkaProducer<K, V> producer;

  private final String topic;

  public SimpleKafkaProducer(@NonNull final Properties properties, @NonNull final String topic) {

    log.debug("Creating Kafka producer for topic: {}", topic);
    this.producer = new KafkaProducer<>(properties);
    this.topic = topic;
  }

  public void send(@NonNull final V value) {

    final ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, value);
    send(producerRecord);
  }

  public void send(@NonNull final ProducerRecord<K, V> record) {

    send(record, (metadata, e) -> {
      if (e != null) {
        log.error("Send failed for record {}", record, e);
      }
    });
  }

  public void send(@NonNull final ProducerRecord<K, V> record, @NonNull final Callback callback) {

    log.debug("Sending Message: {}", record.value());
    producer.send(record, callback);
    producer.flush();
  }

  @Override
  public void close() {

    log.debug("Closing producer");
    this.producer.close();
  }
}
