package org.genesys.simpleclients.kafka.consumer;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.genesys.simpleclients.kafka.exception.InvalidConfigurationException;

@Slf4j
public final class SimpleKafkaConsumer<K, V> implements AutoCloseable {

  private final KafkaConsumer<K, V> consumer;

  private final List<ConsumerThread<K, V>> workers;

  public SimpleKafkaConsumer(@NonNull final Properties properties, @NonNull final String topic) {

    log.debug("Creating Kafka consumer for topic: {}", topic);
    this.consumer = new KafkaConsumer<>(properties);
    this.consumer.subscribe(Collections.singletonList(topic));
    this.workers = Collections.singletonList(new ConsumerThread<>(consumer));
  }

  public void receive() {

    if (consumer.subscription().isEmpty()) {
      throw new InvalidConfigurationException();
    }

    workers.forEach(worker -> new Thread(worker).start());
  }

  @Override
  public void close() {

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.debug("Shutting down consumers");
      workers.forEach(ConsumerThread::stop);
    }));
  }
}
