package org.genesys.simpleclients.kafka.consumer;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

@Slf4j
public final class ConsumerThread<K, V> implements Runnable {

  private final KafkaConsumer<K, V> consumer;

  private final AtomicBoolean running;

  public ConsumerThread(@NonNull final KafkaConsumer<K, V> consumer) {

    this.consumer = consumer;
    this.running = new AtomicBoolean(true);
  }

  @Override
  public void run() {

    while (running.get()) {
      log.debug("Reading messages in {}", Thread.currentThread().getName());
      final ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(1500));
      records.forEach(record ->
          log.info("Consumer Record:({}, {}, {}, {})",
              record.key(), record.value(), record.partition(), record.offset()));
    }
  }

  public void stop() {

    log.debug("Stopping consumer {}", Thread.currentThread().getName());
    running.getAndSet(false);
  }
}
