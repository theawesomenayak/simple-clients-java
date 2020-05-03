package org.genesys.simpleclients.kafka.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.genesys.simpleclients.kafka.exception.InvalidConfigurationException;

@Slf4j
public final class SimpleKafkaConsumer<K, V> implements AutoCloseable {

  private final KafkaConsumer<K, V> consumer;

  private final ExecutorService executorService;

  public SimpleKafkaConsumer(@NonNull final Properties properties, @NonNull final String topic) {

    log.debug("Creating Kafka consumer for topic: {}", topic);
    this.consumer = new KafkaConsumer<>(properties);
    this.consumer.subscribe(Collections.singletonList(topic));
    this.executorService = Executors.newSingleThreadExecutor();
  }

  public void receive() {

    if (consumer.subscription().isEmpty()) {
      throw new InvalidConfigurationException();
    }

    final CountDownLatch latch = new CountDownLatch(5);

    while (latch.getCount() > 0) {
      log.debug("Reading messages");
      final ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(1500));
      if (records.isEmpty()) {
        log.debug("No messages found. Count: {}", latch.getCount());
        latch.countDown();
      } else {
        log.debug("Processing messages");
        executorService.submit(new RecordPrinter<>(records));
      }
    }
  }

  @Override
  public void close() {

    log.debug("Closing consumer");
    this.consumer.close();
  }
}
