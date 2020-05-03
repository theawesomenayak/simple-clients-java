package org.genesys.simpleclients.kafka.consumer;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;

@Slf4j
public final class RecordPrinter<K, V> implements Runnable {

  private final ConsumerRecords<K, V> records;

  public RecordPrinter(@NonNull final ConsumerRecords<K, V> records) {

    this.records = records;
  }

  @Override
  public void run() {

    records.forEach(record ->
        log.info("Consumer Record:({}, {}, {}, {})",
            record.key(), record.value(), record.partition(), record.offset()));
  }
}
