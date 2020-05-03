package org.genesys.simpleclients.kafka.admin;

import java.util.Collections;
import java.util.Properties;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

@Slf4j
public final class SimpleKafkaAdmin implements AutoCloseable {

  private final AdminClient adminClient;

  public SimpleKafkaAdmin(@NonNull final Properties properties) {

    log.debug("Creating Kafka admin client");
    this.adminClient = AdminClient.create(properties);
  }

  public void createTopic(final String name, final int partitions, final short replications) {

    log.debug("Creating new topic: {}", name);
    final NewTopic topic = new NewTopic(name, partitions, replications);
    adminClient.createTopics(Collections.singleton(topic));
  }

  @Override
  public void close() {

    adminClient.close();
  }
}
