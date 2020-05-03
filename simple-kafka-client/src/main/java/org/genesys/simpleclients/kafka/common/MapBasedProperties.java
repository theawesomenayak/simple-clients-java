package org.genesys.simpleclients.kafka.common;

import java.util.Map;
import java.util.Properties;
import lombok.NonNull;
import org.apache.kafka.clients.producer.ProducerConfig;

public final class MapBasedProperties implements KafkaProperties {

  private final Map<ProducerConfig, String> configMap;

  public MapBasedProperties(@NonNull final Map<ProducerConfig, String> configMap) {

    this.configMap = configMap;
  }

  @Override
  public final Properties get() {

    final Properties properties = new Properties();
    properties.putAll(configMap);
    return properties;
  }
}
