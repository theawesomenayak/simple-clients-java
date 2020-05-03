package org.genesys.simpleclients.kafka.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class FileBasedProperties implements KafkaProperties {

  private final String fileName;

  public FileBasedProperties(@NonNull final String fileName) {

    this.fileName = fileName;
  }

  @Override
  public Properties get() {

    final Properties properties = new Properties();
    try (final InputStream is = getClass().getClassLoader().getResourceAsStream(fileName)) {
      properties.load(is);
    } catch (final IOException e) {
      log.error("Exception in loading properties from file: {}", fileName, e);
    }
    return properties;
  }
}
