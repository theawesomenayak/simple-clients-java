package org.genesys.simpleclients.examples;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.genesys.simpleclients.elasticsearch.SimpleElasticSearchClient;

public final class ElasticSearchClientExample {

  public static void main(final String[] args) throws IOException {

    try (final SimpleElasticSearchClient client = new SimpleElasticSearchClient("localhost",
        9200)) {
      new Random().ints(100, 30, 80)
          .forEach(age -> {
            final Map<String, Object> data = new HashMap<>();
            data.put("Age", age);
            client.add("demographics", data);
          });
    }
  }
}
