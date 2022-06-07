package org.genesys.simpleclients.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

public final class SimpleElasticSearchClient implements AutoCloseable {

  private final ElasticsearchClient client;

  public SimpleElasticSearchClient(final String host, final int port) {

    RestClient restClient = RestClient.builder(new HttpHost(host, port)).build();
    ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
    this.client =  new ElasticsearchClient(transport);
  }

  @SneakyThrows
  public void add(final String index, final String id, final String data) {

    final IndexRequest<String> request = IndexRequest.of(i -> i
            .index(index)
            .id(id)
            .document(data)
    );
    client.index(request);
  }

  @SneakyThrows
  public void add(final String index, final String id, final Map<String, Object> data) {

    final IndexRequest<Map<String, Object>> request = IndexRequest.of(i -> i
            .index(index)
            .id(id)
            .document(data)
    );
    client.index(request);
  }

  @SneakyThrows
  public void add(final String index, final Map<String, Object> data) {

    final IndexRequest<Map<String, Object>> request = IndexRequest.of(i -> i
            .index(index)
            .document(data)
    );
    client.index(request);
  }

  @Override
  public void close() {

    client.shutdown();
  }
}
