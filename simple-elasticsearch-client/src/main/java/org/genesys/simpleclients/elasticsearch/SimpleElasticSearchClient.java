package org.genesys.simpleclients.elasticsearch;

import java.io.IOException;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

public final class SimpleElasticSearchClient implements AutoCloseable {

  private final RestHighLevelClient client;

  public SimpleElasticSearchClient(final String host, final int port) {

    this.client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port)));
  }

  @SneakyThrows
  public void add(final String index, final String id, final String data) {

    final IndexRequest request = new IndexRequest(index)
        .id(id)
        .source(data, XContentType.JSON);
    client.index(request, RequestOptions.DEFAULT);
  }

  @SneakyThrows
  public void add(final String index, final String id, final Map<String, Object> data) {

    final IndexRequest request = new IndexRequest(index)
        .id(id)
        .source(data);
    client.index(request, RequestOptions.DEFAULT);
  }

  @SneakyThrows
  public void add(final String index, final Map<String, Object> data) {

    final IndexRequest request = new IndexRequest(index).source(data);
    client.index(request, RequestOptions.DEFAULT);
  }

  @Override
  public void close() throws IOException {

    client.close();
  }
}
