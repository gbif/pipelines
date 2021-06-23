package org.gbif.pipelines.validator.factory;

import java.util.Arrays;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

@Slf4j
public class ElasticsearchClientFactory {

  private static volatile ElasticsearchClientFactory instance;

  private static final Object MUTEX = new Object();

  private final RestHighLevelClient client;

  private ElasticsearchClientFactory(String[] esHosts) {
    HttpHost[] hosts = Arrays.stream(esHosts).map(HttpHost::create).toArray(HttpHost[]::new);
    this.client = new RestHighLevelClient(RestClient.builder(hosts));
  }

  public static RestHighLevelClient getInstance(String[] esHosts) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new ElasticsearchClientFactory(esHosts);
        }
      }
    }
    return instance.client;
  }

  @SneakyThrows
  public void close() {
    if (client != null) {
      client.close();
    }
  }
}
