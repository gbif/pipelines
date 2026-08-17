package org.gbif.pipelines.validator.factory;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import java.util.Arrays;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

@Slf4j
public class ElasticsearchClientFactory {

  private static volatile ElasticsearchClientFactory instance;

  private static final Object MUTEX = new Object();

  private final ElasticsearchClient client;

  private ElasticsearchClientFactory(String[] esHosts) {
    log.info("Create ES client");
    HttpHost[] hosts = Arrays.stream(esHosts).map(HttpHost::create).toArray(HttpHost[]::new);
    this.client =
        new ElasticsearchClient(
            new RestClientTransport(RestClient.builder(hosts).build(), new JacksonJsonpMapper()));
  }

  public static ElasticsearchClient getInstance(String... esHosts) {
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
      log.info("Close ES client");
      client.close();
    }
  }
}
