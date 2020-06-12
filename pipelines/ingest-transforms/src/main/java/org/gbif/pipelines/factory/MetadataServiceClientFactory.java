package org.gbif.pipelines.factory;

import org.gbif.pipelines.parsers.config.model.ElasticsearchContentConfig;
import org.gbif.pipelines.parsers.config.model.WsConfig;
import org.gbif.pipelines.parsers.ws.client.metadata.MetadataServiceClient;
import org.gbif.pipelines.transforms.SerializableSupplier;

import lombok.SneakyThrows;

public class MetadataServiceClientFactory {

  private final MetadataServiceClient client;
  private static volatile MetadataServiceClientFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private MetadataServiceClientFactory(
      WsConfig wsConfig, ElasticsearchContentConfig esContentConfig) {
    this.client = MetadataServiceClient.create(wsConfig, esContentConfig);
  }

  /* TODO Comment */
  public static MetadataServiceClient getInstance(
      WsConfig wsConfig, ElasticsearchContentConfig esContentConfig) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new MetadataServiceClientFactory(wsConfig, esContentConfig);
        }
      }
    }
    return instance.client;
  }

  /* TODO Comment */
  public static SerializableSupplier<MetadataServiceClient> createSupplier(
      WsConfig wsConfig, ElasticsearchContentConfig elasticsearchContentConfig) {
    return () -> new MetadataServiceClientFactory(wsConfig, elasticsearchContentConfig).client;
  }

  /* TODO Comment */
  public static SerializableSupplier<MetadataServiceClient> getInstanceSupplier(
      WsConfig wsConfig, ElasticsearchContentConfig elasticsearchContentConfig) {
    return () -> getInstance(wsConfig, elasticsearchContentConfig);
  }
}
