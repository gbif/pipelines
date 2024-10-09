package org.gbif.pipelines.factory;

import lombok.SneakyThrows;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;

public class MetadataServiceClientFactory {

  private final MetadataServiceClient client;
  private static volatile MetadataServiceClientFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private MetadataServiceClientFactory(PipelinesConfig config) {
    this.client = MetadataServiceClient.create(config.getGbifApi(), config.getContent());
  }

  public static MetadataServiceClient getInstance(PipelinesConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new MetadataServiceClientFactory(config);
        }
      }
    }
    return instance.client;
  }

  public static SerializableSupplier<MetadataServiceClient> createSupplier(PipelinesConfig config) {
    return () -> new MetadataServiceClientFactory(config).client;
  }

  public static SerializableSupplier<MetadataServiceClient> getInstanceSupplier(
      PipelinesConfig config) {
    return () -> getInstance(config);
  }
}
