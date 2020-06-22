package org.gbif.pipelines.factory;

import org.gbif.pipelines.parsers.config.model.WsConfig;
import org.gbif.pipelines.parsers.ws.client.blast.BlastServiceClient;
import org.gbif.pipelines.transforms.SerializableSupplier;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class BlastServiceClientFactory {

  public static SerializableSupplier<BlastServiceClient> createSupplier(WsConfig wsConfig) {
    return ()-> BlastServiceClient.create(wsConfig);
  }
}
