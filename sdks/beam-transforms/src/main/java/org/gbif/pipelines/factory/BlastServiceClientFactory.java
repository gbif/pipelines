package org.gbif.pipelines.factory;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.pipelines.core.ws.blast.BlastServiceClient;
import org.gbif.pipelines.transforms.SerializableSupplier;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class BlastServiceClientFactory {

  public static SerializableSupplier<BlastServiceClient> createSupplier(WsConfig wsConfig) {
    return () -> BlastServiceClient.create(wsConfig);
  }
}
