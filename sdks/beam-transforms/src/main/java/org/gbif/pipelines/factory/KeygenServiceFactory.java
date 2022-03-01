package org.gbif.pipelines.factory;

import java.io.IOException;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.hadoop.hbase.client.Connection;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.core.config.model.KeygenConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.common.HbaseConnection;
import org.gbif.pipelines.keygen.common.HbaseConnectionFactory;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KeygenServiceFactory {

  public static SerializableSupplier<HBaseLockingKeyService> getInstanceSupplier(
      PipelinesConfig config, String datasetId) {
    return () -> {
      String zk = getZk(config);

      Connection c = HbaseConnectionFactory.getInstance(zk).getConnection();

      return create(config, c, datasetId);
    };
  }

  public static SerializableSupplier<HBaseLockingKeyService> createSupplier(
      PipelinesConfig config, String datasetId) {
    return () -> {
      String zk = getZk(config);

      Connection c;
      try {
        c = HbaseConnection.create(zk);
      } catch (IOException ex) {
        throw new PipelinesException(ex);
      }

      return create(config, c, datasetId);
    };
  }

  private static HBaseLockingKeyService create(
      PipelinesConfig config, Connection c, String datasetId) {
    org.gbif.pipelines.keygen.config.KeygenConfig keygenConfig =
        org.gbif.pipelines.keygen.config.KeygenConfig.builder()
            .counterTable(config.getKeygen().getCounterTable())
            .lookupTable(config.getKeygen().getLookupTable())
            .occurrenceTable(config.getKeygen().getOccurrenceTable())
            .create();

    return new HBaseLockingKeyService(keygenConfig, c, datasetId);
  }

  private static String getZk(PipelinesConfig config) {
    return Optional.ofNullable(config.getKeygen())
        .map(KeygenConfig::getZkConnectionString)
        .filter(x -> !x.isEmpty())
        .orElse(config.getZkConnectionString());
  }
}
