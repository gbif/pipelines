package org.gbif.pipelines.factory;

import java.io.IOException;

import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.common.HbaseConnection;
import org.gbif.pipelines.keygen.common.HbaseConnectionFactory;
import org.gbif.pipelines.parsers.config.model.PipelinesConfig;
import org.gbif.pipelines.transforms.SerializableSupplier;

import org.apache.hadoop.hbase.client.Connection;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KeygenServiceFactory {

  public static SerializableSupplier<HBaseLockingKeyService> getInstanceSupplier(
      PipelinesConfig config, String datasetId) {
    return () -> {
      Connection c =
          HbaseConnectionFactory.getInstance(config.getKeygen().getZkConnectionString())
              .getConnection();

      org.gbif.pipelines.keygen.config.KeygenConfig keygenConfig =
          org.gbif.pipelines.keygen.config.KeygenConfig.builder()
              .counterTable(config.getKeygen().getCounterTable())
              .lookupTable(config.getKeygen().getLookupTable())
              .occurrenceTable(config.getKeygen().getOccurrenceTable())
              .zkConnectionString(config.getKeygen().getZkConnectionString())
              .create();

      return new HBaseLockingKeyService(keygenConfig, c, datasetId);
    };
  }

  public static SerializableSupplier<HBaseLockingKeyService> createSupplier(
      PipelinesConfig config, String datasetId) {
    return () -> {
      Connection c;
      try {
        c = HbaseConnection.create(config.getKeygen().getZkConnectionString());
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }

      org.gbif.pipelines.keygen.config.KeygenConfig keygenConfig =
          org.gbif.pipelines.keygen.config.KeygenConfig.builder()
              .counterTable(config.getKeygen().getCounterTable())
              .lookupTable(config.getKeygen().getLookupTable())
              .occurrenceTable(config.getKeygen().getOccurrenceTable())
              .zkConnectionString(config.getKeygen().getZkConnectionString())
              .create();

      return new HBaseLockingKeyService(keygenConfig, c, datasetId);
    };
  }
}
