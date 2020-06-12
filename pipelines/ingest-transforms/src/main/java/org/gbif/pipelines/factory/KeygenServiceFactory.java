package org.gbif.pipelines.factory;

import java.io.IOException;

import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.common.HbaseConnection;
import org.gbif.pipelines.keygen.common.HbaseConnectionFactory;
import org.gbif.pipelines.keygen.config.KeygenConfig;
import org.gbif.pipelines.transforms.SerializableSupplier;

import org.apache.hadoop.hbase.client.Connection;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KeygenServiceFactory {

  public static SerializableSupplier<HBaseLockingKeyService> getInstanceSupplier(
      KeygenConfig config, String datasetId) {
    return () -> {
      Connection c = HbaseConnectionFactory.getInstance(config.getHbaseZk()).getConnection();
      return new HBaseLockingKeyService(config, c, datasetId);
    };
  }

  public static SerializableSupplier<HBaseLockingKeyService> createSupplier(
      KeygenConfig config, String datasetId) {
    return () -> {
      Connection c;
      try {
        c = HbaseConnection.create(config.getHbaseZk());
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
      return new HBaseLockingKeyService(config, c, datasetId);
    };
  }
}
