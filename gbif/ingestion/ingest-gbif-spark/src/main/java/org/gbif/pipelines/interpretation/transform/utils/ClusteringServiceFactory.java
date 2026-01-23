package org.gbif.pipelines.interpretation.transform.utils;

import java.io.IOException;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.client.Connection;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.core.config.model.ClusteringRelationshipConfig;
import org.gbif.pipelines.core.config.model.KeygenConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.parsers.clustering.ClusteringService;
import org.gbif.pipelines.keygen.common.HbaseConnection;

public class ClusteringServiceFactory {

  private final ClusteringService service;
  private static volatile ClusteringServiceFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private ClusteringServiceFactory(Connection c, ClusteringRelationshipConfig config) {
    this.service = ClusteringService.create(c, config);
  }

  public static SerializableSupplier<ClusteringService> createSupplier(PipelinesConfig config) {
    return () -> {
      if (config.getClusteringRelationshipConfig() == null
          || config.getClusteringRelationshipConfig().getRelationshipTableName() == null) {
        return null;
      }

      String zk = getZk(config);

      Connection c;
      try {
        c = HbaseConnection.create(zk, getHBaseZnode(config));
      } catch (IOException ex) {
        throw new PipelinesException(ex);
      }

      return new ClusteringServiceFactory(c, config.getClusteringRelationshipConfig()).service;
    };
  }

  private static String getZk(PipelinesConfig config) {
    return Optional.ofNullable(config.getKeygen())
        .map(KeygenConfig::getZkConnectionString)
        .filter(x -> !x.isEmpty())
        .orElse(config.getZkConnectionString());
  }

  private static String getHBaseZnode(PipelinesConfig config) {
    return Optional.ofNullable(config.getKeygen())
        .map(KeygenConfig::getHbaseZnode)
        .filter(x -> !x.isEmpty())
        .orElse("/hbase");
  }
}
