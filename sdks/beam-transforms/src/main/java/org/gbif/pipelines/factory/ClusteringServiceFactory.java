package org.gbif.pipelines.factory;

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
import org.gbif.pipelines.keygen.common.HbaseConnectionFactory;

public class ClusteringServiceFactory {

  private final ClusteringService service;
  private static volatile ClusteringServiceFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private ClusteringServiceFactory(Connection c, ClusteringRelationshipConfig config) {
    this.service = ClusteringService.create(c, config);
  }

  public static ClusteringService getInstance(Connection c, ClusteringRelationshipConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new ClusteringServiceFactory(c, config);
        }
      }
    }
    return instance.service;
  }

  public static SerializableSupplier<ClusteringService> getInstanceSupplier(
      PipelinesConfig config) {
    return () -> {
      if (config.getClusteringRelationshipConfig() == null
          || config.getClusteringRelationshipConfig().getRelationshipTableName() == null) {
        return null;
      }

      String zk = getZk(config);
      Connection c = HbaseConnectionFactory.getInstance(zk).getConnection();
      return getInstance(c, config.getClusteringRelationshipConfig());
    };
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
        c = HbaseConnection.create(zk);
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
}
