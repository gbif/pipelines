package org.gbif.pipelines.core.parsers.clustering;

import java.io.Serializable;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.pipelines.core.config.model.ClusteringRelationshipConfig;

public class ClusteringService implements Serializable {

  private final Connection connection;
  private final ClusteringRelationshipConfig config;

  @SneakyThrows
  private ClusteringService(Connection connection, ClusteringRelationshipConfig config) {
    this.connection = connection;
    this.config = config;
  }

  public static ClusteringService create(
      Connection connection, ClusteringRelationshipConfig config) {
    return new ClusteringService(connection, config);
  }

  @SneakyThrows
  public boolean isClustered(Long gbifId) {
    try (Table table = connection.getTable(TableName.valueOf(config.getRelationshipTableName()))) {
      Scan scan = new Scan();
      scan.addFamily(Bytes.toBytes("o"));
      int salt = Math.abs(gbifId.toString().hashCode()) % config.getRelationshipTableSalt();
      scan.setRowPrefixFilter(Bytes.toBytes(salt + ":" + gbifId));
      ResultScanner s = table.getScanner(scan);
      Result row = s.next();
      return row != null;
    }
  }
}
