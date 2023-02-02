package org.gbif.pipelines.core.parsers.clustering;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.pipelines.core.config.model.ClusteringRelationshipConfig;

@AllArgsConstructor(staticName = "create")
public class ClusteringService implements Serializable {

  private final Connection connection;
  private final ClusteringRelationshipConfig config;

  @SneakyThrows
  public boolean isClustered(Long gbifId) {
    try (Table table = connection.getTable(TableName.valueOf(config.getRelationshipTableName()))) {
      Scan scan = new Scan();
      scan.setBatch(1);
      scan.addFamily(Bytes.toBytes("o"));
      int salt = Math.abs(gbifId.toString().hashCode()) % config.getRelationshipTableSalt();
      scan.setRowPrefixFilter(Bytes.toBytes(salt + ":" + gbifId + ":"));
      ResultScanner s = table.getScanner(scan);
      Result row = s.next();
      return row != null;
    }
  }
}
