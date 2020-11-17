package org.gbif.pipelines.core.parsers.clustering;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.pipelines.core.config.model.ClusteringRelationshipConfig;

public class ClusteringService implements Closeable, Serializable {

  private final Table table;
  private final int relationshipSalt;

  @SneakyThrows
  private ClusteringService(Connection connection, ClusteringRelationshipConfig config) {
    this.table = connection.getTable(TableName.valueOf(config.getRelationshipTableName()));
    this.relationshipSalt = config.getRelationshipTableSalt();
  }

  public static ClusteringService create(
      Connection connection, ClusteringRelationshipConfig config) {
    return new ClusteringService(connection, config);
  }

  @SneakyThrows
  public boolean isClustered(Long gbifId) {
    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes("o"));
    int salt = Math.abs(gbifId.toString().hashCode()) % relationshipSalt;
    scan.setRowPrefixFilter(Bytes.toBytes(salt + ":" + gbifId));
    ResultScanner s = table.getScanner(scan);
    Result row = s.next();
    return row != null;
  }

  @Override
  public void close() throws IOException {
    table.close();
  }
}
