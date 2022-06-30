package org.gbif.pipelines.diagnostics.common;

import lombok.Builder;
import org.apache.hadoop.hbase.client.Connection;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.common.HbaseConnectionFactory;
import org.gbif.pipelines.keygen.config.KeygenConfig;

@Builder
public class KeygenServiceFactory {
  private final String zkConnection;
  private final String lookupTable;
  private final String counterTable;
  private final String occurrenceTable;
  private final String datasetKey;
  private Connection connection;

  public HBaseLockingKeyService create() {
    KeygenConfig cfg =
        KeygenConfig.builder()
            .zkConnectionString(zkConnection)
            .lookupTable(lookupTable)
            .counterTable(counterTable)
            .occurrenceTable(occurrenceTable)
            .create();

    if (connection == null) {
      connection = HbaseConnectionFactory.getInstance(zkConnection).getConnection();
    }

    return new HBaseLockingKeyService(cfg, connection, datasetKey);
  }
}
