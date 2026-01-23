package org.gbif.pipelines.interpretation.transform.utils;

import org.apache.hadoop.hbase.client.Connection;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.common.HbaseConnectionFactory;

public class KeygenServiceFactory {

  public static HBaseLockingKeyService create(PipelinesConfig config, String datasetId) {

    String zk = config.getKeygen().getZkConnectionString();
    String znode = config.getKeygen().getHbaseZnode();

    Connection connection = HbaseConnectionFactory.getInstance(zk, znode).getConnection();

    org.gbif.pipelines.keygen.config.KeygenConfig keygenConfig =
        org.gbif.pipelines.keygen.config.KeygenConfig.builder()
            .counterTable(config.getKeygen().getCounterTable())
            .lookupTable(config.getKeygen().getLookupTable())
            .occurrenceTable(config.getKeygen().getOccurrenceTable())
            .create();

    return new HBaseLockingKeyService(keygenConfig, connection, datasetId);
  }
}
