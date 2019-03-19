package org.gbif.pipeleins.common;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HbaseConnectionFactory {

  public static Connection create(List<Configuration> hbaseConfigs) throws IOException {
    if (hbaseConfigs != null && !hbaseConfigs.isEmpty()) {
      for (Configuration cfg : hbaseConfigs) {
        try {
          return ConnectionFactory.createConnection(cfg);
        } catch (IOException ex) {
          log.warn("HBase connection exception!", ex);
        }
      }
      throw new IOException("Can't create a connection to HBase");
    }
    return ConnectionFactory.createConnection(HBaseConfiguration.create());
  }

  public static Connection create() throws IOException {
    return create(null);
  }

}
