package org.gbif.pipelines.keygen.common;

import com.google.common.base.Strings;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HbaseConnection {

  public static Connection create(String hbaseZk) throws IOException {
    if (Strings.isNullOrEmpty(hbaseZk)) {
      return ConnectionFactory.createConnection(HBaseConfiguration.create());
    }
    Configuration hbaseConfig = HBaseConfiguration.create();
    hbaseConfig.set("hbase.zookeeper.quorum", hbaseZk);
    return ConnectionFactory.createConnection(hbaseConfig);
  }

  public static Connection create() throws IOException {
    return create(null);
  }
}
