package org.gbif.pipelines.keygen.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HbaseConnectionFactory {

  private static Connection instance;

  @SneakyThrows
  public static synchronized Connection createSingleton(String hbaseZk) {
    if (instance == null || instance.isClosed()) {
      instance = create(hbaseZk);
    }
    return instance;
  }

  public static Connection createSingleton() {
    return createSingleton(null);
  }

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
