package org.gbif.pipelines.keygen.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.google.common.base.Strings;
import lombok.SneakyThrows;

public class HbaseConnectionFactory {

  private final Connection connection;
  private static volatile HbaseConnectionFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private HbaseConnectionFactory(String hbaseZk) {
    if (Strings.isNullOrEmpty(hbaseZk)) {
      connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
    } else {
      Configuration hbaseConfig = HBaseConfiguration.create();
      hbaseConfig.set("hbase.zookeeper.quorum", hbaseZk);
      connection = ConnectionFactory.createConnection(hbaseConfig);
    }
  }

  public static HbaseConnectionFactory getInstance(String hbaseZk) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new HbaseConnectionFactory(hbaseZk);
        }
      }
    }
    return instance;
  }

  public static HbaseConnectionFactory getInstance() {
    return getInstance(null);
  }

  public Connection getConnection() {
    return connection;
  }

}
