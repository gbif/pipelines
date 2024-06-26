package org.gbif.pipelines.keygen.common;

import java.util.function.Predicate;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.client.Connection;

public class HbaseConnectionFactory {

  private final Connection connection;
  private static volatile HbaseConnectionFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private HbaseConnectionFactory(String hbaseZk, String hbaseZnode) {
    connection = HbaseConnection.create(hbaseZk, hbaseZnode);
  }

  public static HbaseConnectionFactory getInstance(String hbaseZk, String hbaseZnode) {
    Predicate<HbaseConnectionFactory> pr =
        i -> i == null || i.getConnection() == null || i.getConnection().isClosed();
    if (pr.test(instance)) {
      synchronized (MUTEX) {
        if (pr.test(instance)) {
          instance = new HbaseConnectionFactory(hbaseZk, hbaseZnode);
        }
      }
    }
    return instance;
  }

  public static HbaseConnectionFactory getInstance() {
    return getInstance(null, null);
  }

  public Connection getConnection() {
    return connection;
  }
}
