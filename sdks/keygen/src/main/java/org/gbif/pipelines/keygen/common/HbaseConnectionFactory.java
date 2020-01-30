package org.gbif.pipelines.keygen.common;

import java.util.function.Predicate;

import org.apache.hadoop.hbase.client.Connection;

import lombok.SneakyThrows;

public class HbaseConnectionFactory {

  private final Connection connection;
  private static volatile HbaseConnectionFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private HbaseConnectionFactory(String hbaseZk) {
    connection = HbaseConnection.create(hbaseZk);
  }

  public static Connection getInstance(String hbaseZk) {
    Predicate<HbaseConnectionFactory> pr = i -> i == null || i.connection == null || i.connection.isClosed();
    if (pr.test(instance)) {
      synchronized (MUTEX) {
        if (pr.test(instance)) {
          instance = new HbaseConnectionFactory(hbaseZk);
        }
      }
    }
    return instance.connection;
  }

  public static Connection getInstance() {
    return getInstance(null);
  }

}
