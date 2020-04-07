package org.gbif.pipelines.crawler;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;

import lombok.NoArgsConstructor;

@NoArgsConstructor(staticName = "create")
public class HbaseConnectionStub implements Connection {

  @Override
  public Configuration getConfiguration() {
    return null;
  }

  @Override
  public Table getTable(TableName tableName) throws IOException {
    return null;
  }

  @Override
  public Table getTable(TableName tableName, ExecutorService executorService) throws IOException {
    return null;
  }

  @Override
  public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
    return null;
  }

  @Override
  public BufferedMutator getBufferedMutator(BufferedMutatorParams bufferedMutatorParams) throws IOException {
    return null;
  }

  @Override
  public RegionLocator getRegionLocator(TableName tableName) throws IOException {
    return null;
  }

  @Override
  public Admin getAdmin() throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public void abort(String s, Throwable throwable) {

  }

  @Override
  public boolean isAborted() {
    return false;
  }
}
