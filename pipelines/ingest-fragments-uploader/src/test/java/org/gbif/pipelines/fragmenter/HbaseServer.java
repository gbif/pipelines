package org.gbif.pipelines.fragmenter;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.rules.ExternalResource;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HbaseServer extends ExternalResource {

  private static final byte[] FRAGMENT_TABLE = Bytes.toBytes("fragment_table");
  private static final byte[] FF = Bytes.toBytes("0");

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private Connection connection = null;

  @Before
  public void beforeClass() throws IOException {
    log.info("Trancate the table");
    TEST_UTIL.truncateTable(FRAGMENT_TABLE);
  }

  @Override
  protected void before() throws Exception {
    log.info("Create hbase mini-cluster");
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(FRAGMENT_TABLE, FF);
    connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
  }

  @SneakyThrows
  @Override
  protected void after() {
    log.info("Shut down hbase mini-cluster");
    TEST_UTIL.shutdownMiniCluster();
    if (connection != null) {
      connection.close();
    }
  }

  public Connection getConnection() {
    return connection;
  }
}
