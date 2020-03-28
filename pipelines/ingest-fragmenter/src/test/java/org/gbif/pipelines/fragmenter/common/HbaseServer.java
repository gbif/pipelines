package org.gbif.pipelines.fragmenter.common;

import java.io.IOException;

import org.gbif.pipelines.keygen.config.KeygenConfig;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.rules.ExternalResource;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class HbaseServer extends ExternalResource {

  public static final KeygenConfig CFG =
      KeygenConfig.create("test_occurrence", "test_occurrence_counter", "test_occurrence_lookup", null);

  public static final String FRAGMENT_TABLE_NAME = "fragment_table";
  public static final byte[] FRAGMENT_TABLE = Bytes.toBytes(FRAGMENT_TABLE_NAME);

  private static final byte[] LOOKUP_TABLE = Bytes.toBytes(CFG.getLookupTable());
  private static final String CF_NAME = "o";
  private static final byte[] CF = Bytes.toBytes(CF_NAME);
  private static final byte[] COUNTER_TABLE = Bytes.toBytes(CFG.getCounterTable());
  private static final String COUNTER_CF_NAME = "o";
  private static final byte[] COUNTER_CF = Bytes.toBytes(COUNTER_CF_NAME);
  private static final byte[] OCCURRENCE_TABLE = Bytes.toBytes(CFG.getOccTable());

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private Connection connection = null;

  public void truncateTable() throws IOException {
    log.info("Trancate the table");
    TEST_UTIL.truncateTable(FRAGMENT_TABLE);
  }

  @Override
  protected void before() throws Exception {
    log.info("Create hbase mini-cluster");
    TEST_UTIL.startMiniCluster(1);

    TEST_UTIL.createTable(FRAGMENT_TABLE, HbaseStore.getFragmentFamily());
    TEST_UTIL.createTable(LOOKUP_TABLE, CF);
    TEST_UTIL.createTable(COUNTER_TABLE, COUNTER_CF);
    TEST_UTIL.createTable(OCCURRENCE_TABLE, CF);

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

}
