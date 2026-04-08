package org.gbif.pipelines.spark;

import java.io.IOException;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.pipelines.keygen.config.KeygenConfig;
import org.junit.rules.ExternalResource;

@Slf4j
@Getter
public class HbaseServer extends ExternalResource {

  public static final KeygenConfig CFG =
      KeygenConfig.builder()
          .counterTable("test_occurrence_counter")
          .lookupTable("test_occurrence_lookup")
          .occurrenceTable("test_occurrence")
          .zkConnectionString(null)
          .create();

  public static final String FRAGMENT_TABLE_NAME = "fragment_table";
  private static final byte[] FF_BYTES = Bytes.toBytes("fragment");
  public static final TableName FRAGMENT_TABLE = TableName.valueOf(FRAGMENT_TABLE_NAME);

  private static final TableName LOOKUP_TABLE = TableName.valueOf(CFG.getLookupTable());
  private static final String CF_NAME = "o";
  private static final byte[] CF = Bytes.toBytes(CF_NAME);
  private static final TableName COUNTER_TABLE = TableName.valueOf(CFG.getCounterTable());
  private static final String COUNTER_CF_NAME = "o";
  private static final byte[] COUNTER_CF = Bytes.toBytes(COUNTER_CF_NAME);
  private static final TableName OCCURRENCE_TABLE = TableName.valueOf(CFG.getOccurrenceTable());

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private Connection connection = null;
  private String zkQuorum = null;

  public void truncateTable() throws IOException {
    log.info("Truncate the table");
    TEST_UTIL.truncateTable(FRAGMENT_TABLE);
  }

  @Override
  protected void before() throws Exception {
    log.info("Create hbase mini-cluster");
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(FRAGMENT_TABLE, FF_BYTES);
    TEST_UTIL.createTable(LOOKUP_TABLE, CF);
    TEST_UTIL.createTable(COUNTER_TABLE, COUNTER_CF);
    TEST_UTIL.createTable(OCCURRENCE_TABLE, CF);

    connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
    zkQuorum = TEST_UTIL.getZooKeeperWatcher().getQuorum();
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
