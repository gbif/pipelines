package org.gbif.pipelines.keygen;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.pipelines.keygen.config.KeygenConfig;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

abstract class HBaseIT {

  static final String A = "a";
  static final String B = "b";
  static final String C = "c";

  static final KeygenConfig CFG =
      KeygenConfig.builder()
          .zkConnectionString(null)
          .occurrenceTable("test_occurrence")
          .lookupTable("test_occurrence_lookup")
          .counterTable("test_occurrence_counter")
          .create();

  static final byte[] COUNTER_TABLE = Bytes.toBytes(CFG.getCounterTable());
  static final byte[] COUNTER_CF = Bytes.toBytes("o");
  static final byte[] OCCURRENCE_TABLE = Bytes.toBytes(CFG.getOccurrenceTable());
  static final byte[] LOOKUP_TABLE = Bytes.toBytes(CFG.getLookupTable());
  static final byte[] CF = Bytes.toBytes("o");

  static Connection connection = null;
  static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  HBaseLockingKeyService keyService;

  @Rule public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.port", HBaseTestingUtility.randomFreePort());
    TEST_UTIL
        .getConfiguration()
        .setInt("hbase.master.info.port", HBaseTestingUtility.randomFreePort());
    TEST_UTIL
        .getConfiguration()
        .setInt("hbase.regionserver.port", HBaseTestingUtility.randomFreePort());
    TEST_UTIL
        .getConfiguration()
        .setInt("hbase.regionserver.info.port", HBaseTestingUtility.randomFreePort());
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(LOOKUP_TABLE, CF);
    TEST_UTIL.createTable(COUNTER_TABLE, COUNTER_CF);
    TEST_UTIL.createTable(OCCURRENCE_TABLE, CF);
    connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
  }

  @Before
  public void before() throws IOException {
    TEST_UTIL.truncateTable(LOOKUP_TABLE);
    TEST_UTIL.truncateTable(COUNTER_TABLE);
    TEST_UTIL.truncateTable(OCCURRENCE_TABLE);

    keyService = new HBaseLockingKeyService(CFG, connection, UUID.randomUUID().toString());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    if (connection != null) {
      connection.close();
    }
  }
}
