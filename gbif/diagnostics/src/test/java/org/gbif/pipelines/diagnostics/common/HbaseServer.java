package org.gbif.pipelines.diagnostics.common;

import java.io.IOException;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.pipelines.diagnostics.RepairGbifIDLookupToolIT;
import org.gbif.pipelines.diagnostics.strategy.BothStrategyIT;
import org.gbif.pipelines.diagnostics.strategy.MaxStrategyIT;
import org.gbif.pipelines.diagnostics.strategy.MinStrategyIT;
import org.gbif.pipelines.diagnostics.strategy.OccurrenceIdStrategyIT;
import org.gbif.pipelines.diagnostics.strategy.TripletStrategyIT;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.config.KeygenConfig;
import org.gbif.pipelines.keygen.hbase.Columns;
import org.gbif.pipelines.keygen.hbase.HBaseStore;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@Getter
@Slf4j
@RunWith(Suite.class)
@SuiteClasses({
  BothStrategyIT.class,
  MaxStrategyIT.class,
  MinStrategyIT.class,
  OccurrenceIdStrategyIT.class,
  TripletStrategyIT.class,
  RepairGbifIDLookupToolIT.class
})
public class HbaseServer {

  public static final KeygenConfig CFG =
      KeygenConfig.builder()
          .counterTable("test_occurrence_counter")
          .lookupTable("test_occurrence_lookup")
          .occurrenceTable("test_occurrence")
          .zkConnectionString(null)
          .create();

  private static final byte[] LOOKUP_TABLE = Bytes.toBytes(CFG.getLookupTable());
  private static final String CF_NAME = "o";
  private static final byte[] CF = Bytes.toBytes(CF_NAME);
  private static final byte[] COUNTER_TABLE = Bytes.toBytes(CFG.getCounterTable());
  private static final String COUNTER_CF_NAME = "o";
  private static final byte[] COUNTER_CF = Bytes.toBytes(COUNTER_CF_NAME);
  private static final byte[] OCCURRENCE_TABLE = Bytes.toBytes(CFG.getOccurrenceTable());

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  public static HBaseStore<String> lookupTableStore = null;
  public static Connection connection = null;

  @ClassRule
  public static ExternalResource getResource() {
    return new ExternalResource() {
      @Override
      protected void before() throws Throwable {
        log.info("Create hbase mini-cluster");
        TEST_UTIL.startMiniCluster(1);
        TEST_UTIL.createTable(LOOKUP_TABLE, CF);
        TEST_UTIL.createTable(COUNTER_TABLE, COUNTER_CF);
        TEST_UTIL.createTable(OCCURRENCE_TABLE, CF);
        connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());

        lookupTableStore =
            new HBaseStore<>(
                CFG.getLookupTable(),
                Columns.OCCURRENCE_COLUMN_FAMILY,
                connection,
                HBaseLockingKeyService.NUMBER_OF_BUCKETS);
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
    };
  }

  public static void truncateTable() throws IOException {
    log.info("Trancate the table");
    TEST_UTIL.truncateTable(LOOKUP_TABLE);
  }
}
