package org.gbif.pipelines.fragmenter;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executors;

import org.gbif.pipelines.fragmenter.common.HbaseServer;
import org.gbif.pipelines.fragmenter.common.TableAssert;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.JVM)
public class XmlFragmentsUploaderIT {

  /** {@link ClassRule} requires this field to be public. */
  @ClassRule
  public static final HbaseServer HBASE_SERVER = new HbaseServer();

  private final Path xmlArchivePath = Paths.get(getClass().getResource("/xml").getFile());

  @Before
  public void before() throws IOException {
    HBASE_SERVER.truncateTable();
  }
  
  @Test
  public void syncUploadTest() throws IOException {
    // State
    int expSize = 40;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 1;
    String protocol = "BIOCASE";

    // When
    long result = XmlFragmentsUploader.builder()
        .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(xmlArchivePath)
        .useTriplet(true)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attempt)
        .protocol(protocol)
        .hbaseConnection(HBASE_SERVER.getConnection())
        .build()
        .upload();

    // Should
    Assert.assertEquals(expSize, result);
    TableAssert.assertTableData(HBASE_SERVER.getConnection(), expSize, datasetId, attempt, protocol, false);
  }

  @Test
  public void asyncUploadTest() throws IOException {
    // State
    int expSize = 40;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 1;
    String protocol = "BIOCASE";

    // When
    long result = XmlFragmentsUploader.builder()
        .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(xmlArchivePath)
        .useTriplet(true)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attempt)
        .protocol(protocol)
        .hbaseConnection(HBASE_SERVER.getConnection())
        .useSyncMode(false)
        .build()
        .upload();

    // Should
    Assert.assertEquals(expSize, result);
    TableAssert.assertTableData(HBASE_SERVER.getConnection(), expSize, datasetId, attempt, protocol, false);
  }

  @Test
  public void syncDoubeUploadTest() throws IOException {
    // State
    int expSize = 40;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attemptFirst = 231;
    int attemptSecond = 232;
    String protocol = "BIOCASE";

    // When
    long resultFirst = XmlFragmentsUploader.builder()
        .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(xmlArchivePath)
        .useTriplet(true)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attemptFirst)
        .protocol("XML")
        .hbaseConnection(HBASE_SERVER.getConnection())
        .build()
        .upload();

    long resultSecond = XmlFragmentsUploader.builder()
        .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(xmlArchivePath)
        .useTriplet(true)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attemptSecond)
        .protocol(protocol)
        .batchSize(1)
        .hbaseConnection(HBASE_SERVER.getConnection())
        .build()
        .upload();

    // Should
    Assert.assertEquals(expSize, resultFirst);
    Assert.assertEquals(expSize, resultSecond);
    TableAssert.assertTableData(HBASE_SERVER.getConnection(), expSize, datasetId, attemptSecond, protocol, true);
  }

  @Test
  public void asyncDoubeUploadTest() throws IOException {
    // State
    int expSize = 40;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attemptFirst = 231;
    int attemptSecond = 232;
    String protocol = "BIOCASE";

    // When
    long resultFirst = XmlFragmentsUploader.builder()
        .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(xmlArchivePath)
        .useTriplet(true)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attemptFirst)
        .protocol("XML")
        .hbaseConnection(HBASE_SERVER.getConnection())
        .executor(Executors.newFixedThreadPool(2))
        .useSyncMode(false)
        .build()
        .upload();

    long resultSecond = XmlFragmentsUploader.builder()
        .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(xmlArchivePath)
        .useTriplet(true)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attemptSecond)
        .protocol(protocol)
        .hbaseConnection(HBASE_SERVER.getConnection())
        .backPressure(1)
        .batchSize(1)
        .useSyncMode(false)
        .build()
        .upload();

    // Should
    Assert.assertEquals(expSize, resultFirst);
    Assert.assertEquals(expSize, resultSecond);
    TableAssert.assertTableData(HBASE_SERVER.getConnection(), expSize, datasetId, attemptSecond, protocol, true);
  }


}
