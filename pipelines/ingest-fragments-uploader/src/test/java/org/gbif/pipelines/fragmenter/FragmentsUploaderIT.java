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
public class FragmentsUploaderIT {

  /** {@link ClassRule} requires this field to be public. */
  @ClassRule
  public static final HbaseServer HBASE_SERVER = new HbaseServer();

  private final Path regularDwca = Paths.get(getClass().getResource("/dwca/regular").getFile());
  private final Path regularZipDwca = Paths.get(getClass().getResource("/dwca/dwca.dwca").getFile());
  private final Path occurrenceAsExtensionDwca = Paths.get(getClass().getResource("/dwca/occext").getFile());
  private final Path multimediaExtensionDwca = Paths.get(getClass().getResource("/dwca/multimedia").getFile());
  private final Path xmlArchivePath = Paths.get(getClass().getResource("/xml").getFile());

  @Before
  public void before() throws IOException {
    HBASE_SERVER.truncateTable();
  }

  @Test
  public void dwcaDwcaZipSyncUploadTest() throws IOException {

    // State
    int expSize = 210;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    String protocol = "DWCA";

    // When
    long result = FragmentsUploader.dwcaBuilder()
        .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(regularZipDwca)
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
    TableAssert.assertTable(HBASE_SERVER.getConnection(), expSize, datasetId, attempt, protocol);
  }

  @Test
  public void dwcaSyncUploadTest() throws IOException {
    // State
    int expSize = 210;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    String protocol = "DWCA";

    // When
    long result = FragmentsUploader.dwcaBuilder()
        .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(regularDwca)
        .useTriplet(false)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attempt)
        .protocol(protocol)
        .hbaseConnection(HBASE_SERVER.getConnection())
        .build()
        .upload();

    // Should
    Assert.assertEquals(expSize, result);
    TableAssert.assertTable(HBASE_SERVER.getConnection(), expSize, datasetId, attempt, protocol);
  }

  @Test
  public void dwcaAsyncUploadTest() throws IOException {
    // State
    int expSize = 210;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    String protocol = "DWCA";

    // When
    long result = FragmentsUploader.dwcaBuilder()
        .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(regularDwca)
        .useTriplet(false)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attempt)
        .protocol(protocol)
        .executor(Executors.newFixedThreadPool(2))
        .hbaseConnection(HBASE_SERVER.getConnection())
        .useSyncMode(false)
        .build()
        .upload();

    // Should
    Assert.assertEquals(expSize, result);
    TableAssert.assertTable(HBASE_SERVER.getConnection(), expSize, datasetId, attempt, protocol);
  }

  @Test
  public void dwcaSyncUpdateUploadTest() throws IOException {
    // State
    int expSize = 210;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attemptFirst = 231;
    int attemptSecond = 232;
    String protocol = "DWCA";

    // When
    long resultFirst = FragmentsUploader.dwcaBuilder()
        .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(regularDwca)
        .useTriplet(false)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attemptFirst)
        .protocol("XML")
        .hbaseConnection(HBASE_SERVER.getConnection())
        .executor(Executors.newFixedThreadPool(2))
        .build()
        .upload();

    long resultSecond = FragmentsUploader.dwcaBuilder()
        .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(regularDwca)
        .useTriplet(false)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attemptSecond)
        .protocol(protocol)
        .hbaseConnection(HBASE_SERVER.getConnection())
        .build()
        .upload();

    // Should
    Assert.assertEquals(expSize, resultFirst);
    Assert.assertEquals(expSize, resultSecond);
    TableAssert.assertTablDateUpdated(HBASE_SERVER.getConnection(), expSize, datasetId, attemptSecond, protocol);
  }

  @Test
  public void dwcaAsyncUpdateUploadTest() throws IOException {
    // State
    int expSize = 210;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attemptFirst = 231;
    int attemptSecond = 232;
    String protocol = "DWCA";

    // When
    long resultFirst = FragmentsUploader.dwcaBuilder()
        .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(regularDwca)
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

    long resultSecond = FragmentsUploader.dwcaBuilder()
        .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(regularDwca)
        .useTriplet(true)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attemptSecond)
        .protocol(protocol)
        .batchSize(2)
        .hbaseConnection(HBASE_SERVER.getConnection())
        .useSyncMode(false)
        .build()
        .upload();

    // Should
    Assert.assertEquals(expSize, resultFirst);
    Assert.assertEquals(expSize, resultSecond);
    TableAssert.assertTablDateUpdated(HBASE_SERVER.getConnection(), expSize, datasetId, attemptSecond, protocol);
  }

  @Test
  public void dwcaOccExtSyncUploadTest() throws IOException {
    // State
    int expSize = 477;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    String protocol = "DWCA";

    // When
    long result = FragmentsUploader.dwcaBuilder()
        .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(occurrenceAsExtensionDwca)
        .useTriplet(false)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attempt)
        .protocol(protocol)
        .hbaseConnection(HBASE_SERVER.getConnection())
        .build()
        .upload();

    // Should
    Assert.assertEquals(expSize, result);
    TableAssert.assertTable(HBASE_SERVER.getConnection(), expSize, datasetId, attempt, protocol);
  }

  @Test
  public void dwcaOccExtAsyncUploadTest() throws IOException {
    // State
    int expSize = 477;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    String protocol = "DWCA";

    // When
    long result = FragmentsUploader.dwcaBuilder()
        .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(occurrenceAsExtensionDwca)
        .useTriplet(true)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attempt)
        .protocol(protocol)
        .executor(Executors.newFixedThreadPool(2))
        .hbaseConnection(HBASE_SERVER.getConnection())
        .useSyncMode(false)
        .build()
        .upload();

    // Should
    Assert.assertEquals(expSize, result);
    TableAssert.assertTable(HBASE_SERVER.getConnection(), expSize, datasetId, attempt, protocol);
  }

  @Test
  public void dwcaMultimediaSyncUploadTest() throws IOException {
    // State
    int expSize = 368;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    String protocol = "DWCA";

    // When
    long result = FragmentsUploader.dwcaBuilder()
        .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(multimediaExtensionDwca)
        .useTriplet(false)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attempt)
        .protocol(protocol)
        .hbaseConnection(HBASE_SERVER.getConnection())
        .build()
        .upload();

    // Should
    Assert.assertEquals(expSize, result);
    TableAssert.assertTable(HBASE_SERVER.getConnection(), expSize, datasetId, attempt, protocol);
  }

  @Test
  public void dwcaMultimediaAsyncUploadTest() throws IOException {
    // State
    int expSize = 368;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    String protocol = "DWCA";

    // When
    long result = FragmentsUploader.dwcaBuilder()
        .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(multimediaExtensionDwca)
        .useTriplet(false)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attempt)
        .protocol(protocol)
        .executor(Executors.newFixedThreadPool(2))
        .hbaseConnection(HBASE_SERVER.getConnection())
        .useSyncMode(false)
        .build()
        .upload();

    // Should
    Assert.assertEquals(expSize, result);
    TableAssert.assertTable(HBASE_SERVER.getConnection(), expSize, datasetId, attempt, protocol);
  }


  @Test
  public void xmlSyncUploadTest() throws IOException {
    // State
    int expSize = 40;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 1;
    String protocol = "BIOCASE";

    // When
    long result = FragmentsUploader.xmlBuilder()
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
    TableAssert.assertTable(HBASE_SERVER.getConnection(), expSize, datasetId, attempt, protocol);
  }

  @Test
  public void xmlAsyncUploadTest() throws IOException {
    // State
    int expSize = 40;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 1;
    String protocol = "BIOCASE";

    // When
    long result = FragmentsUploader.xmlBuilder()
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
    TableAssert.assertTable(HBASE_SERVER.getConnection(), expSize, datasetId, attempt, protocol);
  }

  @Test
  public void xmlSyncDoubeUploadTest() throws IOException {
    // State
    int expSize = 40;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attemptFirst = 231;
    int attemptSecond = 232;
    String protocol = "BIOCASE";

    // When
    long resultFirst = FragmentsUploader.xmlBuilder()
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

    long resultSecond = FragmentsUploader.xmlBuilder()
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
    TableAssert.assertTablDateUpdated(HBASE_SERVER.getConnection(), expSize, datasetId, attemptSecond, protocol);
  }

  @Test
  public void xmlAsyncDoubeUploadTest() throws IOException {
    // State
    int expSize = 40;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attemptFirst = 231;
    int attemptSecond = 232;
    String protocol = "BIOCASE";

    // When
    long resultFirst = FragmentsUploader.xmlBuilder()
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

    long resultSecond = FragmentsUploader.xmlBuilder()
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
    TableAssert.assertTablDateUpdated(HBASE_SERVER.getConnection(), expSize, datasetId, attemptSecond, protocol);
  }
}
