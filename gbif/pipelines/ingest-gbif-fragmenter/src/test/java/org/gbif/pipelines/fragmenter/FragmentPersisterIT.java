package org.gbif.pipelines.fragmenter;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.pipelines.fragmenter.common.HbaseServer;
import org.gbif.pipelines.fragmenter.common.TableAssert;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.JVM)
public class FragmentPersisterIT {

  /** {@link ClassRule} requires this field to be public. */
  @ClassRule public static final HbaseServer HBASE_SERVER = new HbaseServer();

  private final Path regularDwca = Paths.get(getClass().getResource("/dwca/regular").getFile());
  private final Path regularZipDwca =
      Paths.get(getClass().getResource("/dwca/dwca.dwca").getFile());
  private final Path occurrenceAsExtensionDwca =
      Paths.get(getClass().getResource("/dwca/occext").getFile());
  private final Path multimediaExtensionDwca =
      Paths.get(getClass().getResource("/dwca/multimedia").getFile());
  private final Path xmlArchivePath = Paths.get(getClass().getResource("/xml").getFile());

  @Before
  public void before() throws IOException {
    HBASE_SERVER.truncateTable();
  }

  @Test
  public void dwcaDwcaZipSyncUploadTest() throws IOException {

    // State
    int expSize = 210;
    String datasetKey = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    // When
    long result =
        FragmentPersister.dwcaBuilder()
            .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
            .keygenConfig(HbaseServer.CFG)
            .pathToArchive(regularZipDwca)
            .useTriplet(true)
            .useOccurrenceId(true)
            .datasetKey(datasetKey)
            .attempt(attempt)
            .endpointType(endpointType)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .build()
            .persist();

    // Should
    Assert.assertEquals(expSize, result);
    TableAssert.assertTable(
        HBASE_SERVER.getConnection(), expSize, datasetKey, attempt, endpointType);
  }

  @Test
  public void dwcaSyncUploadTest() throws IOException {
    // State
    int expSize = 210;
    String datasetKey = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    // When
    long result =
        FragmentPersister.dwcaBuilder()
            .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
            .keygenConfig(HbaseServer.CFG)
            .pathToArchive(regularDwca)
            .useTriplet(false)
            .useOccurrenceId(true)
            .datasetKey(datasetKey)
            .attempt(attempt)
            .endpointType(endpointType)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .build()
            .persist();

    // Should
    Assert.assertEquals(expSize, result);
    TableAssert.assertTable(
        HBASE_SERVER.getConnection(), expSize, datasetKey, attempt, endpointType);
  }

  @Test
  public void dwcaAsyncUploadTest() throws IOException {
    // State
    int expSize = 210;
    String datasetKey = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    // When
    long result =
        FragmentPersister.dwcaBuilder()
            .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
            .keygenConfig(HbaseServer.CFG)
            .pathToArchive(regularDwca)
            .useTriplet(false)
            .useOccurrenceId(true)
            .datasetKey(datasetKey)
            .attempt(attempt)
            .endpointType(endpointType)
            .executor(Executors.newSingleThreadExecutor())
            .hbaseConnection(HBASE_SERVER.getConnection())
            .useSyncMode(false)
            .build()
            .persist();

    // Should
    Assert.assertEquals(expSize, result);
    TableAssert.assertTable(
        HBASE_SERVER.getConnection(), expSize, datasetKey, attempt, endpointType);
  }

  @Test
  public void dwcaSyncUpdateUploadTest() throws IOException {
    // State
    int expSize = 210;
    String datasetKey = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attemptFirst = 231;
    int attemptSecond = 232;
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    // When
    long resultFirst =
        FragmentPersister.dwcaBuilder()
            .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
            .keygenConfig(HbaseServer.CFG)
            .pathToArchive(regularDwca)
            .useTriplet(false)
            .useOccurrenceId(true)
            .datasetKey(datasetKey)
            .attempt(attemptFirst)
            .endpointType(EndpointType.BIOCASE_XML_ARCHIVE)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .executor(Executors.newSingleThreadExecutor())
            .build()
            .persist();

    long resultSecond =
        FragmentPersister.dwcaBuilder()
            .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
            .keygenConfig(HbaseServer.CFG)
            .pathToArchive(regularDwca)
            .useTriplet(false)
            .useOccurrenceId(true)
            .datasetKey(datasetKey)
            .attempt(attemptSecond)
            .endpointType(endpointType)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .build()
            .persist();

    // Should
    Assert.assertEquals(expSize, resultFirst);
    Assert.assertEquals(expSize, resultSecond);
    TableAssert.assertTableDateUpdated(
        HBASE_SERVER.getConnection(), expSize, datasetKey, attemptSecond, endpointType);
  }

  @Test
  public void dwcaAsyncUpdateUploadTest() throws IOException {
    // State
    int expSize = 210;
    String datasetKey = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attemptFirst = 231;
    int attemptSecond = 232;
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    // When
    long resultFirst =
        FragmentPersister.dwcaBuilder()
            .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
            .keygenConfig(HbaseServer.CFG)
            .pathToArchive(regularDwca)
            .useTriplet(false)
            .useOccurrenceId(true)
            .datasetKey(datasetKey)
            .attempt(attemptFirst)
            .endpointType(EndpointType.BIOCASE_XML_ARCHIVE)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .executor(Executors.newSingleThreadExecutor())
            .useSyncMode(false)
            .build()
            .persist();

    long resultSecond =
        FragmentPersister.dwcaBuilder()
            .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
            .keygenConfig(HbaseServer.CFG)
            .pathToArchive(regularDwca)
            .useTriplet(false)
            .useOccurrenceId(true)
            .datasetKey(datasetKey)
            .attempt(attemptSecond)
            .endpointType(endpointType)
            .batchSize(2)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .useSyncMode(false)
            .build()
            .persist();

    // Should
    Assert.assertEquals(expSize, resultFirst);
    Assert.assertEquals(expSize, resultSecond);
    TableAssert.assertTableDateUpdated(
        HBASE_SERVER.getConnection(), expSize, datasetKey, attemptSecond, endpointType);
  }

  @Test
  public void dwcaOccExtSyncUploadTest() throws IOException {
    // State
    int expSize = 477;
    String datasetKey = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    // When
    long result =
        FragmentPersister.dwcaBuilder()
            .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
            .keygenConfig(HbaseServer.CFG)
            .pathToArchive(occurrenceAsExtensionDwca)
            .useTriplet(false)
            .useOccurrenceId(true)
            .datasetKey(datasetKey)
            .attempt(attempt)
            .endpointType(endpointType)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .build()
            .persist();

    // Should
    Assert.assertEquals(expSize, result);
    TableAssert.assertTable(
        HBASE_SERVER.getConnection(), expSize, datasetKey, attempt, endpointType);
  }

  @Test
  public void dwcaOccExtAsyncUploadTest() throws IOException {
    // State
    int expSize = 477;
    String datasetKey = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    // When
    long result =
        FragmentPersister.dwcaBuilder()
            .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
            .keygenConfig(HbaseServer.CFG)
            .pathToArchive(occurrenceAsExtensionDwca)
            .useTriplet(true)
            .useOccurrenceId(true)
            .datasetKey(datasetKey)
            .attempt(attempt)
            .endpointType(endpointType)
            .executor(Executors.newSingleThreadExecutor())
            .hbaseConnection(HBASE_SERVER.getConnection())
            .useSyncMode(false)
            .build()
            .persist();

    // Should
    Assert.assertEquals(expSize, result);
    TableAssert.assertTable(
        HBASE_SERVER.getConnection(), expSize, datasetKey, attempt, endpointType);
  }

  @Test
  public void dwcaMultimediaSyncUploadTest() throws IOException {
    // State
    int expSize = 368;
    String datasetKey = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    // When
    long result =
        FragmentPersister.dwcaBuilder()
            .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
            .keygenConfig(HbaseServer.CFG)
            .pathToArchive(multimediaExtensionDwca)
            .useTriplet(false)
            .useOccurrenceId(true)
            .datasetKey(datasetKey)
            .attempt(attempt)
            .endpointType(endpointType)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .build()
            .persist();

    // Should
    Assert.assertEquals(expSize, result);
    TableAssert.assertTable(
        HBASE_SERVER.getConnection(), expSize, datasetKey, attempt, endpointType);
  }

  @Test
  public void dwcaMultimediaAsyncUploadTest() throws IOException {
    // State
    int expSize = 368;
    String datasetKey = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    // When
    long result =
        FragmentPersister.dwcaBuilder()
            .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
            .keygenConfig(HbaseServer.CFG)
            .pathToArchive(multimediaExtensionDwca)
            .useTriplet(false)
            .useOccurrenceId(true)
            .datasetKey(datasetKey)
            .attempt(attempt)
            .endpointType(endpointType)
            .executor(Executors.newSingleThreadExecutor())
            .hbaseConnection(HBASE_SERVER.getConnection())
            .useSyncMode(false)
            .backPressure(5)
            .build()
            .persist();

    // Should
    Assert.assertEquals(expSize, result);
    TableAssert.assertTable(
        HBASE_SERVER.getConnection(), expSize, datasetKey, attempt, endpointType);
  }

  @Test
  public void xmlSyncUploadTest() throws IOException {
    // State
    int expSize = 40;
    String datasetKey = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 1;
    EndpointType endpointType = EndpointType.BIOCASE_XML_ARCHIVE;

    // When
    long result =
        FragmentPersister.xmlBuilder()
            .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
            .keygenConfig(HbaseServer.CFG)
            .pathToArchive(xmlArchivePath)
            .useTriplet(true)
            .useOccurrenceId(true)
            .datasetKey(datasetKey)
            .attempt(attempt)
            .endpointType(endpointType)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .build()
            .persist();

    // Should
    Assert.assertEquals(expSize, result);
    TableAssert.assertTable(
        HBASE_SERVER.getConnection(), expSize, datasetKey, attempt, endpointType);
  }

  @Test
  public void xmlAsyncUploadTest() throws IOException {
    // State
    int expSize = 40;
    String datasetKey = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 1;
    EndpointType endpointType = EndpointType.BIOCASE_XML_ARCHIVE;

    // When
    long result =
        FragmentPersister.xmlBuilder()
            .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
            .keygenConfig(HbaseServer.CFG)
            .pathToArchive(xmlArchivePath)
            .useTriplet(true)
            .useOccurrenceId(true)
            .datasetKey(datasetKey)
            .attempt(attempt)
            .endpointType(endpointType)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .useSyncMode(false)
            .build()
            .persist();

    // Should
    Assert.assertEquals(expSize, result);
    TableAssert.assertTable(
        HBASE_SERVER.getConnection(), expSize, datasetKey, attempt, endpointType);
  }

  @Test
  public void xmlSyncDoubeUploadTest() throws IOException {
    // State
    int expSize = 40;
    String datasetKey = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attemptFirst = 231;
    int attemptSecond = 232;
    EndpointType endpointType = EndpointType.BIOCASE_XML_ARCHIVE;

    // When
    long resultFirst =
        FragmentPersister.xmlBuilder()
            .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
            .keygenConfig(HbaseServer.CFG)
            .pathToArchive(xmlArchivePath)
            .useTriplet(true)
            .useOccurrenceId(true)
            .datasetKey(datasetKey)
            .attempt(attemptFirst)
            .endpointType(EndpointType.DWC_ARCHIVE)
            .backPressure(5)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .build()
            .persist();

    long resultSecond =
        FragmentPersister.xmlBuilder()
            .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
            .keygenConfig(HbaseServer.CFG)
            .pathToArchive(xmlArchivePath)
            .useTriplet(true)
            .useOccurrenceId(true)
            .datasetKey(datasetKey)
            .attempt(attemptSecond)
            .endpointType(endpointType)
            .batchSize(1)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .build()
            .persist();

    // Should
    Assert.assertEquals(expSize, resultFirst);
    Assert.assertEquals(expSize, resultSecond);
    TableAssert.assertTableDateUpdated(
        HBASE_SERVER.getConnection(), expSize, datasetKey, attemptSecond, endpointType);
  }

  @Test
  public void xmlAsyncDoubeUploadTest() throws IOException {
    // State
    int expSize = 40;
    String datasetKey = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attemptFirst = 231;
    int attemptSecond = 232;
    EndpointType endpointType = EndpointType.BIOCASE_XML_ARCHIVE;

    // When
    long resultFirst =
        FragmentPersister.xmlBuilder()
            .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
            .keygenConfig(HbaseServer.CFG)
            .pathToArchive(xmlArchivePath)
            .useTriplet(true)
            .useOccurrenceId(true)
            .datasetKey(datasetKey)
            .attempt(attemptFirst)
            .endpointType(EndpointType.DWC_ARCHIVE)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .executor(Executors.newSingleThreadExecutor())
            .useSyncMode(false)
            .build()
            .persist();

    long resultSecond =
        FragmentPersister.xmlBuilder()
            .tableName(HbaseServer.FRAGMENT_TABLE_NAME)
            .keygenConfig(HbaseServer.CFG)
            .pathToArchive(xmlArchivePath)
            .useTriplet(true)
            .useOccurrenceId(true)
            .datasetKey(datasetKey)
            .attempt(attemptSecond)
            .endpointType(endpointType)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .backPressure(1)
            .batchSize(1)
            .useSyncMode(false)
            .build()
            .persist();

    // Should
    Assert.assertEquals(expSize, resultFirst);
    Assert.assertEquals(expSize, resultSecond);
    TableAssert.assertTableDateUpdated(
        HBASE_SERVER.getConnection(), expSize, datasetKey, attemptSecond, endpointType);
  }
}
