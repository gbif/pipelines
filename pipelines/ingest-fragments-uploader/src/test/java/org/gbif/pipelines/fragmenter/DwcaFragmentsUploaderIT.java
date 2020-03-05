package org.gbif.pipelines.fragmenter;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executors;

import org.gbif.pipelines.fragmenter.common.FragmentsConfig;
import org.gbif.pipelines.fragmenter.common.HbaseServer;
import org.gbif.pipelines.fragmenter.common.TableAssert;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.JVM)
public class DwcaFragmentsUploaderIT {

  /** {@link ClassRule} requires this field to be public. */
  @ClassRule
  public static final HbaseServer HBASE_SERVER = new HbaseServer();

  private final Path regularDwca = Paths.get(getClass().getResource("/dwca/regular").getFile());
  private final Path occurrenceAsExtensionDwca = Paths.get(getClass().getResource("/dwca/occext").getFile());
  private final Path multimediaExtensionDwca = Paths.get(getClass().getResource("/dwca/multimedia").getFile());

  @Before
  public void before() throws IOException {
    HBASE_SERVER.truncateTable();
  }

  @Test
  public void syncUploadTest() throws IOException {
    // State
    int expSize = 210;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    String protocol = "DWCA";

    // When
    long result = DwcaFragmentsUploader.builder()
        .config(FragmentsConfig.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(regularDwca)
        .tempDir(regularDwca)
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
    TableAssert.assertTableData(HBASE_SERVER.getConnection(), expSize, datasetId, attempt, protocol, false);
  }

  @Test
  public void asyncUploadTest() throws IOException {
    // State
    int expSize = 210;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    String protocol = "DWCA";

    // When
    long result = DwcaFragmentsUploader.builder()
        .config(FragmentsConfig.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(regularDwca)
        .tempDir(regularDwca)
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
    TableAssert.assertTableData(HBASE_SERVER.getConnection(), expSize, datasetId, attempt, protocol, false);
  }

  @Test
  public void syncUpdateUploadTest() throws IOException {
    // State
    int expSize = 210;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attemptFirst = 231;
    int attemptSecond = 232;
    String protocol = "DWCA";

    // When
    long resultFirst = DwcaFragmentsUploader.builder()
        .config(FragmentsConfig.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(regularDwca)
        .tempDir(regularDwca)
        .useTriplet(false)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attemptFirst)
        .protocol("XML")
        .hbaseConnection(HBASE_SERVER.getConnection())
        .executor(Executors.newFixedThreadPool(2))
        .build()
        .upload();

    long resultSecond = DwcaFragmentsUploader.builder()
        .config(FragmentsConfig.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(regularDwca)
        .tempDir(regularDwca)
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
    TableAssert.assertTableData(HBASE_SERVER.getConnection(), expSize, datasetId, attemptSecond, protocol, true);
  }

  @Test
  public void asyncUpdateUploadTest() throws IOException {
    // State
    int expSize = 210;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attemptFirst = 231;
    int attemptSecond = 232;
    String protocol = "DWCA";

    // When
    long resultFirst = DwcaFragmentsUploader.builder()
        .config(FragmentsConfig.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(regularDwca)
        .tempDir(regularDwca)
        .useTriplet(false)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attemptFirst)
        .protocol("XML")
        .hbaseConnection(HBASE_SERVER.getConnection())
        .executor(Executors.newFixedThreadPool(2))
        .useSyncMode(false)
        .build()
        .upload();

    long resultSecond = DwcaFragmentsUploader.builder()
        .config(FragmentsConfig.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(regularDwca)
        .tempDir(regularDwca)
        .useTriplet(false)
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
    TableAssert.assertTableData(HBASE_SERVER.getConnection(), expSize, datasetId, attemptSecond, protocol, true);
  }

  @Test
  public void occExtSyncUploadTest() throws IOException {
    // State
    int expSize = 477;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    String protocol = "DWCA";

    // When
    long result = DwcaFragmentsUploader.builder()
        .config(FragmentsConfig.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(occurrenceAsExtensionDwca)
        .tempDir(occurrenceAsExtensionDwca)
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
    TableAssert.assertTableData(HBASE_SERVER.getConnection(), expSize, datasetId, attempt, protocol, false);
  }

  @Test
  public void occExtAsyncUploadTest() throws IOException {
    // State
    int expSize = 477;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    String protocol = "DWCA";

    // When
    long result = DwcaFragmentsUploader.builder()
        .config(FragmentsConfig.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(occurrenceAsExtensionDwca)
        .tempDir(occurrenceAsExtensionDwca)
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
    TableAssert.assertTableData(HBASE_SERVER.getConnection(), expSize, datasetId, attempt, protocol, false);
  }

  @Test
  public void multimediaSyncUploadTest() throws IOException {
    // State
    int expSize = 368;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    String protocol = "DWCA";

    // When
    long result = DwcaFragmentsUploader.builder()
        .config(FragmentsConfig.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(multimediaExtensionDwca)
        .tempDir(multimediaExtensionDwca)
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
    TableAssert.assertTableData(HBASE_SERVER.getConnection(), expSize, datasetId, attempt, protocol, false);
  }

  @Test
  public void multimediaAsyncUploadTest() throws IOException {
    // State
    int expSize = 368;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    String protocol = "DWCA";

    // When
    long result = DwcaFragmentsUploader.builder()
        .config(FragmentsConfig.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(multimediaExtensionDwca)
        .tempDir(multimediaExtensionDwca)
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
    TableAssert.assertTableData(HBASE_SERVER.getConnection(), expSize, datasetId, attempt, protocol, false);
  }
}
