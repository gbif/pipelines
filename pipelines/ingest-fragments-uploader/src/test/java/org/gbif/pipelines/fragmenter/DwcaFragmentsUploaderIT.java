package org.gbif.pipelines.fragmenter;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.Executors;

import org.gbif.pipelines.fragmenter.common.FragmentsConfig;
import org.gbif.pipelines.fragmenter.common.HbaseServer;
import org.gbif.pipelines.fragmenter.common.TableAssert;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.JVM)
public class DwcaFragmentsUploaderIT {

  /** {@link ClassRule} requires this field to be public. */
  @ClassRule
  public static final HbaseServer HBASE_SERVER = new HbaseServer();

  private final String inpPath = getClass().getResource("/dwca").getFile();

  @Test
  public void syncUploadTest() throws IOException {
    // State
    int expSize = 210;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;

    // When
    long result = DwcaFragmentsUploader.builder()
        .config(FragmentsConfig.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(Paths.get(inpPath))
        .tempDir(Paths.get(inpPath))
        .useTriplet(false)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attempt)
        .hbaseConnection(HBASE_SERVER.getConnection())
        .build()
        .upload();

    // Should
    Assert.assertEquals(expSize, result);
    TableAssert.assertTableData(HBASE_SERVER.getConnection(), expSize, datasetId, attempt);
  }

  @Test
  public void asyncUploadTest() throws IOException {
    // State
    int expSize = 210;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;

    // When
    long result = DwcaFragmentsUploader.builder()
        .config(FragmentsConfig.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(Paths.get(inpPath))
        .tempDir(Paths.get(inpPath))
        .useTriplet(false)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attempt)
        .executor(Executors.newFixedThreadPool(2))
        .hbaseConnection(HBASE_SERVER.getConnection())
        .useSyncMode(false)
        .build()
        .upload();

    // Should
    Assert.assertEquals(expSize, result);
    TableAssert.assertTableData(HBASE_SERVER.getConnection(), expSize, datasetId, attempt);
  }

  @Test
  public void syncUpdateUploadTest() throws IOException {
    // State
    int expSize = 210;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attemptFirst = 231;
    int attemptSecond = 232;

    // When
    long resultFirst = DwcaFragmentsUploader.builder()
        .config(FragmentsConfig.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(Paths.get(inpPath))
        .tempDir(Paths.get(inpPath))
        .useTriplet(false)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attemptFirst)
        .hbaseConnection(HBASE_SERVER.getConnection())
        .executor(Executors.newFixedThreadPool(2))
        .build()
        .upload();

    long resultSecond = DwcaFragmentsUploader.builder()
        .config(FragmentsConfig.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(Paths.get(inpPath))
        .tempDir(Paths.get(inpPath))
        .useTriplet(false)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attemptSecond)
        .hbaseConnection(HBASE_SERVER.getConnection())
        .build()
        .upload();

    // Should
    Assert.assertEquals(expSize, resultFirst);
    Assert.assertEquals(expSize, resultSecond);
    TableAssert.assertTableData(HBASE_SERVER.getConnection(), expSize, datasetId, attemptSecond);
  }

  @Test
  public void asyncUpdateUploadTest() throws IOException {
    // State
    int expSize = 210;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attemptFirst = 231;
    int attemptSecond = 232;

    // When
    long resultFirst = DwcaFragmentsUploader.builder()
        .config(FragmentsConfig.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(Paths.get(inpPath))
        .tempDir(Paths.get(inpPath))
        .useTriplet(false)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attemptFirst)
        .hbaseConnection(HBASE_SERVER.getConnection())
        .executor(Executors.newFixedThreadPool(2))
        .useSyncMode(false)
        .build()
        .upload();

    long resultSecond = DwcaFragmentsUploader.builder()
        .config(FragmentsConfig.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(Paths.get(inpPath))
        .tempDir(Paths.get(inpPath))
        .useTriplet(false)
        .useOccurrenceId(true)
        .datasetId(datasetId)
        .attempt(attemptSecond)
        .batchSize(2)
        .hbaseConnection(HBASE_SERVER.getConnection())
        .useSyncMode(false)
        .build()
        .upload();

    // Should
    Assert.assertEquals(expSize, resultFirst);
    Assert.assertEquals(expSize, resultSecond);
    TableAssert.assertTableData(HBASE_SERVER.getConnection(), expSize, datasetId, attemptSecond);
  }
}
