package org.gbif.pipelines.fragmenter;

import java.io.IOException;
import java.nio.file.Paths;

import org.gbif.pipelines.fragmenter.common.FragmentsConfiguration;
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

  @Test(expected = NullPointerException.class)
  public void hbaseConfigIsNullTest() {
    // When
    DwcaFragmentsUploader.builder()
        .pathToArchive(Paths.get(inpPath))
        .keygenConfig(HbaseServer.CFG)
        .datasetId("50c9509d-22c7-4a22-a47d-8c48425ef4a8")
        .attempt(1)
        .build()
        .upload();

  }

  @Test(expected = NullPointerException.class)
  public void pathToArchvieIsNullTest() {
    // When
    DwcaFragmentsUploader.builder()
        .config(FragmentsConfiguration.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .datasetId("50c9509d-22c7-4a22-a47d-8c48425ef4a8")
        .attempt(1)
        .build()
        .upload();
  }

  @Test
  public void syncUploadTest() throws IOException {
    // State
    int expSize = 210;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;

    // When
    long result = DwcaFragmentsUploader.builder()
        .config(FragmentsConfiguration.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(Paths.get(inpPath))
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
        .config(FragmentsConfiguration.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(Paths.get(inpPath))
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
  public void syncDoubleUploadTest() throws IOException {
    // State
    int expSize = 210;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;

    // When
    long resultFirst = DwcaFragmentsUploader.builder()
        .config(FragmentsConfiguration.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(Paths.get(inpPath))
        .datasetId(datasetId)
        .attempt(attempt)
        .hbaseConnection(HBASE_SERVER.getConnection())
        .build()
        .upload();

    long resultSecond = DwcaFragmentsUploader.builder()
        .config(FragmentsConfiguration.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(Paths.get(inpPath))
        .datasetId(datasetId)
        .attempt(attempt)
        .hbaseConnection(HBASE_SERVER.getConnection())
        .build()
        .upload();

    // Should
    Assert.assertEquals(expSize, resultFirst);
    Assert.assertEquals(expSize, resultSecond);
    TableAssert.assertTableData(HBASE_SERVER.getConnection(), expSize, datasetId, attempt);
  }

  @Test
  public void asyncDoubleUploadTest() throws IOException {
    // State
    int expSize = 210;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;

    // When
    long resultFirst = DwcaFragmentsUploader.builder()
        .config(FragmentsConfiguration.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(Paths.get(inpPath))
        .datasetId(datasetId)
        .attempt(attempt)
        .hbaseConnection(HBASE_SERVER.getConnection())
        .build()
        .upload();

    long resultSecond = DwcaFragmentsUploader.builder()
        .config(FragmentsConfiguration.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(Paths.get(inpPath))
        .datasetId(datasetId)
        .attempt(attempt)
        .hbaseConnection(HBASE_SERVER.getConnection())
        .build()
        .upload();

    // Should
    Assert.assertEquals(expSize, resultFirst);
    Assert.assertEquals(expSize, resultSecond);
    TableAssert.assertTableData(HBASE_SERVER.getConnection(), expSize, datasetId, attempt);
  }
}
