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
public class XmlFragmentsUploaderIT {

  /** {@link ClassRule} requires this field to be public. */
  @ClassRule
  public static final HbaseServer HBASE_SERVER = new HbaseServer();

  private final String inpPath =
      getClass().getResource("/xml").getFile();

  @Test(expected = NullPointerException.class)
  public void hbaseConfigIsNullTest() {
    // When
    XmlFragmentsUploader.builder()
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
    XmlFragmentsUploader.builder()
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
    int expSize = 40;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 1;

    // When
    long result = XmlFragmentsUploader.builder()
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
    int expSize = 40;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 1;

    // When
    long result = XmlFragmentsUploader.builder()
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
  public void syncDoubeUploadTest() throws IOException {
    // State
    int expSize = 40;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 1;

    // When
    long resultFirst = XmlFragmentsUploader.builder()
        .config(FragmentsConfiguration.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(Paths.get(inpPath))
        .datasetId(datasetId)
        .attempt(attempt)
        .hbaseConnection(HBASE_SERVER.getConnection())
        .build()
        .upload();

    long resultSecond = XmlFragmentsUploader.builder()
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
  public void asyncDoubeUploadTest() throws IOException {
    // State
    int expSize = 40;
    String datasetId = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 1;

    // When
    long resultFirst = XmlFragmentsUploader.builder()
        .config(FragmentsConfiguration.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .keygenConfig(HbaseServer.CFG)
        .pathToArchive(Paths.get(inpPath))
        .datasetId(datasetId)
        .attempt(attempt)
        .hbaseConnection(HBASE_SERVER.getConnection())
        .build()
        .upload();

    long resultSecond = XmlFragmentsUploader.builder()
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
