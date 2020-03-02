package org.gbif.pipelines.fragmenter;

import java.nio.file.Paths;

import org.gbif.pipelines.fragmenter.common.HbaseConfiguration;

import org.junit.Assert;
import org.junit.Test;

public class XmlFragmentsUploaderTest {

  private final String inpPath =
      getClass().getResource("/xml").getFile();

  @Test(expected = NullPointerException.class)
  public void hbaseConfigIsNullTest() {
    // When
    XmlFragmentsUploader.builder()
        .pathToArchive(Paths.get(inpPath))
        .build()
        .upload();

  }

  @Test(expected = NullPointerException.class)
  public void pathToArchvieIsNullTest() {
    // When
    XmlFragmentsUploader.builder()
        .config(HbaseConfiguration.create())
        .build()
        .upload();

  }

  @Test
  public void test() {
    // When
    long result = XmlFragmentsUploader.builder()
        .config(HbaseConfiguration.create())
        .pathToArchive(Paths.get(inpPath))
        .build()
        .upload();

    Assert.assertEquals(42, result);

  }

}
