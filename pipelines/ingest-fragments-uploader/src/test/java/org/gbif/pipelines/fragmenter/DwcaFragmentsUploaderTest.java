package org.gbif.pipelines.fragmenter;

import java.nio.file.Paths;

import org.gbif.pipelines.fragmenter.common.HbaseConfiguration;

import org.junit.Assert;
import org.junit.Test;

public class DwcaFragmentsUploaderTest {

  private final String inpPath = getClass().getResource("/dwca").getFile();

  @Test(expected = NullPointerException.class)
  public void hbaseConfigIsNullTest() {
    // When
    DwcaFragmentsUploader.builder()
        .pathToArchive(Paths.get(inpPath))
        .build()
        .upload();

  }

  @Test(expected = NullPointerException.class)
  public void pathToArchvieIsNullTest() {
    // When
    DwcaFragmentsUploader.builder()
        .config(HbaseConfiguration.create())
        .build()
        .upload();

  }

  @Test
  public void test() {
    // When
    long result = DwcaFragmentsUploader.builder()
        .config(HbaseConfiguration.create())
        .pathToArchive(Paths.get(inpPath))
        .build()
        .upload();

    Assert.assertEquals(307, result);

  }

}
