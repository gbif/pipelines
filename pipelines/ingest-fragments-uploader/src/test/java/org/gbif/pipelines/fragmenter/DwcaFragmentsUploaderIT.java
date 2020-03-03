package org.gbif.pipelines.fragmenter;

import java.nio.file.Paths;

import org.gbif.pipelines.fragmenter.common.FragmentsConfiguration;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
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
        .build()
        .upload();

  }

  @Test(expected = NullPointerException.class)
  public void pathToArchvieIsNullTest() {
    // When
    DwcaFragmentsUploader.builder()
        .config(FragmentsConfiguration.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .build()
        .upload();

  }

  @Test
  public void test() {
    // When
    long result = DwcaFragmentsUploader.builder()
        .config(FragmentsConfiguration.create(HbaseServer.FRAGMENT_TABLE_NAME))
        .pathToArchive(Paths.get(inpPath))
        .build()
        .upload();

    Assert.assertEquals(307, result);

  }

}
