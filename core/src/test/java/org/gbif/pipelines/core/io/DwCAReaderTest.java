package org.gbif.pipelines.core.io;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class DwCAReaderTest {

  @Test
  public void testUncompressedReader() throws IOException {
    // State
    String fileName = getClass().getResource("/dwca/plants_dwca").getFile();

    // When
    DwCAReader dwCAReader = new DwCAReader(fileName);
    dwCAReader.init();
    ExtendedRecord current = dwCAReader.getCurrent();

    // Should
    assertNotNull(current);
    assertNotNull(current.getId());
  }

  @Test
  public void testZipFileReader() throws IOException {
    // State
    String fileName = getClass().getResource("/dwca/plants.zip").getFile();
    String fileOut = new File("target/tmp").getAbsolutePath();

    // When
    DwCAReader dwCAReader = new DwCAReader(fileName, fileOut);
    dwCAReader.init();
    ExtendedRecord current = dwCAReader.getCurrent();

    // Should
    assertNotNull(current);
    assertNotNull(current.getId());
  }

}