package org.gbif.pipelines.parsers.io;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class DwcaReaderTest {

  @Test
  public void uncompressedReaderTest() throws IOException {
    // State
    String fileName = getClass().getResource("/dwca/plants_dwca").getFile();

    // When
    DwcaReader dwCAReader = new DwcaReader(fileName);
    dwCAReader.init();
    ExtendedRecord current = dwCAReader.getCurrent();
    dwCAReader.close();

    // Should
    assertNotNull(current);
    assertNotNull(current.getId());
  }

  @Test
  public void zipFileReaderTest() throws IOException {
    // State
    String fileName = getClass().getResource("/dwca/plants.zip").getFile();
    String fileOut = new File("target/tmp").getAbsolutePath();

    // When
    DwcaReader dwCAReader = new DwcaReader(fileName, fileOut);
    dwCAReader.init();
    ExtendedRecord current = dwCAReader.getCurrent();
    dwCAReader.close();

    // Should
    assertNotNull(current);
    assertNotNull(current.getId());
  }
}
