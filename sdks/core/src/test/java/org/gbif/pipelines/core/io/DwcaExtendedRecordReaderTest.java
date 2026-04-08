package org.gbif.pipelines.core.io;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Test;

public class DwcaExtendedRecordReaderTest {

  @Test
  public void uncompressedReaderExtensionTest() throws IOException {
    // State
    String fileName = getClass().getResource("/dwca/plants_dwca_ext").getFile();

    // When
    try (DwcaExtendedRecordReader dwcaReader = DwcaExtendedRecordReader.fromLocation(fileName)) {
      dwcaReader.advance();
      ExtendedRecord current = dwcaReader.getCurrent();
      // Should
      assertNotNull(current);
      assertNotNull(current.getId());
    }
  }

  @Test
  public void uncompressedReaderTest() throws IOException {
    // State
    String fileName = getClass().getResource("/dwca/plants_dwca").getFile();

    // When
    try (DwcaExtendedRecordReader dwcaReader = DwcaExtendedRecordReader.fromLocation(fileName)) {
      dwcaReader.advance();
      ExtendedRecord current = dwcaReader.getCurrent();
      // Should
      assertNotNull(current);
      assertNotNull(current.getId());
    }
  }

  @Test(expected = NullPointerException.class)
  public void nullTermTest() throws IOException {
    // State
    String fileName = getClass().getResource("/dwca/plants_dwca_null").getFile();

    // When
    try (DwcaExtendedRecordReader dwcaReader = DwcaExtendedRecordReader.fromLocation(fileName)) {
      dwcaReader.advance();
      // Should
      dwcaReader.getCurrent();
    }
  }
}
