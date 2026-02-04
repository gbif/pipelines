package org.gbif.pipelines.core.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;
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

  @Test
  public void extensionsCountTest() throws IOException {
    // State
    String fileName = getClass().getResource("/dwca/plants_dwca_ext").getFile();

    // When
    try (DwcaExtendedRecordReader dwcaReader = DwcaExtendedRecordReader.fromLocation(fileName)) {
      // Read all records
      while (dwcaReader.advance()) {
        // Just advance through all records
      }

      // Should
      Map<String, Long> extensionsCount = dwcaReader.getExtensionsCount();
      assertNotNull("extensionsCount should not be null", extensionsCount);
      assertTrue("extensionsCount should contain Identifier extension", 
          extensionsCount.containsKey("http://rs.gbif.org/terms/1.0/Identifier"));
      
      // Verify the count is correct - the test data has 307 identifier extension records
      assertEquals("extensionsCount should have 307 Identifier records", 
          Long.valueOf(307), extensionsCount.get("http://rs.gbif.org/terms/1.0/Identifier"));
    }
  }

  @Test
  public void extensionsCountNoExtensionsTest() throws IOException {
    // State
    String fileName = getClass().getResource("/dwca/plants_dwca").getFile();

    // When
    try (DwcaExtendedRecordReader dwcaReader = DwcaExtendedRecordReader.fromLocation(fileName)) {
      // Read all records
      while (dwcaReader.advance()) {
        // Just advance through all records
      }

      // Should
      Map<String, Long> extensionsCount = dwcaReader.getExtensionsCount();
      assertNotNull("extensionsCount should not be null", extensionsCount);
      assertTrue("extensionsCount should be empty when there are no extensions", 
          extensionsCount.isEmpty());
    }
  }
}
