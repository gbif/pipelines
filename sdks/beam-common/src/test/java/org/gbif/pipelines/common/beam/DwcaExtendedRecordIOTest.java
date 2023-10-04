package org.gbif.pipelines.common.beam;

import static org.junit.Assert.*;

import com.google.common.io.Files;
import java.io.File;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.gbif.pipelines.common.beam.DwcaIO.Read;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Test;

public class DwcaExtendedRecordIOTest {

  @Test
  public void fromLocationTest() {
    String inpPath = getClass().getResource("/dwca/plants_dwca").getFile();

    Read<ExtendedRecord> read = DwcaExtendedRecordIO.fromLocation(inpPath);

    ResourceHints hints = read.getResourceHints();
    assertNotNull(read);
    assertEquals(0, hints.hints().size());
  }

  @Test
  public void fromCompressedTest() {
    String inpPath = getClass().getResource("/dwca/plants.zip").getFile();
    File tempDir = Files.createTempDir();

    Read<ExtendedRecord> read = DwcaExtendedRecordIO.fromCompressed(inpPath, tempDir.getPath());

    ResourceHints hints = read.getResourceHints();
    assertNotNull(read);
    assertEquals(0, hints.hints().size());
  }
}
