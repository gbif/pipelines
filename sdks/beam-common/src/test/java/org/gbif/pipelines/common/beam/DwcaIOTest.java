package org.gbif.pipelines.common.beam;

import static org.junit.Assert.*;

import java.io.IOException;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.gbif.pipelines.common.beam.DwcaIO.Read;
import org.gbif.pipelines.core.io.ExtendedRecordReader;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Test;

public class DwcaIOTest {

  @Test
  public void fromLocationTest() throws IOException {
    String inpPath = getClass().getResource("/dwca/plants_dwca").getFile();

    Read<ExtendedRecord> read =
        Read.create(ExtendedRecord.class, ExtendedRecordReader.fromLocation(inpPath));

    ResourceHints hints = read.getResourceHints();
    assertNotNull(read);
    assertEquals(0, hints.hints().size());
  }
}
