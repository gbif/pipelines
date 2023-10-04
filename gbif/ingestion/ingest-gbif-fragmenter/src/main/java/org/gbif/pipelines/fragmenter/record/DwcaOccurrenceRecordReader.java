package org.gbif.pipelines.fragmenter.record;

import java.io.IOException;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.dwc.record.StarRecord;
import org.gbif.pipelines.core.io.DwcaReader;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DwcaOccurrenceRecordReader {
  private static final Function<Object, DwcaOccurrenceRecord> CONVERT_FN =
      r -> DwcaOccurrenceRecord.create((StarRecord) r);

  public static DwcaReader<DwcaOccurrenceRecord> fromLocation(String path) throws IOException {
    return DwcaReader.fromLocation(path, CONVERT_FN, CONVERT_FN);
  }

  public static DwcaReader<DwcaOccurrenceRecord> fromCompressed(String source, String workingDir)
      throws IOException {
    return DwcaReader.fromCompressed(source, workingDir, CONVERT_FN, CONVERT_FN);
  }
}
