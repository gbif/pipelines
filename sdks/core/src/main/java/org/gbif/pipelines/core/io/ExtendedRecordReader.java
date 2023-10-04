package org.gbif.pipelines.core.io;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.record.StarRecord;
import org.gbif.pipelines.core.converters.ExtendedRecordConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ExtendedRecordReader {

  private static final Function<Object, ExtendedRecord> CONVERT_FN =
      r -> ExtendedRecordConverter.from((Record) r, Collections.emptyMap());

  private static final Function<Object, ExtendedRecord> CONVERT_EXT_FN =
      r -> {
        StarRecord starRecord = (StarRecord) r;
        return ExtendedRecordConverter.from(starRecord.core(), starRecord.extensions());
      };

  public static DwcaReader<ExtendedRecord> fromLocation(String path) throws IOException {
    return DwcaReader.fromLocation(path, CONVERT_FN, CONVERT_EXT_FN);
  }

  public static DwcaReader<ExtendedRecord> fromCompressed(String source, String workingDir)
      throws IOException {
    return DwcaReader.fromCompressed(source, workingDir, CONVERT_FN, CONVERT_EXT_FN);
  }
}
