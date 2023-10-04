package org.gbif.pipelines.common.beam;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.gbif.pipelines.core.io.ExtendedRecordReader;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * IO operations for DwC-A formats.
 *
 * <p>Provides the ability to read a DwC-A as a bounded source, but in a non-splittable manner. This
 * means that a single threaded approach to reading is enforced.
 *
 * <p>This is intended only for demonstration usage, and not for production.
 *
 * <p>To use this:
 *
 * <pre>{@code
 * p.apply("read", DwcaIO.Read.fromCompressed("/tmp/my-dwca.zip", "/tmp/working")
 *     ...;
 * }</pre>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DwcaExtendedRecordIO {

  @SneakyThrows
  public static DwcaIO.Read<ExtendedRecord> fromLocation(String inputPath) {
    return DwcaIO.Read.create(ExtendedRecord.class, ExtendedRecordReader.fromLocation(inputPath));
  }

  @SneakyThrows
  public static DwcaIO.Read<ExtendedRecord> fromCompressed(String inputPath, String tmpPath) {
    return DwcaIO.Read.create(
        ExtendedRecord.class, ExtendedRecordReader.fromCompressed(inputPath, tmpPath));
  }
}
