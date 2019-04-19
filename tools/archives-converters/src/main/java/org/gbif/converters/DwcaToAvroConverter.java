package org.gbif.converters;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import org.gbif.converters.converter.ConverterToVerbatim;
import org.gbif.pipelines.core.io.DwcaReader;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.avro.file.DataFileWriter;

import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.core.converters.ExtendedRecordConverter.RECORD_ID_ERROR;
import static org.gbif.pipelines.core.utils.HashUtils.getSha1;

/**
 * Converts DWC archive into {@link ExtendedRecord} AVRO file
 */
@Slf4j
@NoArgsConstructor(staticName = "create")
@Builder
public class DwcaToAvroConverter extends ConverterToVerbatim {

  public static void main(String... args) {
    if (args.length < 2) {
      throw new IllegalArgumentException("You must specify input and output paths");
    }
    String inputPath = args[0];
    String outputPath = args[1];
    boolean isFileCreated = DwcaToAvroConverter.create().inputPath(inputPath).outputPath(outputPath).convert();
    log.info("Verbatim avro file has been created - {}", isFileCreated);
  }

  /**
   * Converts DWC archive into {@link ExtendedRecord} AVRO file
   *
   * @param inputPath Path to DWCA file
   * @param dataFileWriter AVRO data writer for {@link ExtendedRecord}
   */
  @Override
  protected long convert(Path inputPath, DataFileWriter<ExtendedRecord> dataFileWriter) throws IOException {
    return convert(inputPath, dataFileWriter, null);
  }

  /**
   * Converts DWC archive into {@link ExtendedRecord} AVRO file
   *
   * @param inputPath Path to DWCA file
   * @param dataFileWriter AVRO data writer for {@link ExtendedRecord}
   * @param idHashPrefix prefix to use for hash function and get hashed id instead of raw id, as example it can be
   * dataset ID
   */
  @Override
  protected long convert(Path inputPath, DataFileWriter<ExtendedRecord> dataFileWriter, String idHashPrefix)
      throws IOException {
    DwcaReader reader = DwcaReader.fromLocation(inputPath.toString());
    log.info("Exporting the DwC Archive to Avro started {}", inputPath);

    // Read all records
    while (reader.advance()) {
      ExtendedRecord record = reader.getCurrent();
      String id = record.getId();
      if (!id.equals(RECORD_ID_ERROR)) {
        Optional.ofNullable(idHashPrefix).ifPresent(x -> record.setId(getSha1(idHashPrefix, id)));
        dataFileWriter.append(record);
      }
    }
    reader.close();

    return reader.getRecordsReturned();
  }
}
