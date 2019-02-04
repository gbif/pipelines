package org.gbif.converters;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import org.gbif.converters.converter.ConverterToVerbatim;
import org.gbif.pipelines.core.io.DwcaReader;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.avro.file.DataFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.converters.converter.HashUtils.getSha1;
import static org.gbif.pipelines.core.converters.ExtendedRecordConverter.RECORD_ID_ERROR;

public class DwcaToAvroConverter extends ConverterToVerbatim {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaToAvroConverter.class);

  private DwcaToAvroConverter() {}

  public static DwcaToAvroConverter create() {
    return new DwcaToAvroConverter();
  }

  /** TODO: DOC */
  public static void main(String... args) {
    if (args.length < 2) {
      throw new IllegalArgumentException("You must specify input and output paths");
    }
    String inputPath = args[0];
    String outputPath = args[1];
    boolean isFileCreated = DwcaToAvroConverter.create().inputPath(inputPath).outputPath(outputPath).convert();
    LOG.info("Verbatim avro file has been created - {}", isFileCreated);
  }

  /** TODO: DOC */
  @Override
  protected long convert(Path inputPath, DataFileWriter<ExtendedRecord> dataFileWriter) throws IOException {
    return convert(inputPath, dataFileWriter, null);
  }

  /** TODO: DOC */
  @Override
  protected long convert(Path inputPath, DataFileWriter<ExtendedRecord> dataFileWriter, String idHashPrefix)
      throws IOException {
    DwcaReader reader = DwcaReader.fromLocation(inputPath.toString());
    LOG.info("Exporting the DwC Archive to Avro started {}", inputPath);

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
