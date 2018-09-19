package org.gbif.converters;

import org.gbif.converters.converter.ConverterToVerbatim;
import org.gbif.pipelines.core.io.DwcaReader;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.avro.file.DataFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    boolean isFileCreated = DwcaToAvroConverter.create().convert(inputPath, outputPath);
    LOG.info("Verbatim avro file has been created - {}", isFileCreated);
  }

  /** TODO: DOC */
  @Override
  protected void convert(Path inputPath, DataFileWriter<ExtendedRecord> dataFileWriter) throws IOException {
    DwcaReader reader = DwcaReader.fromLocation(inputPath.toString());
    LOG.info("Exporting the DwC Archive to Avro started {}", inputPath);

    // Read all records
    while (reader.advance()) {
      dataFileWriter.append(reader.getCurrent());
    }
  }
}
