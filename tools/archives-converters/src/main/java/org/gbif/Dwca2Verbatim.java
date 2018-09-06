package org.gbif;

import org.gbif.converter.ConverterToVerbatim;
import org.gbif.pipelines.core.io.DwcaReader;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.avro.file.DataFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dwca2Verbatim extends ConverterToVerbatim {

  private static final Logger LOG = LoggerFactory.getLogger(Dwca2Verbatim.class);

  private Dwca2Verbatim() {}

  public static Dwca2Verbatim create() {
    return new Dwca2Verbatim();
  }

  /** TODO: DOC */
  public static void main(String... args) {
    if (args.length < 2) {
      throw new IllegalArgumentException("You must specify input and output paths");
    }
    String inputPath = args[0];
    String outputPath = args[1];
    boolean isFileCreated = Dwca2Verbatim.create().convert(inputPath, outputPath);
    LOG.info("Verbatim avro file has been created - {}", isFileCreated);
  }

  /** TODO: DOC */
  @Override
  protected void convert(Path inputPath, DataFileWriter<ExtendedRecord> dataFileWriter)
      throws IOException {
    DwcaReader reader = new DwcaReader(inputPath.toString());
    reader.init();
    LOG.info("Exporting the DwC Archive to avro {} started");
    // Read first record
    dataFileWriter.append(reader.getCurrent());
    // Read all records
    while (reader.advance()) {
      dataFileWriter.append(reader.getCurrent());
    }
  }
}
