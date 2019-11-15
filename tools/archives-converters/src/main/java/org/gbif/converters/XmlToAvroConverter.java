package org.gbif.converters;

import java.nio.file.Path;

import org.gbif.converters.converter.ConverterToVerbatim;
import org.gbif.converters.converter.SyncDataFileWriter;
import org.gbif.converters.parser.xml.ExtendedRecordConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Converts ABCD/etc archive into {@link ExtendedRecord} AVRO file
 */
@Slf4j
@NoArgsConstructor(staticName = "create")
public class XmlToAvroConverter extends ConverterToVerbatim {

  private int xmlReaderParallelism = Runtime.getRuntime().availableProcessors();

  /**
   * @param xmlReaderParallelism number of threads for reader
   */
  public XmlToAvroConverter xmlReaderParallelism(int xmlReaderParallelism) {
    this.xmlReaderParallelism = xmlReaderParallelism;
    return this;
  }

  public static void main(String... args) {
    if (args.length < 2) {
      throw new IllegalArgumentException("You must specify input and output paths");
    }
    String inputPath = args[0];
    String outputPath = args[1];
    boolean isFileCreated = XmlToAvroConverter.create().inputPath(inputPath).outputPath(outputPath).convert();
    log.info("Verbatim avro file has been created - {}", isFileCreated);
  }

  /**
   * Converts ABCD/etc archive into {@link ExtendedRecord} AVRO file
   *
   * @param inputPath Path to DWCA file
   * @param dataFileWriter AVRO data writer for {@link ExtendedRecord}
   */
  @Override
  public long convert(Path inputPath, SyncDataFileWriter<ExtendedRecord> dataFileWriter) {
    return ExtendedRecordConverter.crete(xmlReaderParallelism).toAvro(inputPath.toString(), dataFileWriter);
  }
}
