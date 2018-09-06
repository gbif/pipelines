package org.gbif.converters;

import org.gbif.converters.converter.ConverterToVerbatim;
import org.gbif.converters.parser.xml.ExtendedRecordConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.nio.file.Path;

import org.apache.avro.file.DataFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XmlToAvroConverter extends ConverterToVerbatim {

  private static final Logger LOG = LoggerFactory.getLogger(XmlToAvroConverter.class);

  private int xmlReaderParallelism = Runtime.getRuntime().availableProcessors();

  private XmlToAvroConverter() {}

  public static XmlToAvroConverter create() {
    return new XmlToAvroConverter();
  }

  public XmlToAvroConverter xmlReaderParallelism(int xmlReaderParallelism) {
    this.xmlReaderParallelism = xmlReaderParallelism;
    return this;
  }

  /** TODO: DOC */
  public static void main(String... args) {
    if (args.length < 2) {
      throw new IllegalArgumentException("You must specify input and output paths");
    }
    String inputPath = args[0];
    String outputPath = args[1];
    boolean isFileCreated = XmlToAvroConverter.create().convert(inputPath, outputPath);
    LOG.info("Verbatim avro file has been created - {}", isFileCreated);
  }

  /** TODO: DOC */
  @Override
  public void convert(Path inputPath, DataFileWriter<ExtendedRecord> dataFileWriter) {
    ExtendedRecordConverter.crete(xmlReaderParallelism)
        .toAvro(inputPath.toString(), dataFileWriter);
  }
}
