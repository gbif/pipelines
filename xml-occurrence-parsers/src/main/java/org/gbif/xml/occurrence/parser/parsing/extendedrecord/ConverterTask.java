package org.gbif.xml.occurrence.parser.parsing.extendedrecord;

import org.gbif.xml.occurrence.parser.OccurrenceParser;
import org.gbif.xml.occurrence.parser.ParsingException;
import org.gbif.xml.occurrence.parser.parsing.xml.XmlFragmentParser;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConverterTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ConverterTask.class);

  private final File file;
  private final DataFileWriterProxy dataFileWriter;

  public ConverterTask(File file, DataFileWriterProxy dataFileWriter) {
    this.file = file;
    this.dataFileWriter = dataFileWriter;
  }

  @Override
  public void run() {
    new OccurrenceParser().parseFile(file).stream()
      .map(XmlFragmentParser::parseRecord)
      .forEach(rawRecords -> rawRecords.stream()
        .filter(MapCache.getInstance()::isUnique)
        .forEach(rawRecord -> {
          try {
            dataFileWriter.append(ExtendedRecordConverter.from(rawRecord));
          } catch (IOException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new ParsingException("Parsing failed", ex);
          }
        })
      );
  }

}
