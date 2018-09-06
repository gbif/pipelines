package org.gbif.converters.parser.xml.parsing.extendedrecord;

import org.gbif.converters.parser.xml.OccurrenceParser;
import org.gbif.converters.parser.xml.ParsingException;
import org.gbif.converters.parser.xml.parsing.validators.UniquenessValidator;
import org.gbif.converters.parser.xml.parsing.xml.XmlFragmentParser;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The task for CompletableFuture which reads a xml response file, parses and converts to
 * ExtendedRecord avro file
 */
public class ConverterTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ConverterTask.class);

  private final File inputFile;
  private final SyncDataFileWriter dataFileWriter;
  private final UniquenessValidator validator;

  public ConverterTask(
      File inputFile, SyncDataFileWriter dataFileWriter, UniquenessValidator validator) {
    this.inputFile = inputFile;
    this.dataFileWriter = dataFileWriter;
    this.validator = validator;
  }

  @Override
  public void run() {
    new OccurrenceParser()
        .parseFile(inputFile)
        .stream()
        .map(XmlFragmentParser::parseRecord)
        .forEach(
            rawRecords ->
                rawRecords
                    .stream()
                    .map(ExtendedRecordConverter::from)
                    .filter(extendedRecord -> validator.isUnique(extendedRecord.getId()))
                    .forEach(
                        extendedRecord -> {
                          try {
                            dataFileWriter.append(extendedRecord);
                          } catch (IOException ex) {
                            LOG.error(ex.getMessage(), ex);
                            throw new ParsingException("Parsing failed", ex);
                          }
                        }));
  }
}
