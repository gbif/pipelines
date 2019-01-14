package org.gbif.converters.parser.xml.parsing.extendedrecord;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.gbif.converters.parser.xml.OccurrenceParser;
import org.gbif.converters.parser.xml.ParsingException;
import org.gbif.converters.parser.xml.parsing.validators.UniquenessValidator;
import org.gbif.converters.parser.xml.parsing.xml.XmlFragmentParser;

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
  private final AtomicLong counter;

  public ConverterTask(File inputFile, SyncDataFileWriter writer, UniquenessValidator validator, AtomicLong counter) {
    this.inputFile = inputFile;
    this.dataFileWriter = writer;
    this.validator = validator;
    this.counter = counter;
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
                            counter.incrementAndGet();
                          } catch (IOException ex) {
                            LOG.error(ex.getMessage(), ex);
                            throw new ParsingException("Parsing failed", ex);
                          }
                        }));
  }
}
