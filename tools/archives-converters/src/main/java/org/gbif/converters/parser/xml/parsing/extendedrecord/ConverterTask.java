package org.gbif.converters.parser.xml.parsing.extendedrecord;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.gbif.converters.parser.xml.OccurrenceParser;
import org.gbif.converters.parser.xml.ParsingException;
import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;
import org.gbif.converters.parser.xml.parsing.validators.UniquenessValidator;
import org.gbif.converters.parser.xml.parsing.xml.XmlFragmentParser;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.converters.converter.HashUtils.getSha1;
import static org.gbif.converters.parser.xml.parsing.extendedrecord.ExtendedRecordConverter.RECORD_ID_ERROR;

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
  private final String idHashPrefix;

  public ConverterTask(File inputFile, SyncDataFileWriter writer, UniquenessValidator validator, AtomicLong counter,
      String idHashPrefix) {
    this.inputFile = inputFile;
    this.dataFileWriter = writer;
    this.validator = validator;
    this.counter = counter;
    this.idHashPrefix = idHashPrefix;
  }

  @Override
  public void run() {
    new OccurrenceParser()
        .parseFile(inputFile)
        .stream()
        .map(XmlFragmentParser::parseRecord)
        .forEach(this::appendRawOccurrenceRecords);
  }

  /*TODO:DOC*/
  private void appendRawOccurrenceRecords(List<RawOccurrenceRecord> records) {
    records
        .stream()
        .map(ExtendedRecordConverter::from)
        .filter(extendedRecord -> validator.isUnique(extendedRecord.getId()))
        .forEach(this::appendExtendedRecord);
  }

  /*TODO:DOC*/
  private void appendExtendedRecord(ExtendedRecord record) {
    try {
      String id = record.getId();
      if (!id.equals(RECORD_ID_ERROR)) {
        Optional.ofNullable(idHashPrefix).ifPresent(x -> record.setId(getSha1(idHashPrefix, id)));
        dataFileWriter.append(record);
        counter.incrementAndGet();
      }
    } catch (IOException ex) {
      LOG.error(ex.getMessage(), ex);
      throw new ParsingException("Parsing failed", ex);
    }
  }
}
