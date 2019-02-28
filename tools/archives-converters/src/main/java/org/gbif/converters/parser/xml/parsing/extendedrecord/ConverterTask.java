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

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.converters.converter.HashUtils.getSha1;
import static org.gbif.converters.parser.xml.parsing.extendedrecord.ExtendedRecordConverter.RECORD_ID_ERROR;

/**
 * The task for CompletableFuture which reads a xml response file, parses and converts to
 * ExtendedRecord avro file
 */
@Slf4j
@AllArgsConstructor
public class ConverterTask implements Runnable {

  private final File inputFile;
  private final SyncDataFileWriter dataFileWriter;
  private final UniquenessValidator validator;
  private final AtomicLong counter;
  private final String idHashPrefix;

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
      log.error(ex.getMessage(), ex);
      throw new ParsingException("Parsing failed", ex);
    }
  }
}
