package org.gbif.converters.parser.xml.parsing.extendedrecord;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.converters.converter.SyncDataFileWriter;
import org.gbif.converters.parser.xml.OccurrenceParser;
import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;
import org.gbif.converters.parser.xml.parsing.validators.UniquenessValidator;
import org.gbif.converters.parser.xml.parsing.xml.XmlFragmentParser;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * The task for CompletableFuture which reads a xml response file, parses and converts to
 * ExtendedRecord avro file
 */
@Slf4j
@AllArgsConstructor
public class ConverterTask implements Runnable {

  private final File inputFile;
  private final SyncDataFileWriter<ExtendedRecord> dataFileWriter;
  private final UniquenessValidator validator;
  private final AtomicLong counter;

  /**
   * Converts list of {@link org.gbif.converters.parser.xml.parsing.RawXmlOccurrence} into list of
   * {@link RawOccurrenceRecord} and appends AVRO file
   */
  @Override
  public void run() {
    new OccurrenceParser()
        .parseFile(inputFile).stream()
            .map(XmlFragmentParser::parseRecord)
            .forEach(this::appendRawOccurrenceRecords);
  }

  /**
   * Converts list of {@link RawOccurrenceRecord} into list of {@link ExtendedRecord} and appends
   * AVRO file
   */
  private void appendRawOccurrenceRecords(List<RawOccurrenceRecord> records) {
    records.stream()
        .map(ExtendedRecordConverter::from)
        .filter(extendedRecord -> validator.isUnique(extendedRecord.getId()))
        .forEach(this::appendExtendedRecord);
  }

  /**
   * Converts {@link ExtendedRecord#getId} into id hash, appends AVRO file and counts the number of
   * records
   */
  private void appendExtendedRecord(ExtendedRecord record) {
    if (!record.getId().equals(ExtendedRecordConverter.getRecordIdError())) {
      dataFileWriter.append(record);
      counter.incrementAndGet();
    }
  }
}
