package org.gbif.converters.parser.xml.parsing.extendedrecord;

import java.io.File;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.converters.parser.xml.OccurrenceParser;
import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;
import org.gbif.converters.parser.xml.parsing.validators.UniquenessValidator;
import org.gbif.converters.parser.xml.parsing.xml.XmlFragmentParser;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
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
            .flatMap(Collection::stream)
            .map(ExtendedRecordConverter::from)
            .filter(er -> validator.isUnique(er.getId()))
            .filter(er -> !er.getId().equals(ExtendedRecordConverter.getRecordIdError()))
            .forEach(
                er -> {
                  dataFileWriter.append(er);
                  counter.incrementAndGet();
                });
  }
}
