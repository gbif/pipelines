package org.gbif.pipelines.core.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.Archive;
import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.record.StarRecord;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.converters.ExtendedRecordConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.utils.file.ClosableIterator;

/**
 * A utility class to simplify handling of DwC-A files using a local filesystem exposing data in
 * Avro.
 */
@Slf4j
public class DwcaExtendedRecordReader implements Closeable {

  private final Function<Object, ExtendedRecord> convertFn;
  private final ClosableIterator<?> iterator;

  @Getter private long recordsReturned;
  @Getter private long occurrenceRecordsReturned;
  private ExtendedRecord current;

  /** Creates a DwcaReader of an expanded archive. */
  public static DwcaExtendedRecordReader fromLocation(String path) throws IOException {
    return new DwcaExtendedRecordReader(DwcFiles.fromLocation(Paths.get(path)));
  }

  /**
   * Creates a DwcaReader for a compressed archive that it will be expanded in a working directory.
   */
  public static DwcaExtendedRecordReader fromCompressed(String source, String workingDir)
      throws IOException {
    return new DwcaExtendedRecordReader(
        DwcFiles.fromCompressed(Paths.get(source), Paths.get(workingDir)));
  }

  /** Creates and DwcaReader using a StarRecord iterator. */
  private DwcaExtendedRecordReader(Archive archive) {

    archive.getCore().getHeader().stream()
        .flatMap(Collection::stream)
        .forEach(
            x -> Objects.requireNonNull(x, "One of the terms is NULL, please check meta.xml file"));

    if (archive.getExtensions().isEmpty()) {
      this.iterator = archive.getCore().iterator();
      this.convertFn =
          dwcar -> ExtendedRecordConverter.from((Record) dwcar, Collections.emptyMap());
    } else {
      this.iterator = archive.iterator();
      this.convertFn =
          dwcar -> {
            StarRecord starRecord = (StarRecord) dwcar;
            return ExtendedRecordConverter.from(starRecord.core(), starRecord.extensions());
          };
    }
  }

  /** Has the archive more records?. */
  public boolean hasNext() {
    return iterator.hasNext();
  }

  /** Read next element. */
  public boolean advance() {
    if (!iterator.hasNext()) {
      return false;
    }
    recordsReturned++;

    current = convertFn.apply(iterator.next());

    if (current.getCoreRowType().equals(DwcTerm.Occurrence.qualifiedName())) {
      occurrenceRecordsReturned++;
    } else {
      Integer size =
          Optional.ofNullable(current.getExtensions().get(DwcTerm.Occurrence.qualifiedName()))
              .map(List::size)
              .orElse(0);
      occurrenceRecordsReturned += size;
    }

    if (recordsReturned % 100_000 == 0) {
      log.info(
          "Read [{}] records, occurrence records [{}]", recordsReturned, occurrenceRecordsReturned);
    }

    return true;
  }

  /** Gets the current extended record. */
  public ExtendedRecord getCurrent() {
    if (current == null) {
      throw new NoSuchElementException(
          "No current record found (Hint: did you init() the reader?)");
    }
    return current;
  }

  @Override
  public void close() throws IOException {
    if (iterator != null) {
      try {
        log.info(
            "Closing DwC-A reader having read [{}] records and [{}] occurrence records",
            recordsReturned,
            occurrenceRecordsReturned);
        iterator.close();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }
}
