package org.gbif.pipelines.core.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.Archive;
import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.record.StarRecord;
import org.gbif.pipelines.core.converters.ExtendedRecordConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.utils.file.ClosableIterator;

/**
 * A utility class to simplify handling of DwC-A files using a local filesystem exposing data in
 * Avro.
 */
@Slf4j
public class DwcaReader implements Closeable {

  private final Function<Object, ExtendedRecord> convertFn;
  private final ClosableIterator<?> iterator;
  private long recordsReturned;
  private ExtendedRecord current;

  /** Creates a DwcaReader of a expanded archive. */
  public static DwcaReader fromLocation(String path) throws IOException {
    return new DwcaReader(DwcFiles.fromLocation(Paths.get(path)));
  }

  /**
   * Creates a DwcaReader for a compressed archive that it will be expanded in a working directory.
   */
  public static DwcaReader fromCompressed(String source, String workingDir) throws IOException {
    return new DwcaReader(DwcFiles.fromCompressed(Paths.get(source), Paths.get(workingDir)));
  }

  /** Creates and DwcaReader using a StarRecord iterator. */
  private DwcaReader(Archive archive) {

    archive.getCore().getHeader().stream()
        .flatMap(Collection::stream)
        .forEach(
            x -> Objects.requireNonNull(x, "One of the terms is NULL, please check meta.xml file"));

    if (archive.getExtensions().isEmpty()) {
      this.iterator = archive.getCore().iterator();
      this.convertFn =
          record -> ExtendedRecordConverter.from((Record) record, Collections.emptyMap());
    } else {
      this.iterator = archive.iterator();
      this.convertFn =
          record -> {
            StarRecord starRecord = (StarRecord) record;
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
    if (recordsReturned % 100_000 == 0) {
      log.info("Read [{}] records", recordsReturned);
    }

    current = convertFn.apply(iterator.next());

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

  public long getRecordsReturned() {
    return recordsReturned;
  }

  @Override
  public void close() throws IOException {
    if (iterator != null) {
      try {
        log.info("Closing DwC-A reader having read [{}] records", recordsReturned);
        iterator.close();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }
}
