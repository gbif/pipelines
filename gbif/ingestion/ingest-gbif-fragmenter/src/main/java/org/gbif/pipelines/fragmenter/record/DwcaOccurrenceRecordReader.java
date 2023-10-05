package org.gbif.pipelines.fragmenter.record;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.Archive;
import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.record.StarRecord;
import org.gbif.utils.file.ClosableIterator;

/**
 * A utility class to simplify handling of DwC-A files using a local filesystem exposing data in
 * Avro.
 */
@Slf4j
public class DwcaOccurrenceRecordReader implements Closeable {

  private final Function<StarRecord, DwcaOccurrenceRecord> convertFn;
  private final ClosableIterator<StarRecord> iterator;

  @Getter private long recordsReturned;
  private DwcaOccurrenceRecord current;

  /** Creates a DwcaReader of a expanded archive. */
  public static DwcaOccurrenceRecordReader fromLocation(String path) throws IOException {
    return new DwcaOccurrenceRecordReader(DwcFiles.fromLocation(Paths.get(path)));
  }

  /** Creates and DwcaReader using a StarRecord iterator. */
  private DwcaOccurrenceRecordReader(Archive archive) {

    archive.getCore().getHeader().stream()
        .flatMap(Collection::stream)
        .forEach(
            x -> Objects.requireNonNull(x, "One of the terms is NULL, please check meta.xml file"));

    this.iterator = archive.iterator();
    this.convertFn = DwcaOccurrenceRecord::create;
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
  public DwcaOccurrenceRecord getCurrent() {
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
        log.info("Closing DwC-A reader having read [{}] records", recordsReturned);
        iterator.close();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }
}
