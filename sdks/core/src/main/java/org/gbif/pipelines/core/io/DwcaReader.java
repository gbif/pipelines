package org.gbif.pipelines.core.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.NoSuchElementException;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.DwcFiles;
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

  private final ClosableIterator<StarRecord> starRecordsIt;
  private long recordsReturned;
  private ExtendedRecord current;

  /** Creates a DwcaReader of a expanded archive. */
  public static DwcaReader fromLocation(String path) throws IOException {
    return new DwcaReader(DwcFiles.fromLocation(Paths.get(path)).iterator());
  }

  /**
   * Creates a DwcaReader for a compressed archive that it will be expanded in a working directory.
   */
  public static DwcaReader fromCompressed(String source, String workingDir) throws IOException {
    return new DwcaReader(
        DwcFiles.fromCompressed(Paths.get(source), Paths.get(workingDir)).iterator());
  }

  /** Creates and DwcaReader using a StarRecord iterator. */
  private DwcaReader(ClosableIterator<StarRecord> starRecordsIt) {
    this.starRecordsIt = starRecordsIt;
  }

  /** Has the archive more records?. */
  public boolean hasNext() {
    return starRecordsIt.hasNext();
  }

  /** Read next element. */
  public boolean advance() {
    if (!starRecordsIt.hasNext()) {
      return false;
    }
    StarRecord next = starRecordsIt.next();
    recordsReturned++;
    if (recordsReturned % 10_000 == 0) {
      log.info("Read [{}] records", recordsReturned);
    }
    current = ExtendedRecordConverter.from(next);
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
    if (starRecordsIt != null) {
      try {
        log.info("Closing DwC-A reader having read [{}] records", recordsReturned);
        starRecordsIt.close();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }
}
