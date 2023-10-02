package org.gbif.pipelines.core.io;

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
import org.gbif.utils.file.ClosableIterator;

/**
 * A utility class to simplify handling of DwC-A files using a local filesystem exposing data in
 * Avro.
 */
@Slf4j
public class DwcaReader<T> implements Closeable {

  private final Function<Object, T> convertFn;
  private final ClosableIterator<?> iterator;
  @Getter private long recordsReturned;
  private T current;

  /** Creates a DwcaReader of an expanded archive. */
  public static <T> DwcaReader<T> fromLocation(
      String path, Function<Object, T> convertFn, Function<Object, T> convertWithExtFn)
      throws IOException {
    Archive archive = DwcFiles.fromLocation(Paths.get(path));
    return new DwcaReader<>(archive, convertFn, convertWithExtFn);
  }

  /**
   * Creates a DwcaReader for a compressed archive that it will be expanded in a working directory.
   */
  public static <T> DwcaReader<T> fromCompressed(
      String source,
      String workingDir,
      Function<Object, T> convertFn,
      Function<Object, T> convertWithExtFn)
      throws IOException {
    Archive archive = DwcFiles.fromCompressed(Paths.get(source), Paths.get(workingDir));
    return new DwcaReader<>(archive, convertFn, convertWithExtFn);
  }

  /** Creates and DwcaReader using a StarRecord iterator. */
  private DwcaReader(
      Archive archive, Function<Object, T> convertFn, Function<Object, T> convertWithExtFn) {

    archive.getCore().getHeader().stream()
        .flatMap(Collection::stream)
        .forEach(
            x -> Objects.requireNonNull(x, "One of the terms is NULL, please check meta.xml file"));

    if (archive.getExtensions().isEmpty()) {
      this.iterator = archive.getCore().iterator();
      this.convertFn = convertFn;
    } else {
      this.iterator = archive.iterator();
      this.convertFn = convertWithExtFn;
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
  public T getCurrent() {
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
