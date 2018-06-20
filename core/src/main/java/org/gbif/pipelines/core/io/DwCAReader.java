package org.gbif.pipelines.core.io;

import org.gbif.dwc.Archive;
import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.record.StarRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transform.functions.FunctionFactory;
import org.gbif.utils.file.ClosableIterator;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class to simplify handling of DwC-A files using a local filesystem exposing data in
 * Avro.
 */
public class DwCAReader {

  private static final String UNCOMPRESSED = "UNCOMPRESSED";

  private static final Logger LOG = LoggerFactory.getLogger(DwCAReader.class);
  private final String source;
  private final String workingDir;
  private ClosableIterator<StarRecord> starRecordsIt;
  private long recordsReturned;
  private ExtendedRecord current;

  public DwCAReader(String workingDir) {
    this(UNCOMPRESSED, workingDir);
  }

  public DwCAReader(String source, String workingDir) {
    this.source = source;
    this.workingDir = workingDir;
  }

  public boolean init() throws IOException {
    LOG.info("Opening DwC-A from[{}] with working directory[{}]", source, workingDir);
    Path extractToFolder = Paths.get(workingDir);

    Archive dwcArchive;
    if (UNCOMPRESSED.equals(source)) {
      dwcArchive = DwcFiles.fromLocation(Paths.get(workingDir));
    } else {
      dwcArchive = DwcFiles.fromCompressed(Paths.get(source), extractToFolder);
    }

    starRecordsIt = dwcArchive.iterator();
    return advance();
  }

  public boolean advance() {
    if (!starRecordsIt.hasNext()) {
      return false;
    }
    StarRecord next = starRecordsIt.next();
    recordsReturned++;
    if (recordsReturned % 1000 == 0) {
      LOG.info("Read [{}] records", recordsReturned);
    }
    current = FunctionFactory.extendedRecordBuilder().apply(next);
    return true;
  }

  public ExtendedRecord getCurrent() {
    if (current == null) {
      throw new NoSuchElementException(
          "No current record found (Hint: did you init() the reader?)");
    }
    return current;
  }

  public void close() throws IOException {
    if (starRecordsIt == null) {
      return;
    }
    try {
      LOG.info("Closing DwC-A reader having read [{}] records", recordsReturned);
      starRecordsIt.close();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
