package org.gbif.pipelines.core.io;

import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.NormalizedDwcArchive;
import org.gbif.dwca.io.Archive;
import org.gbif.dwca.record.StarRecord;
import org.gbif.pipelines.core.functions.Functions;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.utils.file.ClosableIterator;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class to simplify handling of DwC-A files using a local filesystem exposing data in Avro.
 */
public class DwCAReader {
  private static final Logger LOG = LoggerFactory.getLogger(DwCAReader.class);
  private String source;
  private String workingDir;
  private ClosableIterator<StarRecord> iter;
  private long recordsReturned;
  private ExtendedRecord current;

  public DwCAReader(String source, String workingDir) {
    this.source = source;
    this.workingDir = workingDir;
  }

  public boolean init() throws IOException {
    LOG.info("Opening DwC-A from[{}] with working directory[{}]", source, workingDir);
    Path extractToFolder = Paths.get(workingDir);
    Archive dwcArchive = DwcFiles.fromCompressed(Paths.get(source), extractToFolder);
    NormalizedDwcArchive nda = DwcFiles.prepareArchive(dwcArchive, false, false);
    iter = nda.iterator();
    return advance();
  }

  public boolean advance() throws IOException {
    if (!iter.hasNext()) {
      return false;
    }
    final StarRecord next = iter.next();
    recordsReturned++;
    if (recordsReturned % 1000 == 0) {
      LOG.info("Read [{}] records", recordsReturned);
    }
    current = Functions.extendedRecordBuilder().apply(next);
    return true;
  }

  public ExtendedRecord getCurrent() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException("No current record found (Hint: did you init() the reader?)");
    }
    return current;
  }

  public void close() throws IOException {
    if (iter!=null) {
      try {
        LOG.info("Closing DwC-A reader having read [{}] records", recordsReturned);
        iter.close();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }
}
