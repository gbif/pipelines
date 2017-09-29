package org.gbif.data.pipelines.io.dwca;

import org.gbif.data.io.avro.ExtendedRecord;
import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.NormalizedDwcArchive;
import org.gbif.dwca.io.Archive;
import org.gbif.dwca.record.StarRecord;
import org.gbif.utils.file.ClosableIterator;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO operations for DwC-A formats.
 *
 * Provides the ability to read a DwC-A as a bounded source, but in a non splittable manner.  This means that a single
 * threaded approach to reading is enforced.
 *
 * To use this:
 * <pre>
 * {@code
 * p.apply("read",
 *     DwCAIO.Read.withPaths("/tmp/my-dwca.zip", "/tmp/working")
 *     ...;
 * }</pre>
 * @deprecated use the DwCAInputFormat instead.
 */
@Deprecated
public class DwCAIO {
  private static final Logger LOG = LoggerFactory.getLogger(DwCAIO.class);

  public static class Read extends PTransform<PBegin, PCollection<ExtendedRecord>> {
    private final String path;
    private final String workingPath;

    public static Read withPaths(Path file, Path working) {
      return new Read(file, working);
    }

    private Read(Path filePath, Path workingPath) {
      this.path = filePath.toString();
      this.workingPath = workingPath.toString();
    }

    @Override
    public PCollection<ExtendedRecord> expand(PBegin input) {
      DwCASource source = new DwCASource(this);
      return input.getPipeline().apply(org.apache.beam.sdk.io.Read.from(source));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("DwC-A Path", path.toString()));
    }
  }

  /**
   * A non-splittable bounded source.
   */
  private static class DwCASource extends BoundedSource<ExtendedRecord> {
    private final Read read;

    DwCASource(Read read) {
      this.read = read;
    }

    @Override
    public void validate() {
    }

    @Override
    public Coder<ExtendedRecord> getDefaultOutputCoder() {
      // TODO: use this? AvroCoder.of(ExtendedRecord.class)
      return new ExtendedRecordCoder();
    }

    /**
     * Will always return a single entry list of just ourselves. This is not splittable.
     */
    @Override
    public List<? extends BoundedSource<ExtendedRecord>> split(
      long desiredBundleSizeBytes, PipelineOptions options
    ) throws Exception {
      List<DwCASource> readers = new ArrayList<>();
      readers.add(this);
      return readers;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return 0; // unknown
    }

    @Override
    public BoundedReader<ExtendedRecord> createReader(PipelineOptions options) throws IOException {
      return new DwCAReader(this);
    }
  }

  /**
   * A wrapper around the standard DwC-IO provided NormalizedDwcArchive.
   */
  private static class DwCAReader extends BoundedSource.BoundedReader<ExtendedRecord> {
    private final DwCASource source;
    private ClosableIterator<StarRecord> iter;
    private long recordsReturned;
    private ExtendedRecord current;

    private DwCAReader(DwCASource source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      LOG.info("Opening DwC-A from[{}] with working directory[{}]", source.read.path, source.read.workingPath);
      Path extractToFolder = Paths.get(source.read.workingPath);
      Archive dwcArchive = DwcFiles.fromCompressed(Paths.get(source.read.path), extractToFolder);
      NormalizedDwcArchive nda = DwcFiles.prepareArchive(dwcArchive, false, false);
      iter = nda.iterator();
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (!iter.hasNext()) {
        return false;
      }
      final StarRecord next = iter.next();
      recordsReturned++;
      if (recordsReturned % 1000 == 0) {
        LOG.info("Read [{}] records", recordsReturned);
      }
      current = ExtendedRecords.newFromStarRecord(next);
      return true;
    }

    @Override
    public ExtendedRecord getCurrent() throws NoSuchElementException {

      return current;
    }

    @Override
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

    @Override
    public BoundedSource<ExtendedRecord> getCurrentSource() {
      return source;
    }
  }
}
