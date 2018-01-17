package org.gbif.pipelines.common.beam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.pipelines.core.io.DwCAReader;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO operations for DwC-A formats.
 *
 * Provides the ability to read a DwC-A as a bounded source, but in a non splittable manner.  This means that a single
 * threaded approach to reading is enforced.
 *
 * This is intended only for demonstration usage, and not for production.
 *
 * To use this:
 * <pre>
 * {@code
 * p.apply("read",
 *     DwCAIO.Read.withPaths("/tmp/my-dwca.zip", "/tmp/working")
 *     ...;
 * }</pre>
 */
public class DwCAIO {
  private static final Logger LOG = LoggerFactory.getLogger(DwCAIO.class);

  public static class Read extends PTransform<PBegin, PCollection<ExtendedRecord>> {
    private final String path;
    private final String workingPath;

    public static Read withPaths(String file, String working) {
      return new Read(file, working);
    }

    private Read(String filePath, String workingPath) {
      path = filePath;
      this.workingPath = workingPath;
    }

    @Override
    public PCollection<ExtendedRecord> expand(PBegin input) {
      DwCASource source = new DwCASource(this);
      return input.getPipeline().apply(org.apache.beam.sdk.io.Read.from(source));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("DwC-A Path", path));
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
      return AvroCoder.of(ExtendedRecord.class);
    }

    /**
     * Will always return a single entry list of just ourselves. This is not splittable.
     */
    @Override
    public List<? extends BoundedSource<ExtendedRecord>> split(long desiredBundleSizeBytes,
                                                               PipelineOptions options) throws Exception {
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
      return new BoundedDwCAReader(this);
    }
  }

  /**
   * A wrapper around the standard DwC-IO provided NormalizedDwcArchive.
   */
  private static class BoundedDwCAReader extends BoundedSource.BoundedReader<ExtendedRecord> {
    private final DwCASource source;
    private DwCAReader reader;

    private BoundedDwCAReader(DwCASource source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      reader = new DwCAReader(source.read.path, source.read.workingPath);
      return reader.init();
    }

    @Override
    public boolean advance() throws IOException {
      return reader.advance();
    }

    @Override
    public ExtendedRecord getCurrent() throws NoSuchElementException {
      return reader.getCurrent();
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public BoundedSource<ExtendedRecord> getCurrentSource() {
      return source;
    }
  }
}
