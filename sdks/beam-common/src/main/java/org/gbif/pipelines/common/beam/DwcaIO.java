package org.gbif.pipelines.common.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.ARCHIVE_TO_ER_COUNT;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.pipelines.core.io.DwcaReader;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * IO operations for DwC-A formats.
 *
 * <p>Provides the ability to read a DwC-A as a bounded source, but in a non splittable manner. This
 * means that a single threaded approach to reading is enforced.
 *
 * <p>This is intended only for demonstration usage, and not for production.
 *
 * <p>To use this:
 *
 * <pre>{@code
 * p.apply("read", DwcaIO.Read.fromCompressed("/tmp/my-dwca.zip", "/tmp/working")
 *     ...;
 * }</pre>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DwcaIO {

  public static class Read extends PTransform<PBegin, PCollection<ExtendedRecord>> {

    private final String path;
    private final String workingPath;
    private final boolean unCompressed;

    /**
     * Reads an expanded/uncompressed DwCA content
     *
     * @param working path to an expanded/uncompressed DwCA content
     */
    public static Read fromLocation(String working) {
      return new Read(working);
    }

    /**
     * Reads a DwCA archive and stores uncompressed DwCA content to a working directory
     *
     * @param file path to a DwCA archive
     * @param working path to a directory for storing uncompressed DwCA content
     */
    public static Read fromCompressed(String file, String working) {
      return new Read(file, working, false);
    }

    /**
     * Reads an expanded/uncompressed DwCA content
     *
     * @param workingPath path to an expanded/uncompressed DwCA content
     */
    private Read(String workingPath) {
      this(null, workingPath, true);
    }

    /**
     * Reads a DwCA archive and stores uncompressed DwCA content to a working directory
     *
     * @param filePath path to a DwCA archive
     * @param workingPath path to a directory for storing uncompressed DwCA content
     * @param unCompressed flag that determines if the source is compressed or expanded.
     */
    private Read(String filePath, String workingPath, boolean unCompressed) {
      path = filePath;
      this.workingPath = workingPath;
      this.unCompressed = unCompressed;
    }

    @Override
    public PCollection<ExtendedRecord> expand(PBegin input) {
      DwcaSource source = new DwcaSource(this);
      return input.getPipeline().apply(org.apache.beam.sdk.io.Read.from(source));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("DwC-A Path", path));
    }
  }

  /** A non-splittable bounded source. */
  @AllArgsConstructor(access = AccessLevel.PACKAGE)
  private static class DwcaSource extends BoundedSource<ExtendedRecord> {

    private final Read read;

    @Override
    public Coder<ExtendedRecord> getOutputCoder() {
      return AvroCoder.of(ExtendedRecord.class);
    }

    /** Will always return a single entry list of just ourselves. This is not splittable. */
    @Override
    public List<? extends BoundedSource<ExtendedRecord>> split(
        long desiredBundleSizeBytes, PipelineOptions options) {
      return Collections.singletonList(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {
      return 0; // unknown
    }

    @Override
    public BoundedReader<ExtendedRecord> createReader(PipelineOptions options) {
      return new BoundedDwCAReader(this);
    }
  }

  /** A wrapper around the standard DwC-IO provided NormalizedDwcArchive. */
  private static class BoundedDwCAReader extends BoundedSource.BoundedReader<ExtendedRecord> {

    private final Counter dwcaCount = Metrics.counter("DwcaIO", ARCHIVE_TO_ER_COUNT);

    private final DwcaSource source;
    private DwcaReader reader;

    private BoundedDwCAReader(DwcaSource source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      reader =
          source.read.unCompressed
              ? DwcaReader.fromLocation(source.read.workingPath)
              : DwcaReader.fromCompressed(source.read.path, source.read.workingPath);
      return reader.advance();
    }

    @Override
    public boolean advance() {
      dwcaCount.inc();
      return reader.advance();
    }

    @Override
    public ExtendedRecord getCurrent() {
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
