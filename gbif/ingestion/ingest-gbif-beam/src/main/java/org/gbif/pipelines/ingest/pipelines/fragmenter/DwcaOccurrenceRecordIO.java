package org.gbif.pipelines.ingest.pipelines.fragmenter;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.ARCHIVE_TO_ER_COUNT;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.pipelines.fragmenter.record.DwcaOccurrenceRecord;
import org.gbif.pipelines.fragmenter.record.DwcaOccurrenceRecordReader;

/**
 * IO operations for DwC-A formats.
 *
 * <p>Provides the ability to read a DwC-A as a bounded source, but in a non-splittable manner. This
 * means that a single threaded approach to reading is enforced.
 *
 * <p>This is intended only for demonstration usage, and not for production.
 *
 * <p>To use this:
 *
 * <pre>{@code
 * p.apply("read", DwcaIO.Read.fromLocation("/dwca/working")
 *     ...;
 * }</pre>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DwcaOccurrenceRecordIO {

  public static class Read extends PTransform<PBegin, PCollection<DwcaOccurrenceRecord>> {

    private final String workingPath;

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
     * @param workingPath path to a directory for storing uncompressed DwCA content
     */
    private Read(String workingPath) {
      this.workingPath = workingPath;
    }

    @Override
    public PCollection<DwcaOccurrenceRecord> expand(PBegin input) {
      DwcaSource source = new DwcaSource(this);
      return input.getPipeline().apply(org.apache.beam.sdk.io.Read.from(source));
    }
  }

  /** A non-splittable bounded source. */
  @AllArgsConstructor(access = AccessLevel.PACKAGE)
  private static class DwcaSource extends BoundedSource<DwcaOccurrenceRecord> {

    private final Read read;

    @Override
    public Coder<DwcaOccurrenceRecord> getOutputCoder() {
      return DwcaOccurrenceRecordCoder.of();
    }

    /** Will always return a single entry list of just ourselves. This is not splittable. */
    @Override
    public List<? extends BoundedSource<DwcaOccurrenceRecord>> split(
        long desiredBundleSizeBytes, PipelineOptions options) {
      return Collections.singletonList(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {
      return 0; // unknown
    }

    @Override
    public BoundedReader<DwcaOccurrenceRecord> createReader(PipelineOptions options) {
      return new BoundedDwCAReader(this);
    }
  }

  /** A wrapper around the standard DwC-IO provided NormalizedDwcArchive. */
  private static class BoundedDwCAReader extends BoundedSource.BoundedReader<DwcaOccurrenceRecord> {

    private final Counter dwcaCount = Metrics.counter("DwcaIO", ARCHIVE_TO_ER_COUNT);

    private final DwcaSource source;
    private DwcaOccurrenceRecordReader reader;

    private BoundedDwCAReader(DwcaSource source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      reader = DwcaOccurrenceRecordReader.fromLocation(source.read.workingPath);
      return reader.advance();
    }

    @Override
    public boolean advance() {
      dwcaCount.inc();
      return reader.advance();
    }

    @Override
    public DwcaOccurrenceRecord getCurrent() {
      return reader.getCurrent();
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public BoundedSource<DwcaOccurrenceRecord> getCurrentSource() {
      return source;
    }
  }
}
