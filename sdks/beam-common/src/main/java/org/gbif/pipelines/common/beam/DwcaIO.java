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
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.pipelines.core.io.DwcaReader;

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
 * p.apply("read", DwcaIO.Read.fromCompressed("/tmp/my-dwca.zip", "/tmp/working")
 *     ...;
 * }</pre>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DwcaIO {

  public static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    private final Class<T> serializableClass;
    private final DwcaReader<T> reader;

    private Read(Class<T> serializableClass, DwcaReader<T> reader) {
      this.serializableClass = serializableClass;
      this.reader = reader;
    }

    public static <T> Read<T> create(Class<T> serializableClass, DwcaReader<T> reader) {
      return new Read<>(serializableClass, reader);
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      DwcaSource<T> source = new DwcaSource<>(serializableClass, reader);
      return input.getPipeline().apply(org.apache.beam.sdk.io.Read.from(source));
    }
  }

  /** A non-splittable bounded source. */
  @AllArgsConstructor(access = AccessLevel.PACKAGE)
  private static class DwcaSource<T> extends BoundedSource<T> {

    private final Class<T> clazz;
    private final DwcaReader<T> reader;

    @Override
    public Coder<T> getOutputCoder() {
      return AvroCoder.of(clazz);
    }

    /** Will always return a single entry list of just ourselves. This is not splittable. */
    @Override
    public List<? extends BoundedSource<T>> split(
        long desiredBundleSizeBytes, PipelineOptions options) {
      return Collections.singletonList(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {
      return 0; // unknown
    }

    @Override
    public BoundedReader<T> createReader(PipelineOptions options) {
      return new BoundedDwCAReader<>(this, reader);
    }
  }

  /** A wrapper around the standard DwC-IO provided NormalizedDwcArchive. */
  private static class BoundedDwCAReader<T> extends BoundedSource.BoundedReader<T> {

    private final Counter dwcaCount = Metrics.counter("DwcaIO", ARCHIVE_TO_ER_COUNT);

    private final DwcaSource<T> source;
    private final DwcaReader<T> reader;

    private BoundedDwCAReader(DwcaSource<T> source, DwcaReader<T> reader) {
      this.source = source;
      this.reader = reader;
    }

    @Override
    public boolean start() {
      return reader.advance();
    }

    @Override
    public boolean advance() {
      dwcaCount.inc();
      return reader.advance();
    }

    @Override
    public T getCurrent() {
      return reader.getCurrent();
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public BoundedSource<T> getCurrentSource() {
      return source;
    }
  }
}
