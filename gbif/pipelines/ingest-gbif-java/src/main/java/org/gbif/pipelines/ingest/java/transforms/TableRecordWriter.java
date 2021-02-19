package org.gbif.pipelines.ingest.java.transforms;

import static org.gbif.pipelines.core.utils.FsUtils.createParentDirectories;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.core.io.SyncDataFileWriterBuilder;
import org.gbif.pipelines.io.avro.BasicRecord;

@Builder
public class TableRecordWriter<T> {

  @NonNull private final InterpretationPipelineOptions options;
  @NonNull private final Collection<BasicRecord> basicRecords;
  @NonNull private final Function<BasicRecord, Optional<T>> recordFunction;
  @NonNull private final String targetTempPath;
  @NonNull private final Schema schema;
  @NonNull private final ExecutorService executor;

  @SneakyThrows
  public void write() {
    try (SyncDataFileWriter<T> writer = createWriter(options)) {
      boolean useSyncMode = options.getSyncThreshold() > basicRecords.size();
      if (useSyncMode) {
        syncWrite(writer);
      } else {
        asyncWrite(writer);
      }
    }
  }

  @SneakyThrows
  private void asyncWrite(SyncDataFileWriter<T> writer) {
    CompletableFuture<?>[] futures =
        basicRecords.stream()
            .map(
                br -> {
                  Optional<T> t = recordFunction.apply(br);
                  if (t.isPresent()) {
                    Runnable runnable = () -> writer.append(t.get());
                    return CompletableFuture.runAsync(runnable, executor);
                  }
                  return null;
                })
            .filter(Objects::nonNull)
            .toArray(CompletableFuture[]::new);
    // Wait for all futures
    CompletableFuture.allOf(futures).get();
  }

  private void syncWrite(SyncDataFileWriter<T> writer) {
    basicRecords.stream()
        .map(recordFunction)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(writer::append);
  }

  /** Create an AVRO file writer */
  @SneakyThrows
  private SyncDataFileWriter<T> createWriter(InterpretationPipelineOptions options) {
    Path path = new Path(targetTempPath);
    FileSystem verbatimFs =
        createParentDirectories(options.getHdfsSiteConfig(), options.getCoreSiteConfig(), path);
    return SyncDataFileWriterBuilder.builder()
        .schema(schema)
        .codec(options.getAvroCompressionType())
        .outputStream(verbatimFs.create(path))
        .syncInterval(options.getAvroSyncInterval())
        .build()
        .createSyncDataFileWriter();
  }
}
