package org.gbif.pipelines.ingest.java.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;
import static org.gbif.pipelines.core.utils.FsUtils.createParentDirectories;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.core.io.SyncDataFileWriterBuilder;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;

@Builder
public class OccurrenceHdfsRecordWriter {

  @NonNull private final InterpretationPipelineOptions options;
  @NonNull private final Collection<BasicRecord> basicRecords;
  @NonNull private final Function<BasicRecord, OccurrenceHdfsRecord> occurrenceHdfsRecordFn;
  @NonNull private final ExecutorService executor;

  @SneakyThrows
  public void write() {
    try (SyncDataFileWriter<OccurrenceHdfsRecord> writer = createWriter(options)) {
      boolean useSyncMode = options.getSyncThreshold() > basicRecords.size();
      if (useSyncMode) {
        syncWrite(writer);
      } else {
        asyncWrite(writer);
      }
    }
  }

  @SneakyThrows
  private void asyncWrite(SyncDataFileWriter<OccurrenceHdfsRecord> writer) {
    CompletableFuture<?>[] futures =
        basicRecords.stream()
            .map(
                br ->
                    CompletableFuture.runAsync(
                        () -> writer.append(occurrenceHdfsRecordFn.apply(br)), executor))
            .toArray(CompletableFuture[]::new);
    // Wait for all futures
    CompletableFuture.allOf(futures).get();
  }

  private void syncWrite(SyncDataFileWriter<OccurrenceHdfsRecord> writer) {
    basicRecords.stream().map(occurrenceHdfsRecordFn).forEach(writer::append);
  }

  /** Create an AVRO file writer */
  @SneakyThrows
  private SyncDataFileWriter<OccurrenceHdfsRecord> createWriter(
      InterpretationPipelineOptions options) {
    String id = options.getDatasetId() + '_' + options.getAttempt();
    String targetTempPath =
        PathBuilder.buildFilePathHdfsViewUsingInputPath(options, id + AVRO_EXTENSION);
    Path path = new Path(targetTempPath);
    FileSystem verbatimFs =
        createParentDirectories(options.getHdfsSiteConfig(), options.getCoreSiteConfig(), path);
    return SyncDataFileWriterBuilder.builder()
        .schema(OccurrenceHdfsRecord.getClassSchema())
        .codec(options.getAvroCompressionType())
        .outputStream(verbatimFs.create(path))
        .syncInterval(options.getAvroSyncInterval())
        .build()
        .createSyncDataFileWriter();
  }
}
