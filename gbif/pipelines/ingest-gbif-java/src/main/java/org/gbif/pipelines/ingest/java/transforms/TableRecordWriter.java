package org.gbif.pipelines.ingest.java.transforms;

import static org.gbif.pipelines.core.utils.FsUtils.createParentDirectories;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.InterpretationType;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.core.io.SyncDataFileWriterBuilder;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.transforms.common.CheckTransforms;

@Builder
public class TableRecordWriter<T> {

  @NonNull private final InterpretationPipelineOptions options;
  @NonNull private final Collection<IdentifierRecord> identifierRecords;
  @NonNull private final Function<IdentifierRecord, List<T>> recordFunction;
  @NonNull private final Function<InterpretationType, String> targetPathFn;
  @NonNull private final Schema schema;
  @NonNull private final ExecutorService executor;
  @NonNull private final Set<String> types;
  @NonNull private final InterpretationType recordType;

  @SneakyThrows
  public void write() {
    if (CheckTransforms.checkRecordType(types, recordType)) {
      try (SyncDataFileWriter<T> writer = createWriter(options)) {
        boolean useSyncMode = options.getSyncThreshold() > identifierRecords.size();
        if (useSyncMode) {
          syncWrite(writer);
        } else {
          CompletableFuture<?>[] futures = asyncWrite(writer);
          CompletableFuture.allOf(futures).get();
        }
      }
    }
  }

  private CompletableFuture<?>[] asyncWrite(SyncDataFileWriter<T> writer) {
    return identifierRecords.stream()
        .map(recordFunction)
        .flatMap(List::stream)
        .map(r -> CompletableFuture.runAsync(() -> writer.append(r), executor))
        .toArray(CompletableFuture[]::new);
  }

  private void syncWrite(SyncDataFileWriter<T> writer) {
    identifierRecords.stream().map(recordFunction).flatMap(List::stream).forEach(writer::append);
  }

  /** Create an AVRO file writer */
  @SneakyThrows
  private SyncDataFileWriter<T> createWriter(InterpretationPipelineOptions options) {
    Path path = new Path(targetPathFn.apply(recordType));
    FileSystem verbatimFs =
        createParentDirectories(
            HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()), path);
    return SyncDataFileWriterBuilder.builder()
        .schema(schema)
        .codec(options.getAvroCompressionType())
        .outputStream(verbatimFs.create(path))
        .syncInterval(options.getAvroSyncInterval())
        .build()
        .createSyncDataFileWriter();
  }
}
