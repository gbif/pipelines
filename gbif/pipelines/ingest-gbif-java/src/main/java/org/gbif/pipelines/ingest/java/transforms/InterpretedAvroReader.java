package org.gbif.pipelines.ingest.java.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.io.AvroReader;
import org.gbif.pipelines.io.avro.Record;
import org.gbif.pipelines.transforms.Transform;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class InterpretedAvroReader {

  /** Read avro files and return as Map<ID, Clazz> */
  public static <T extends SpecificRecordBase & Record>
      CompletableFuture<Map<String, T>> readAvroAsFuture(
          InterpretationPipelineOptions options,
          ExecutorService executor,
          Transform<?, T> transform) {
    String path =
        PathBuilder.buildPathInterpretUsingInputPath(
            options, transform.getBaseName(), "*" + AVRO_EXTENSION);
    return CompletableFuture.supplyAsync(
        () ->
            AvroReader.readRecords(
                options.getHdfsSiteConfig(),
                options.getCoreSiteConfig(),
                transform.getReturnClazz(),
                path),
        executor);
  }

  /** Read avro files and return as Map<ID, Clazz> */
  public static <T extends SpecificRecordBase & Record> Map<String, T> readAvroUseTargetPath(
      InterpretationPipelineOptions options, Transform<?, T> transform) {
    String path =
        PathBuilder.buildPathInterpretUsingTargetPath(
            options, transform.getBaseName(), "*" + AVRO_EXTENSION);
    return AvroReader.readRecords(
        options.getHdfsSiteConfig(), options.getCoreSiteConfig(), transform.getReturnClazz(), path);
  }
}
