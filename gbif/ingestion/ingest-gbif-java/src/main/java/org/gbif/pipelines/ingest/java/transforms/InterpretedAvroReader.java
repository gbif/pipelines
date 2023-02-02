package org.gbif.pipelines.ingest.java.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.ALL_AVRO;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.io.AvroReader;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.io.avro.Record;
import org.gbif.pipelines.transforms.Transform;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class InterpretedAvroReader {

  /** Read avro files and return as Map<ID, Clazz> */
  public static <T extends SpecificRecordBase & Record>
      CompletableFuture<Map<String, T>> readAvroAsFuture(
          InterpretationPipelineOptions options,
          DwcTerm coreTerm,
          ExecutorService executor,
          Transform<?, T> transform) {
    String path =
        PathBuilder.buildPathInterpretUsingInputPath(
            options, coreTerm, transform.getBaseName(), ALL_AVRO);
    return CompletableFuture.supplyAsync(
        () ->
            AvroReader.readRecords(
                HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()),
                transform.getReturnClazz(),
                path),
        executor);
  }

  /** Read avro files and return as Map<ID, Clazz> */
  public static <T extends SpecificRecordBase & Record> Map<String, T> readAvroUseTargetPath(
      InterpretationPipelineOptions options, DwcTerm coreTerm, Transform<?, T> transform) {
    return readAvroUseTargetPath(options, transform, coreTerm, transform.getBaseName());
  }

  /** Read avro files and return as Map<ID, Clazz> */
  public static <T extends SpecificRecordBase & Record> Map<String, T> readAvroUseTargetPath(
      InterpretationPipelineOptions options,
      Transform<?, T> transform,
      DwcTerm coreTerm,
      String dirName) {
    String path =
        PathBuilder.buildPathInterpretUsingTargetPath(options, coreTerm, dirName, ALL_AVRO);
    return AvroReader.readRecords(
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()),
        transform.getReturnClazz(),
        path);
  }
}
