package org.gbif.pipelines.ingest.pipelines;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.ingest.utils.MetricsHandler;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.CheckTransforms;
import org.gbif.pipelines.transforms.ReadTransforms;
import org.gbif.pipelines.transforms.UniqueIdTransform;
import org.gbif.pipelines.transforms.WriteTransforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.BASIC;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.LOCATION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.METADATA;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.MULTIMEDIA;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.TAXONOMY;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.TEMPORAL;
import static org.gbif.pipelines.transforms.RecordTransforms.BasicFn;
import static org.gbif.pipelines.transforms.RecordTransforms.LocationFn;
import static org.gbif.pipelines.transforms.RecordTransforms.MetadataFn;
import static org.gbif.pipelines.transforms.RecordTransforms.MultimediaFn;
import static org.gbif.pipelines.transforms.RecordTransforms.TaxonomyFn;
import static org.gbif.pipelines.transforms.RecordTransforms.TemporalFn;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads verbatim.avro file
 *    2) Interprets and converts avro {@link org.gbif.pipelines.io.avro.ExtendedRecord} file
 *        to {@link org.gbif.pipelines.io.avro.MetadataRecord}, {@link
 *        org.gbif.pipelines.io.avro.BasicRecord}, {@link org.gbif.pipelines.io.avro.TemporalRecord},
 *        {@link org.gbif.pipelines.io.avro.MultimediaRecord}, {@link
 *        org.gbif.pipelines.io.avro.TaxonRecord}, {@link org.gbif.pipelines.io.avro.LocationRecord}
 *    3) Writes data to independent files
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -cp target/ingest-gbif-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.pipelines.VerbatimToInterpretedPipeline some.properties
 *
 * or pass all parameters:
 *
 * java -cp target/ingest-gbif-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.pipelines.VerbatimToInterpretedPipeline
 * --wsProperties=/some/path/to/output/ws.properties
 * --datasetId=0057a720-17c9-4658-971e-9578f3577cf5
 * --attempt=1
 * --interpretationTypes=ALL
 * --runner=SparkRunner
 * --targetPath=/some/path/to/output/
 * --inputPath=/some/path/to/output/0057a720-17c9-4658-971e-9578f3577cf5/1/verbatim.avro
 *
 * }</pre>
 */
public class VerbatimToInterpretedPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(VerbatimToInterpretedPipeline.class);

  private VerbatimToInterpretedPipeline() {}

  public static void main(String[] args) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) {

    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());

    List<String> types = options.getInterpretationTypes();
    String wsProperties = options.getWsProperties();
    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    Function<RecordType, String> pathFn = t -> FsUtils.buildPathInterpret(options, t.name(), id);

    LOG.info("Creating a pipeline from options");
    Pipeline p = Pipeline.create(options);

    LOG.info("Reading avro files");
    PCollection<ExtendedRecord> uniqueRecords =
        p.apply("Read ExtendedRecords", ReadTransforms.extended(options.getInputPath()))
            .apply("Filter duplicates", UniqueIdTransform.create());

    LOG.info("Adding interpretations");

    p.apply("Create metadata collection", Create.of(options.getDatasetId()))
        .apply("Check metadata transform condition", CheckTransforms.metadata(types))
        .apply("Interpret metadata", ParDo.of(new MetadataFn(wsProperties)))
        .apply("Write metadata to avro", WriteTransforms.metadata(pathFn.apply(METADATA)));

    uniqueRecords
        .apply("Check basic transform condition", CheckTransforms.basic(types))
        .apply("Interpret basic", ParDo.of(new BasicFn()))
        .apply("Write basic to avro", WriteTransforms.basic(pathFn.apply(BASIC)));

    uniqueRecords
        .apply("Check temporal transform condition", CheckTransforms.temporal(types))
        .apply("Interpret temporal", ParDo.of(new TemporalFn()))
        .apply("Write temporal to avro", WriteTransforms.temporal(pathFn.apply(TEMPORAL)));

    uniqueRecords
        .apply("Check multimedia transform condition", CheckTransforms.multimedia(types))
        .apply("Interpret multimedia", ParDo.of(new MultimediaFn()))
        .apply("Write multimedia to avro", WriteTransforms.multimedia(pathFn.apply(MULTIMEDIA)));

    uniqueRecords
        .apply("Check taxonomy transform condition", CheckTransforms.taxon(types))
        .apply("Interpret taxonomy", ParDo.of(new TaxonomyFn(wsProperties)))
        .apply("Write taxon to avro", WriteTransforms.taxon(pathFn.apply(TAXONOMY)));

    uniqueRecords
        .apply("Check location transform condition", CheckTransforms.location(types))
        .apply("Interpret location", ParDo.of(new LocationFn(wsProperties)))
        .apply("Write location to avro", WriteTransforms.location(pathFn.apply(LOCATION)));

    LOG.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    Optional.ofNullable(options.getMetaFileName()).ifPresent(metadataName -> {
      String metadataPath = metadataName.isEmpty() ? "" : FsUtils.buildPath(options, metadataName);
      MetricsHandler.saveCountersToFile(options.getHdfsSiteConfig(), metadataPath, result);
    });

    LOG.info("Pipeline has been finished");
  }
}
