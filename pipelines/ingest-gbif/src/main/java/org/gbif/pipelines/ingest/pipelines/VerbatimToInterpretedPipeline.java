package org.gbif.pipelines.ingest.pipelines;

import org.gbif.pipelines.core.RecordType;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.CheckTransforms;
import org.gbif.pipelines.transforms.ReadTransforms;
import org.gbif.pipelines.transforms.RecordTransforms;
import org.gbif.pipelines.transforms.UniqueIdTransform;
import org.gbif.pipelines.transforms.WriteTransforms;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.function.Function;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.pipelines.core.RecordType.BASIC;
import static org.gbif.pipelines.core.RecordType.LOCATION;
import static org.gbif.pipelines.core.RecordType.METADATA;
import static org.gbif.pipelines.core.RecordType.MULTIMEDIA;
import static org.gbif.pipelines.core.RecordType.TAXONOMY;
import static org.gbif.pipelines.core.RecordType.TEMPORAL;

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
    createAndRun(options);
  }

  public static void createAndRun(InterpretationPipelineOptions options) {
    LOG.info("Running interpretation pipeline");
    VerbatimToInterpretedPipeline.create(options).run().waitUntilFinish();
    LOG.info("Interpretation pipeline has been finished");
  }

  public static Pipeline create(InterpretationPipelineOptions options) {

    List<String> types = options.getInterpretationTypes();
    String wsProperties = options.getWsProperties();
    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    Function<RecordType, String> metaPathFn = t -> FsUtils.buildPath(options, t.name());
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
        .apply("Interpret metadata", RecordTransforms.metadata(wsProperties))
        .apply("Write metadata to avro", WriteTransforms.metadata(metaPathFn.apply(METADATA)));

    uniqueRecords
        .apply("Check basic transform condition", CheckTransforms.basic(types))
        .apply("Interpret basic", RecordTransforms.basic())
        .apply("Write basic to avro", WriteTransforms.basic(pathFn.apply(BASIC)));

    uniqueRecords
        .apply("Check temporal transform condition", CheckTransforms.temporal(types))
        .apply("Interpret temporal", RecordTransforms.temporal())
        .apply("Write temporal to avro", WriteTransforms.temporal(pathFn.apply(TEMPORAL)));

    uniqueRecords
        .apply("Check multimedia transform condition", CheckTransforms.multimedia(types))
        .apply("Interpret multimedia", RecordTransforms.multimedia())
        .apply("Write multimedia to avro", WriteTransforms.multimedia(pathFn.apply(MULTIMEDIA)));

    uniqueRecords
        .apply("Check taxonomy transform condition", CheckTransforms.taxon(types))
        .apply("Interpret taxonomy", RecordTransforms.taxonomy(wsProperties))
        .apply("Write taxon to avro", WriteTransforms.taxon(pathFn.apply(TAXONOMY)));

    uniqueRecords
        .apply("Check location transform condition", CheckTransforms.location(types))
        .apply("Interpret location", RecordTransforms.location(wsProperties))
        .apply("Write location to avro", WriteTransforms.location(pathFn.apply(LOCATION)));

    return p;
  }
}
