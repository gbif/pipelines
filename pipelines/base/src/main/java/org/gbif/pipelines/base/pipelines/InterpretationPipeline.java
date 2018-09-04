package org.gbif.pipelines.base.pipelines;

import org.gbif.pipelines.base.options.DataPipelineOptionsFactory;
import org.gbif.pipelines.base.options.DataProcessingPipelineOptions;
import org.gbif.pipelines.base.transforms.CheckTransforms;
import org.gbif.pipelines.base.transforms.RecordTransforms;
import org.gbif.pipelines.base.transforms.UniqueIdTransform;
import org.gbif.pipelines.base.transforms.WriteTransforms;
import org.gbif.pipelines.base.utils.FsUtils;
import org.gbif.pipelines.core.RecordType;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.function.Function;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
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

public class InterpretationPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(InterpretationPipeline.class);

  private final DataProcessingPipelineOptions options;

  private InterpretationPipeline(DataProcessingPipelineOptions options) {
    this.options = options;
  }

  public static InterpretationPipeline create(DataProcessingPipelineOptions options) {
    return new InterpretationPipeline(options);
  }

  public static void main(String[] args) {
    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(args);
    InterpretationPipeline.create(options).run();
  }

  public PipelineResult.State run() {

    List<String> types = options.getInterpretationTypes();
    String wsProperties = options.getWsProperties();
    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    Function<RecordType, String> metaPathFn = t -> FsUtils.buildPath(options, t.name());
    Function<RecordType, String> pathFn = t -> FsUtils.buildPathInterpret(options, t.name(), id);

    LOG.info("Creating a pipeline from options");
    Pipeline p = Pipeline.create(options);

    LOG.info("Reading avro files");
    PCollection<ExtendedRecord> uniqueRecords =
        p.apply(AvroIO.read(ExtendedRecord.class).from(options.getInputPath()))
            .apply(UniqueIdTransform.create());

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

    LOG.info("Running the pipeline");
    return p.run().waitUntilFinish();
  }
}
