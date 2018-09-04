package org.gbif.pipelines.base.pipeline;

import org.gbif.pipelines.base.options.DataPipelineOptionsFactory;
import org.gbif.pipelines.base.options.DataProcessingPipelineOptions;
import org.gbif.pipelines.base.transforms.CheckTransforms;
import org.gbif.pipelines.base.transforms.RecordTransforms;
import org.gbif.pipelines.base.transforms.UniqueIdTransform;
import org.gbif.pipelines.base.transforms.WriteTransforms;
import org.gbif.pipelines.base.utils.FsUtils;
import org.gbif.pipelines.core.RecordType;
import org.gbif.pipelines.io.avro.ExtendedRecord;

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
    Function<RecordType, String> pathFn = t -> FsUtils.buildPath(options, t);
    Function<RecordType, String> pathInterFn = t -> FsUtils.buildPathInterpret(options, t);

    LOG.info("Creating a pipeline from options");
    Pipeline p = Pipeline.create(options);

    LOG.info("Reading avro files");
    PCollection<ExtendedRecord> uniqueRecords =
        p.apply(AvroIO.read(ExtendedRecord.class).from(options.getInputPath()))
            .apply(UniqueIdTransform.create());

    LOG.info("Adding interpretations");

    p.apply(Create.of(options.getDatasetId()))
        .apply(CheckTransforms.metadata(types))
        .apply(RecordTransforms.metadata(wsProperties))
        .apply(WriteTransforms.metadata(pathFn.apply(METADATA)));

    uniqueRecords
        .apply(CheckTransforms.basic(types))
        .apply(RecordTransforms.basic())
        .apply(WriteTransforms.basic(pathInterFn.apply(BASIC)));

    uniqueRecords
        .apply(CheckTransforms.temporal(types))
        .apply(RecordTransforms.temporal())
        .apply(WriteTransforms.temporal(pathInterFn.apply(TEMPORAL)));

    uniqueRecords
        .apply(CheckTransforms.multimedia(types))
        .apply(RecordTransforms.multimedia())
        .apply(WriteTransforms.multimedia(pathInterFn.apply(MULTIMEDIA)));

    uniqueRecords
        .apply(CheckTransforms.taxon(types))
        .apply(RecordTransforms.taxonomy(wsProperties))
        .apply(WriteTransforms.taxon(pathInterFn.apply(TAXONOMY)));

    uniqueRecords
        .apply(CheckTransforms.location(types))
        .apply(RecordTransforms.location(wsProperties))
        .apply(WriteTransforms.location(pathInterFn.apply(LOCATION)));

    LOG.info("Running the pipeline");
    return p.run().waitUntilFinish();
  }
}
