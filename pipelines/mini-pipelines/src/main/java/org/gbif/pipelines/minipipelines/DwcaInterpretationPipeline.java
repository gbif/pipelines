package org.gbif.pipelines.minipipelines;

import org.gbif.pipelines.base.transforms.RecordTransforms;
import org.gbif.pipelines.base.transforms.UniqueIdTransform;
import org.gbif.pipelines.base.transforms.WriteTransforms;
import org.gbif.pipelines.base.utils.FsUtils;
import org.gbif.pipelines.common.beam.DwcaIO;
import org.gbif.pipelines.core.RecordType;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.parsers.ws.config.WsConfig;
import org.gbif.pipelines.parsers.ws.config.WsConfigFactory;

import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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

/** TODO: DOC! */
class DwcaInterpretationPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaInterpretationPipeline.class);

  private DwcaInterpretationPipeline() {}

  /** TODO: DOC! */
  static void createAndRun(DwcaPipelineOptions options) {

    WsConfig wsConfig = WsConfigFactory.create(options.getGbifApiUrl());
    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    Function<RecordType, String> metaPathFn = t -> FsUtils.buildPath(options, t.name());
    Function<RecordType, String> pathFn = t -> FsUtils.buildPathInterpret(options, t.name(), id);

    String inputPath = options.getInputPath();
    boolean isDirectory = Paths.get(inputPath).toFile().isDirectory();

    String tmpDir = FsUtils.getTempDir(options);

    DwcaIO.Read reader =
        isDirectory ? DwcaIO.Read.withPaths(inputPath) : DwcaIO.Read.withPaths(inputPath, tmpDir);

    LOG.info("Creating a pipeline from options");
    Pipeline p = Pipeline.create(options);

    LOG.info("Reading avro files");
    PCollection<ExtendedRecord> uniqueRecords =
        p.apply("Read ExtendedRecords", reader)
            .apply("Filter duplicates", UniqueIdTransform.create());

    LOG.info("Adding interpretations");

    p.apply("Create metadata collection", Create.of(options.getDatasetId()))
        .apply("Interpret metadata", RecordTransforms.metadata(wsConfig))
        .apply("Write metadata to avro", WriteTransforms.metadata(metaPathFn.apply(METADATA)));

    uniqueRecords
        .apply("Interpret basic", RecordTransforms.basic())
        .apply("Write basic to avro", WriteTransforms.basic(pathFn.apply(BASIC)));

    uniqueRecords
        .apply("Interpret temporal", RecordTransforms.temporal())
        .apply("Write temporal to avro", WriteTransforms.temporal(pathFn.apply(TEMPORAL)));

    uniqueRecords
        .apply("Interpret multimedia", RecordTransforms.multimedia())
        .apply("Write multimedia to avro", WriteTransforms.multimedia(pathFn.apply(MULTIMEDIA)));

    uniqueRecords
        .apply("Interpret taxonomy", RecordTransforms.taxonomy(wsConfig))
        .apply("Write taxon to avro", WriteTransforms.taxon(pathFn.apply(TAXONOMY)));

    uniqueRecords
        .apply("Interpret location", RecordTransforms.location(wsConfig))
        .apply("Write location to avro", WriteTransforms.location(pathFn.apply(LOCATION)));

    LOG.info("Running interpretation pipeline");
    p.run().waitUntilFinish();
    LOG.info("Interpretation pipeline has been finished");
  }
}
