package org.gbif.pipelines.assembling.interpretation;

import org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier;
import org.gbif.pipelines.assembling.interpretation.steps.PipelineTargetPaths;
import org.gbif.pipelines.assembling.utils.FsUtils;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.config.InterpretationType;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transform.validator.UniqueOccurrenceIdTransform;

import java.util.EnumMap;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

import static org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier.commonGbif;
import static org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier.locationGbif;
import static org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier.multimediaGbif;
import static org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier.taxonomyGbif;
import static org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier.temporalGbif;
import static org.gbif.pipelines.config.InterpretationType.COMMON;
import static org.gbif.pipelines.config.InterpretationType.LOCATION;
import static org.gbif.pipelines.config.InterpretationType.MULTIMEDIA;
import static org.gbif.pipelines.config.InterpretationType.TAXONOMY;
import static org.gbif.pipelines.config.InterpretationType.TEMPORAL;

/**
 * Gbif implementation for a pipeline.
 */
public class GbifInterpretationPipeline implements Supplier<Pipeline> {

  private static final String DATA_FILENAME = "interpreted";
  private static final String ISSUES_FOLDER = "issues";
  private static final String ISSUES_FILENAME = "issues";

  // avro codecs
  private static final String DEFLATE = "deflate";
  private static final String SNAPPY = "snappy";
  private static final String BZIP2 = "bzip2";
  private static final String XZ = "xz";
  private static final String NULL = "null";
  private static final String CODEC_SEPARATOR = "_";

  private final EnumMap<InterpretationType, InterpretationStepSupplier> stepsMap =
    new EnumMap<>(InterpretationType.class);
  private final DataProcessingPipelineOptions options;
  private final CodecFactory avroCodec;
  private final String wsProperties;

  private GbifInterpretationPipeline(DataProcessingPipelineOptions options) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(options.getDatasetId()), "datasetId is required");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(options.getDefaultTargetDirectory()),
                                "defaultTargetDirectory is required");
    Preconditions.checkArgument(Objects.nonNull(options.getAttempt()), "attempt is required");
    this.options = options;
    avroCodec = parseAvroCodec(options.getAvroCompressionType());
    wsProperties = options.getWsProperties();
    initStepsMap();
  }

  /**
   * Creates a new {@link GbifInterpretationPipeline} instance from the {@link DataProcessingPipelineOptions} received.
   */
  public static GbifInterpretationPipeline create(DataProcessingPipelineOptions options) {
    return new GbifInterpretationPipeline(options);
  }

  @Override
  public Pipeline get() {
    return InterpretationPipelineAssembler.of(options.getInterpretationTypes())
      .withOptions(options)
      .withInput(options.getInputFile())
      .using(stepsMap)
      .onBeforeInterpretations(createBeforeStep())
      .assemble();
  }

  private void initStepsMap() {
    stepsMap.put(LOCATION, locationGbif(createPaths(options, InterpretationType.LOCATION), avroCodec, wsProperties));
    stepsMap.put(TEMPORAL, temporalGbif(createPaths(options, InterpretationType.TEMPORAL), avroCodec));
    stepsMap.put(TAXONOMY, taxonomyGbif(createPaths(options, InterpretationType.TAXONOMY), avroCodec, wsProperties));
    stepsMap.put(COMMON, commonGbif(createPaths(options, InterpretationType.COMMON), avroCodec));
    stepsMap.put(MULTIMEDIA, multimediaGbif(createPaths(options, InterpretationType.MULTIMEDIA), avroCodec));
  }


  private BiFunction<PCollection<ExtendedRecord>, Pipeline, PCollection<ExtendedRecord>> createBeforeStep() {
    return (PCollection<ExtendedRecord> verbatimRecords, Pipeline pipeline) -> {
      UniqueOccurrenceIdTransform uniquenessTransform = UniqueOccurrenceIdTransform.create().withAvroCoders(pipeline);
      PCollectionTuple uniqueTuple = verbatimRecords.apply(uniquenessTransform);
      return uniqueTuple.get(uniquenessTransform.getDataTag());
    };
  }

  @VisibleForTesting
  static PipelineTargetPaths createPaths(DataProcessingPipelineOptions options, InterpretationType interType) {
    PipelineTargetPaths paths = new PipelineTargetPaths();

    String targetDirectory = options.getDefaultTargetDirectory();
    String datasetId = options.getDatasetId();
    String attempt = options.getAttempt().toString();
    String type = interType.name().toLowerCase();

    String path = FsUtils.buildPathString(targetDirectory, datasetId, attempt, type);

    paths.setDataTargetPath(FsUtils.buildPathString(path, DATA_FILENAME));
    paths.setIssuesTargetPath(FsUtils.buildPathString(path, ISSUES_FOLDER, ISSUES_FILENAME));

    paths.setTempDir(options.getHdfsTempLocation());

    return paths;
  }

  private static CodecFactory parseAvroCodec(String codec) {
    if (Strings.isNullOrEmpty(codec) || NULL.equalsIgnoreCase(codec)) {
      return CodecFactory.nullCodec();
    }

    if (SNAPPY.equalsIgnoreCase(codec)) {
      return CodecFactory.snappyCodec();
    }

    if (BZIP2.equalsIgnoreCase(codec)) {
      return CodecFactory.bzip2Codec();
    }

    if (codec.toLowerCase().startsWith(DEFLATE)) {
      List<String> pieces = Splitter.on(CODEC_SEPARATOR).splitToList(codec);
      int compressionLevel = CodecFactory.DEFAULT_DEFLATE_LEVEL;
      if (pieces.size() > 1) {
        compressionLevel = Integer.parseInt(pieces.get(1));
      }
      return CodecFactory.deflateCodec(compressionLevel);
    }

    if (codec.toLowerCase().startsWith(XZ)) {
      List<String> pieces = Splitter.on(CODEC_SEPARATOR).splitToList(codec);
      int compressionLevel = CodecFactory.DEFAULT_XZ_LEVEL;
      if (pieces.size() > 1) {
        compressionLevel = Integer.parseInt(pieces.get(1));
      }
      return CodecFactory.xzCodec(compressionLevel);
    }

    throw new IllegalArgumentException("CodecFactory not found for codec " + codec);
  }

}
