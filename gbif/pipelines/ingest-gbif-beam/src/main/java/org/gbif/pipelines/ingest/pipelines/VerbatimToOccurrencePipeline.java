package org.gbif.pipelines.ingest.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.ALL_AVRO;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.IDENTIFIER_ABSENT;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.ingest.pipelines.interpretation.TransformsFactory;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transforms.common.CheckTransforms;
import org.gbif.pipelines.transforms.common.UniqueGbifIdTransform;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.GrscicollTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.gbif.pipelines.transforms.specific.ClusteringTransform;
import org.gbif.pipelines.transforms.specific.GbifIdAbsentTransform;
import org.gbif.pipelines.transforms.specific.GbifIdTransform;
import org.slf4j.MDC;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads verbatim.avro file
 *    2) Interprets and converts avro {@link org.gbif.pipelines.io.avro.ExtendedRecord} file to:
 *      {@link org.gbif.pipelines.io.avro.MetadataRecord},
 *      {@link org.gbif.pipelines.io.avro.BasicRecord},
 *      {@link org.gbif.pipelines.io.avro.TemporalRecord},
 *      {@link org.gbif.pipelines.io.avro.MultimediaRecord},
 *      {@link org.gbif.pipelines.io.avro.ImageRecord},
 *      {@link org.gbif.pipelines.io.avro.AudubonRecord},
 *      {@link org.gbif.pipelines.io.avro.TaxonRecord},
 *      {@link org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord},
 *      {@link org.gbif.pipelines.io.avro.LocationRecord},
 *      ...etc
 *    3) Writes data to independent files
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -jar target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar some.properties
 *
 * or pass all parameters:
 *
 * java -jar target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar
 * --pipelineStep=VERBATIM_TO_INTERPRETED \
 * --properties=/some/path/to/output/ws.properties
 * --datasetId=0057a720-17c9-4658-971e-9578f3577cf5
 * --attempt=1
 * --interpretationTypes=ALL
 * --runner=SparkRunner
 * --targetPath=/some/path/to/output/
 * --inputPath=/some/path/to/output/0057a720-17c9-4658-971e-9578f3577cf5/1/verbatim.avro
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VerbatimToOccurrencePipeline {

  private static final DwcTerm CORE_TERM = DwcTerm.Occurrence;

  public static void main(String[] args) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) {
    run(options, Pipeline::create);
  }

  public static void run(
      InterpretationPipelineOptions options,
      Function<InterpretationPipelineOptions, Pipeline> pipelinesFn) {

    String datasetId = options.getDatasetId();
    Integer attempt = options.getAttempt();
    Set<String> types = options.getInterpretationTypes();
    String targetPath = options.getTargetPath();

    MDC.put("datasetKey", datasetId);
    MDC.put("attempt", attempt.toString());
    MDC.put("step", StepType.VERBATIM_TO_INTERPRETED.name());

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());
    TransformsFactory transformsFactory = TransformsFactory.create(options);

    // Remove directories with avro files for expected interpretation, except IDENTIFIER
    Set<String> deleteTypes = new HashSet<>(types);
    deleteTypes.remove(IDENTIFIER_ABSENT.name());
    FsUtils.deleteInterpretIfExist(
        hdfsConfigs, targetPath, datasetId, attempt, CORE_TERM, deleteTypes);

    // Path function for writing avro files
    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));
    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, CORE_TERM, t, id);

    // Path function for reading avro files
    UnaryOperator<String> interpretedPathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, CORE_TERM, t, ALL_AVRO);

    log.info("Creating pipeline transforms");
    MetadataTransform metadataTransform = transformsFactory.createMetadataTransform();
    VerbatimTransform verbatimTransform = transformsFactory.createVerbatimTransform();
    GbifIdAbsentTransform idAbsentTransform = transformsFactory.createGbifIdAbsentTransform();
    GbifIdTransform idTransform = transformsFactory.createGbifIdTransform();
    UniqueGbifIdTransform uniqueIdTransform = transformsFactory.createUniqueGbifIdTransform();
    ClusteringTransform clusteringTransform = transformsFactory.createClusteringTransform();
    BasicTransform basicTransform = transformsFactory.createBasicTransform();
    TemporalTransform temporalTransform = transformsFactory.createTemporalTransform();
    TaxonomyTransform taxonomyTransform = transformsFactory.createTaxonomyTransform();
    GrscicollTransform grscicollTransform = transformsFactory.createGrscicollTransform();
    LocationTransform locationTransform = transformsFactory.createLocationTransform();
    MultimediaTransform multimediaTransform = transformsFactory.createMultimediaTransform();
    AudubonTransform audubonTransform = transformsFactory.createAudubonTransform();
    ImageTransform imageTransform = transformsFactory.createImageTransform();

    log.info("Creating beam pipeline");
    Pipeline p = pipelinesFn.apply(options);

    // Create and write metadata
    PCollection<MetadataRecord> metadataRecord;
    if (useMetadataRecordWriteIO(types)) {
      metadataRecord =
          p.apply("Create metadata collection", Create.of(options.getDatasetId()))
              .apply("Interpret metadata", metadataTransform.interpret());

      metadataRecord.apply("Write metadata to avro", metadataTransform.write(pathFn));
    } else {
      metadataRecord = p.apply("Read metadata record", metadataTransform.read(interpretedPathFn));
    }

    // Create View for the further usage
    PCollectionView<MetadataRecord> metadataView =
        metadataRecord.apply("Convert into view", View.asSingleton());

    PCollection<ExtendedRecord> uniqueRecords;
    if (metadataTransform.metadataOnly(types)) {
      uniqueRecords = verbatimTransform.emptyCollection(p);
    } else if (useExtendedRecordWriteIO(types)) {
      uniqueRecords =
          p.apply("Read verbatim", verbatimTransform.read(options.getInputPath()))
              .apply(
                  "Read occurrences from extension",
                  transformsFactory.createOccurrenceExtensionTransform())
              .apply("Filter duplicates", transformsFactory.createUniqueIdTransform())
              .apply("Filter extensions", transformsFactory.createExtensionFilterTransform())
              .apply("Set default values", transformsFactory.createDefaultValuesTransform());
    } else {
      uniqueRecords =
          p.apply("Read filtered ExtendedRecords", verbatimTransform.read(interpretedPathFn));
    }

    // Read all dry-run GBIF IDs records
    PCollection<GbifIdRecord> uniqueGbifId;
    if (useGbifIdRecordWriteIO(types)) {
      PCollectionTuple idsTuple =
          uniqueRecords
              .apply("Lookup GBIF IDs records", idTransform.interpret())
              .apply("Filter unique GBIF ids", uniqueIdTransform);
      uniqueGbifId = idsTuple.get(uniqueIdTransform.getTag());
      uniqueGbifId.apply("Write GBIF IDs to avro", idTransform.write(pathFn));
      idsTuple
          .get(uniqueIdTransform.getInvalidTag())
          .apply("Write invalid GBIF IDs to avro", idTransform.writeInvalid(pathFn));
    } else if (useGbifIdReadIO(types)) {
      uniqueGbifId = p.apply("Read GBIF ids records", idTransform.read(interpretedPathFn));
    } else {
      uniqueGbifId = idAbsentTransform.emptyCollection(p);
    }

    // Read and interpret absent/new GBIF IDs records
    String identifierAbsentDir =
        PathBuilder.buildDatasetAttemptPath(
            options, CORE_TERM.simpleName() + "/" + idTransform.getAbsentName(), false);
    if (types.contains(IDENTIFIER_ABSENT.name())
        && FsUtils.fileExists(hdfsConfigs, identifierAbsentDir)) {
      PCollectionTuple absentTyple =
          p.apply(
                  "Read absent GBIF ids records",
                  idAbsentTransform.read(interpretedPathFn.apply(idTransform.getAbsentName())))
              .apply("Interpret absent GBIF ids", idAbsentTransform.interpret())
              .apply("Filter absent GBIF ids", uniqueIdTransform);

      PCollection<GbifIdRecord> absentCreatedGbifIds = absentTyple.get(uniqueIdTransform.getTag());

      absentCreatedGbifIds.apply("Write GBIF ids to avro", idTransform.write(pathFn));

      absentTyple
          .get(uniqueIdTransform.getInvalidTag())
          .apply("Write invalid GBIF ids to avro", idTransform.writeInvalid(pathFn));

      // Merge GBIF ids collections
      uniqueGbifId =
          PCollectionList.of(uniqueGbifId).and(absentCreatedGbifIds).apply(Flatten.pCollections());
    }

    PCollection<ExtendedRecord> filteredUniqueRecords = uniqueRecords;
    if (useExtendedRecordWriteIO(types)) {
      // Filter record with identical identifiers
      PCollection<KV<String, GbifIdRecord>> uniqueGbifIdRecordsKv =
          uniqueGbifId.apply("Map to GBIF ids record KV", idTransform.toKv());
      PCollection<KV<String, ExtendedRecord>> uniqueRecordsKv =
          uniqueRecords.apply("Map verbatim to KV", verbatimTransform.toKv());

      filteredUniqueRecords =
          KeyedPCollectionTuple
              // Core
              .of(verbatimTransform.getTag(), uniqueRecordsKv)
              .and(idTransform.getTag(), uniqueGbifIdRecordsKv)
              // Apply
              .apply("Grouping objects", CoGroupByKey.create())
              .apply(
                  "Filter verbatim",
                  transformsFactory.createFilterRecordsTransform(verbatimTransform, idTransform));
    }

    filteredUniqueRecords
        .apply("Check verbatim transform condition", verbatimTransform.check(types))
        .apply("Write verbatim to avro", verbatimTransform.write(pathFn));

    uniqueGbifId
        .apply(
            "Check clustering transform condition",
            clusteringTransform.check(types, GbifIdRecord.class))
        .apply("Interpret clustering", clusteringTransform.interpret())
        .apply("Write clustering to avro", clusteringTransform.write(pathFn));

    filteredUniqueRecords
        .apply("Check basic transform condition", basicTransform.check(types))
        .apply("Interpret basic", basicTransform.interpret())
        .apply("Write basic to avro", basicTransform.write(pathFn));

    filteredUniqueRecords
        .apply("Check temporal transform condition", temporalTransform.check(types))
        .apply("Interpret temporal", temporalTransform.interpret())
        .apply("Write temporal to avro", temporalTransform.write(pathFn));

    filteredUniqueRecords
        .apply("Check multimedia transform condition", multimediaTransform.check(types))
        .apply("Interpret multimedia", multimediaTransform.interpret())
        .apply("Write multimedia to avro", multimediaTransform.write(pathFn));

    filteredUniqueRecords
        .apply("Check image transform condition", imageTransform.check(types))
        .apply("Interpret image", imageTransform.interpret())
        .apply("Write image to avro", imageTransform.write(pathFn));

    filteredUniqueRecords
        .apply("Check audubon transform condition", audubonTransform.check(types))
        .apply("Interpret audubon", audubonTransform.interpret())
        .apply("Write audubon to avro", audubonTransform.write(pathFn));

    filteredUniqueRecords
        .apply("Check taxonomy transform condition", taxonomyTransform.check(types))
        .apply("Interpret taxonomy", taxonomyTransform.interpret())
        .apply("Write taxon to avro", taxonomyTransform.write(pathFn));

    filteredUniqueRecords
        .apply("Check grscicoll transform condition", grscicollTransform.check(types))
        .apply("Interpret grscicoll", grscicollTransform.interpret(metadataView))
        .apply("Write grscicoll to avro", grscicollTransform.write(pathFn));

    filteredUniqueRecords
        .apply("Check location transform condition", locationTransform.check(types))
        .apply("Interpret location", locationTransform.interpret(metadataView))
        .apply("Write location to avro", locationTransform.write(pathFn));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    log.info("Save metrics into the file and set files owner");
    String metadataPath =
        PathBuilder.buildDatasetAttemptPath(options, options.getMetaFileName(), false);
    if (!FsUtils.fileExists(hdfsConfigs, metadataPath)
        || CheckTransforms.checkRecordType(types, RecordType.IDENTIFIER)
        || CheckTransforms.checkRecordType(types, RecordType.IDENTIFIER_ABSENT)
        || CheckTransforms.checkRecordType(types, RecordType.ALL)) {
      MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());
      FsUtils.setOwnerToCrap(hdfsConfigs, metadataPath);
    }

    // Delete absent GBIF IDs directory
    FsUtils.deleteInterpretIfExist(
        hdfsConfigs, targetPath, datasetId, attempt, CORE_TERM, idTransform.getAbsentName());

    log.info("Deleting beam temporal folders");
    String tempPath = String.join("/", targetPath, datasetId, attempt.toString());
    FsUtils.deleteDirectoryByPrefix(hdfsConfigs, tempPath, ".temp-beam");

    log.info("Set interpreted files permissions");
    String interpretedPath =
        PathBuilder.buildDatasetAttemptPath(options, CORE_TERM.simpleName(), false);
    FsUtils.setOwnerToCrap(hdfsConfigs, interpretedPath);

    log.info("Pipeline has been finished");
  }

  private static boolean useGbifIdReadIO(Set<String> types) {
    return types.contains(RecordType.VERBATIM.name())
        || types.contains(RecordType.CLUSTERING.name())
        || types.contains(IDENTIFIER_ABSENT.name());
  }

  private static boolean useExtendedRecordWriteIO(Set<String> types) {
    return types.contains(RecordType.VERBATIM.name()) || types.contains(RecordType.ALL.name());
  }

  private static boolean useGbifIdRecordWriteIO(Set<String> types) {
    return types.contains(RecordType.IDENTIFIER.name()) || types.contains(RecordType.ALL.name());
  }

  private static boolean useMetadataRecordWriteIO(Set<String> types) {
    return types.contains(RecordType.METADATA.name()) || types.contains(RecordType.ALL.name());
  }
}
