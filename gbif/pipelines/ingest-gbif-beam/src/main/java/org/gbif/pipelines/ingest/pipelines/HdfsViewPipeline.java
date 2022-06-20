package org.gbif.pipelines.ingest.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.ALL_AVRO;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.AMPLIFICATION_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.CHRONOMETRIC_AGE_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.CLONING_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.EXTENDED_MEASUREMENT_OR_FACT_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.GEL_IMAGE_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.GERMPLASM_ACCESSION_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.GERMPLASM_MEASUREMENT_SCORE_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.GERMPLASM_MEASUREMENT_TRAIT_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.GERMPLASM_MEASUREMENT_TRIAL_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.IDENTIFICATION_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.IDENTIFIER_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.LOAN_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.MATERIAL_SAMPLE_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.MEASUREMENT_OR_FACT_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.PERMIT_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.PREPARATION_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.PRESERVATION_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.REFERENCE_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.RESOURCE_RELATIONSHIP_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.getAllTables;

import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.core.utils.HdfsViewUtils;
import org.gbif.pipelines.ingest.utils.HdfsViewAvroUtils;
import org.gbif.pipelines.ingest.utils.SharedLockUtils;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.EventCoreTransform;
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
import org.gbif.pipelines.transforms.specific.GbifIdTransform;
import org.gbif.pipelines.transforms.table.AmplificationTableTransform;
import org.gbif.pipelines.transforms.table.ChronometricAgeTableTransform;
import org.gbif.pipelines.transforms.table.CloningTableTransform;
import org.gbif.pipelines.transforms.table.ExtendedMeasurementOrFactTableTransform;
import org.gbif.pipelines.transforms.table.GelImageTableTransform;
import org.gbif.pipelines.transforms.table.GermplasmAccessionTableTransform;
import org.gbif.pipelines.transforms.table.GermplasmMeasurementScoreTableTransform;
import org.gbif.pipelines.transforms.table.GermplasmMeasurementTraitTableTransform;
import org.gbif.pipelines.transforms.table.GermplasmMeasurementTrialTableTransform;
import org.gbif.pipelines.transforms.table.IdentificationTableTransform;
import org.gbif.pipelines.transforms.table.IdentifierTableTransform;
import org.gbif.pipelines.transforms.table.LoanTableTransform;
import org.gbif.pipelines.transforms.table.MaterialSampleTableTransform;
import org.gbif.pipelines.transforms.table.MeasurementOrFactTableTransform;
import org.gbif.pipelines.transforms.table.OccurrenceHdfsRecordTransform;
import org.gbif.pipelines.transforms.table.PermitTableTransform;
import org.gbif.pipelines.transforms.table.PreparationTableTransform;
import org.gbif.pipelines.transforms.table.PreservationTableTransform;
import org.gbif.pipelines.transforms.table.ReferenceTableTransform;
import org.gbif.pipelines.transforms.table.ResourceRelationshipTableTransform;
import org.gbif.wrangler.lock.Mutex;
import org.slf4j.MDC;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads avro files:
 *      {@link MetadataRecord},
 *      {@link BasicRecord},
 *      {@link TemporalRecord},
 *      {@link MultimediaRecord},
 *      {@link ImageRecord},
 *      {@link AudubonRecord},
 *      {@link TaxonRecord},
 *      {@link GrscicollRecord},
 *      {@link LocationRecord},
 *      and etc
 *    2) Joins avro files
 *    3) Converts to a {@link OccurrenceHdfsRecord} and table based on the input files
 *    4) Moves the produced files to a directory where the latest version of HDFS records are kept
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -jar target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar
 *
 * or pass all parameters:
 *
 * java -jar target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar
 * --pipelineStep=INTERPRETED_TO_HDFS \
 * --datasetId=4725681f-06af-4b1e-8fff-e31e266e0a8f \
 * --attempt=1 \
 * --runner=SparkRunner \
 * --inputPath=/path \
 * --targetPath=/path \
 * --properties=/path/pipelines.properties
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HdfsViewPipeline {

  public static void run(InterpretationPipelineOptions options, RecordType recordType) {
    run(options, Pipeline::create, recordType);
  }

  public static void run(
      InterpretationPipelineOptions options,
      Function<InterpretationPipelineOptions, Pipeline> pipelinesFn,
      RecordType recordType) {

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());
    String datasetId = options.getDatasetId();
    Integer attempt = options.getAttempt();
    Integer numberOfShards = options.getNumberOfShards();
    Set<String> types = options.getInterpretationTypes();
    DwcTerm coreTerm = HdfsViewUtils.getCoreTerm(recordType);

    SerializableFunction<RecordType, String> pathFn =
        st ->
            PathBuilder.buildFilePathViewUsingInputPath(
                options, recordType, st.name().toLowerCase(), datasetId + '_' + attempt);

    MDC.put("datasetKey", datasetId);
    MDC.put("attempt", attempt.toString());
    MDC.put("step", HdfsViewUtils.getSepType(recordType).name());

    Set<String> deleteTypes =
        getAllTables().stream().map(RecordType::name).collect(Collectors.toSet());
    // Deletes the target path if it exists
    FsUtils.deleteInterpretIfExist(
        hdfsConfigs, options.getInputPath(), datasetId, attempt, coreTerm, deleteTypes);

    log.info("Adding step 1: Options");
    UnaryOperator<String> interpretPathFn =
        t -> PathBuilder.buildPathInterpretUsingInputPath(options, coreTerm, t, ALL_AVRO);

    Pipeline p = pipelinesFn.apply(options);

    log.info("Adding step 2: Reading AVROs");
    // Core
    BasicTransform basicTransform = BasicTransform.builder().create();
    GbifIdTransform idTransform = GbifIdTransform.builder().create();
    ClusteringTransform clusteringTransform = ClusteringTransform.builder().create();
    MetadataTransform metadataTransform = MetadataTransform.builder().create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.builder().create();
    TaxonomyTransform taxonomyTransform = TaxonomyTransform.builder().create();
    GrscicollTransform grscicollTransform = GrscicollTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();
    EventCoreTransform eventCoreTransform = EventCoreTransform.builder().create();

    // Extension
    MultimediaTransform multimediaTransform = MultimediaTransform.builder().create();
    AudubonTransform audubonTransform = AudubonTransform.builder().create();
    ImageTransform imageTransform = ImageTransform.builder().create();

    log.info("Adding step 3: Creating beam pipeline");
    PCollectionView<MetadataRecord> metadataView =
        p.apply("Read Metadata", metadataTransform.read(interpretPathFn))
            .apply("Convert to view", View.asSingleton());

    PCollection<KV<String, GbifIdRecord>> idCollection =
        p.apply("Read GBIF ids", idTransform.read(interpretPathFn))
            .apply("Map GBIF ids to KV", idTransform.toKv());

    PCollection<KV<String, ClusteringRecord>> clusteringCollection =
        p.apply("Read clustering", clusteringTransform.read(interpretPathFn))
            .apply("Map clustering to KV", clusteringTransform.toKv());

    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        p.apply("Read Verbatim", verbatimTransform.read(interpretPathFn))
            .apply("Map Verbatim to KV", verbatimTransform.toKv());

    PCollection<KV<String, BasicRecord>> basicCollection =
        p.apply("Read Basic", basicTransform.read(interpretPathFn))
            .apply("Map Basic to KV", basicTransform.toKv());

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        p.apply("Read Temporal", temporalTransform.read(interpretPathFn))
            .apply("Map Temporal to KV", temporalTransform.toKv());

    PCollection<KV<String, LocationRecord>> locationCollection =
        p.apply("Read Location", locationTransform.read(interpretPathFn))
            .apply("Map Location to KV", locationTransform.toKv());

    PCollection<KV<String, TaxonRecord>> taxonCollection =
        p.apply("Read Taxon", taxonomyTransform.read(interpretPathFn))
            .apply("Map Taxon to KV", taxonomyTransform.toKv());

    PCollection<KV<String, GrscicollRecord>> grscicollCollection =
        p.apply("Read Grscicoll", grscicollTransform.read(interpretPathFn))
            .apply("Map Grscicoll to KV", grscicollTransform.toKv());

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
        p.apply("Read Multimedia", multimediaTransform.read(interpretPathFn))
            .apply("Map Multimedia to KV", multimediaTransform.toKv());

    PCollection<KV<String, ImageRecord>> imageCollection =
        p.apply("Read Image", imageTransform.read(interpretPathFn))
            .apply("Map Image to KV", imageTransform.toKv());

    PCollection<KV<String, AudubonRecord>> audubonCollection =
        p.apply("Read Audubon", audubonTransform.read(interpretPathFn))
            .apply("Map Audubon to KV", audubonTransform.toKv());

    PCollection<KV<String, EventCoreRecord>> eventCoreCollection =
        DwcTerm.Event == coreTerm
            ? p.apply("Read EventCoreRecord", eventCoreTransform.read(interpretPathFn))
                .apply("Map EventCoreRecord to KV", eventCoreTransform.toKv())
            : p.apply(
                Create.empty(
                    TypeDescriptors.kvs(
                        TypeDescriptors.strings(), eventCoreTransform.getOutputTypeDescriptor())));

    // OccurrenceHdfsRecord
    log.info("Adding step 3: Converting into a OccurrenceHdfsRecord object");
    OccurrenceHdfsRecordTransform hdfsRecordTransform =
        OccurrenceHdfsRecordTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .gbifIdRecordTag(idTransform.getTag())
            .clusteringRecordTag(clusteringTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .temporalRecordTag(temporalTransform.getTag())
            .locationRecordTag(locationTransform.getTag())
            .taxonRecordTag(taxonomyTransform.getTag())
            .grscicollRecordTag(grscicollTransform.getTag())
            .multimediaRecordTag(multimediaTransform.getTag())
            .imageRecordTag(imageTransform.getTag())
            .audubonRecordTag(audubonTransform.getTag())
            .eventCoreRecordTag(eventCoreTransform.getTag())
            .metadataView(metadataView)
            .build();

    KeyedPCollectionTuple
        // Core
        .of(basicTransform.getTag(), basicCollection)
        .and(idTransform.getTag(), idCollection)
        .and(clusteringTransform.getTag(), clusteringCollection)
        .and(temporalTransform.getTag(), temporalCollection)
        .and(locationTransform.getTag(), locationCollection)
        .and(taxonomyTransform.getTag(), taxonCollection)
        .and(grscicollTransform.getTag(), grscicollCollection)
        .and(eventCoreTransform.getTag(), eventCoreCollection)
        // Extension
        .and(multimediaTransform.getTag(), multimediaCollection)
        .and(imageTransform.getTag(), imageCollection)
        .and(audubonTransform.getTag(), audubonCollection)
        // Raw
        .and(verbatimTransform.getTag(), verbatimCollection)
        // Apply
        .apply("Group hdfs objects", CoGroupByKey.create())
        .apply("Merge to HdfsRecord", hdfsRecordTransform.converter())
        .apply(hdfsRecordTransform.write(pathFn.apply(recordType), numberOfShards));

    // Table records
    PCollection<KV<String, CoGbkResult>> tableCollection =
        KeyedPCollectionTuple
            // Join
            .of(idTransform.getTag(), idCollection)
            .and(verbatimTransform.getTag(), verbatimCollection)
            // Apply
            .apply("Group table objects", CoGroupByKey.create());

    AmplificationTableTransform.builder()
        .extendedRecordTag(verbatimTransform.getTag())
        .gbifIdRecordTag(idTransform.getTag())
        .numShards(numberOfShards)
        .path(pathFn.apply(AMPLIFICATION_TABLE))
        .types(types)
        .build()
        .write(tableCollection);

    IdentificationTableTransform.builder()
        .extendedRecordTag(verbatimTransform.getTag())
        .gbifIdRecordTag(idTransform.getTag())
        .numShards(numberOfShards)
        .path(pathFn.apply(IDENTIFICATION_TABLE))
        .types(types)
        .build()
        .write(tableCollection);

    MeasurementOrFactTableTransform.builder()
        .extendedRecordTag(verbatimTransform.getTag())
        .gbifIdRecordTag(idTransform.getTag())
        .numShards(numberOfShards)
        .path(pathFn.apply(MEASUREMENT_OR_FACT_TABLE))
        .types(types)
        .build()
        .write(tableCollection);

    ResourceRelationshipTableTransform.builder()
        .extendedRecordTag(verbatimTransform.getTag())
        .gbifIdRecordTag(idTransform.getTag())
        .numShards(numberOfShards)
        .path(pathFn.apply(RESOURCE_RELATIONSHIP_TABLE))
        .types(types)
        .build()
        .write(tableCollection);

    CloningTableTransform.builder()
        .extendedRecordTag(verbatimTransform.getTag())
        .gbifIdRecordTag(idTransform.getTag())
        .numShards(numberOfShards)
        .path(pathFn.apply(CLONING_TABLE))
        .types(types)
        .build()
        .write(tableCollection);

    GelImageTableTransform.builder()
        .extendedRecordTag(verbatimTransform.getTag())
        .gbifIdRecordTag(idTransform.getTag())
        .numShards(numberOfShards)
        .path(pathFn.apply(GEL_IMAGE_TABLE))
        .types(types)
        .build()
        .write(tableCollection);

    LoanTableTransform.builder()
        .extendedRecordTag(verbatimTransform.getTag())
        .gbifIdRecordTag(idTransform.getTag())
        .numShards(numberOfShards)
        .path(pathFn.apply(LOAN_TABLE))
        .types(types)
        .build()
        .write(tableCollection);

    MaterialSampleTableTransform.builder()
        .extendedRecordTag(verbatimTransform.getTag())
        .gbifIdRecordTag(idTransform.getTag())
        .numShards(numberOfShards)
        .path(pathFn.apply(MATERIAL_SAMPLE_TABLE))
        .types(types)
        .build()
        .write(tableCollection);

    PermitTableTransform.builder()
        .extendedRecordTag(verbatimTransform.getTag())
        .gbifIdRecordTag(idTransform.getTag())
        .numShards(numberOfShards)
        .path(pathFn.apply(PERMIT_TABLE))
        .types(types)
        .build()
        .write(tableCollection);

    PreparationTableTransform.builder()
        .extendedRecordTag(verbatimTransform.getTag())
        .gbifIdRecordTag(idTransform.getTag())
        .numShards(numberOfShards)
        .path(pathFn.apply(PREPARATION_TABLE))
        .types(types)
        .build()
        .write(tableCollection);

    PreservationTableTransform.builder()
        .extendedRecordTag(verbatimTransform.getTag())
        .gbifIdRecordTag(idTransform.getTag())
        .numShards(numberOfShards)
        .path(pathFn.apply(PRESERVATION_TABLE))
        .types(types)
        .build()
        .write(tableCollection);

    GermplasmMeasurementScoreTableTransform.builder()
        .extendedRecordTag(verbatimTransform.getTag())
        .gbifIdRecordTag(idTransform.getTag())
        .numShards(numberOfShards)
        .path(pathFn.apply(GERMPLASM_MEASUREMENT_SCORE_TABLE))
        .types(types)
        .build()
        .write(tableCollection);

    GermplasmMeasurementTraitTableTransform.builder()
        .extendedRecordTag(verbatimTransform.getTag())
        .gbifIdRecordTag(idTransform.getTag())
        .numShards(numberOfShards)
        .path(pathFn.apply(GERMPLASM_MEASUREMENT_TRAIT_TABLE))
        .types(types)
        .build()
        .write(tableCollection);

    GermplasmMeasurementTrialTableTransform.builder()
        .extendedRecordTag(verbatimTransform.getTag())
        .gbifIdRecordTag(idTransform.getTag())
        .numShards(numberOfShards)
        .path(pathFn.apply(GERMPLASM_MEASUREMENT_TRIAL_TABLE))
        .types(types)
        .build()
        .write(tableCollection);

    GermplasmAccessionTableTransform.builder()
        .extendedRecordTag(verbatimTransform.getTag())
        .gbifIdRecordTag(idTransform.getTag())
        .numShards(numberOfShards)
        .path(pathFn.apply(GERMPLASM_ACCESSION_TABLE))
        .types(types)
        .build()
        .write(tableCollection);

    ExtendedMeasurementOrFactTableTransform.builder()
        .extendedRecordTag(verbatimTransform.getTag())
        .gbifIdRecordTag(idTransform.getTag())
        .numShards(numberOfShards)
        .path(pathFn.apply(EXTENDED_MEASUREMENT_OR_FACT_TABLE))
        .types(types)
        .build()
        .write(tableCollection);

    ChronometricAgeTableTransform.builder()
        .extendedRecordTag(verbatimTransform.getTag())
        .gbifIdRecordTag(idTransform.getTag())
        .numShards(numberOfShards)
        .path(pathFn.apply(CHRONOMETRIC_AGE_TABLE))
        .types(types)
        .build()
        .write(tableCollection);

    ReferenceTableTransform.builder()
        .extendedRecordTag(verbatimTransform.getTag())
        .gbifIdRecordTag(idTransform.getTag())
        .numShards(numberOfShards)
        .path(pathFn.apply(REFERENCE_TABLE))
        .types(types)
        .build()
        .write(tableCollection);

    IdentifierTableTransform.builder()
        .extendedRecordTag(verbatimTransform.getTag())
        .gbifIdRecordTag(idTransform.getTag())
        .numShards(numberOfShards)
        .path(pathFn.apply(IDENTIFIER_TABLE))
        .types(types)
        .build()
        .write(tableCollection);

    log.info("Running the pipeline");
    PipelineResult result = p.run();

    if (PipelineResult.State.DONE == result.waitUntilFinish()) {
      Mutex.Action action = () -> HdfsViewAvroUtils.move(options);
      if (options.getTestMode()) {
        action.execute();
      } else {
        SharedLockUtils.doHdfsPrefixLock(options, action);
      }
    }

    log.info("Save metrics into the file and set files owner");
    MetricsHandler.saveCountersToInputPathFile(options, result.metrics());
    String metadataPath =
        PathBuilder.buildDatasetAttemptPath(options, options.getMetaFileName(), true);
    FsUtils.setOwnerToCrap(hdfsConfigs, metadataPath);

    log.info("Pipeline has been finished");
  }
}
