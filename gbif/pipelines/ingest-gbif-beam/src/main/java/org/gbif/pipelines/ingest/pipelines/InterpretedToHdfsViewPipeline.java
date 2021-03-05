package org.gbif.pipelines.ingest.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.*;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.OCCURRENCE;

import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.ingest.utils.HdfsViewAvroUtils;
import org.gbif.pipelines.ingest.utils.SharedLockUtils;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
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
import org.gbif.pipelines.transforms.table.AmplificationTableTransform;
import org.gbif.pipelines.transforms.table.ChronometricAgeTableTransform;
import org.gbif.pipelines.transforms.table.ChronometricDateTableTransform;
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
public class InterpretedToHdfsViewPipeline {

  public static void main(String[] args) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) {

    String hdfsSiteConfig = options.getHdfsSiteConfig();
    String coreSiteConfig = options.getCoreSiteConfig();
    String datasetId = options.getDatasetId();
    Integer attempt = options.getAttempt();
    Integer numberOfShards = options.getNumberOfShards();
    Set<String> types = getAllTables().stream().map(RecordType::name).collect(Collectors.toSet());

    Function<String, String> pathFn =
        st ->
            PathBuilder.buildFilePathViewUsingInputPath(
                options, st.toLowerCase(), datasetId + '_' + attempt);

    MDC.put("datasetKey", datasetId);
    MDC.put("attempt", attempt.toString());
    MDC.put("step", StepType.HDFS_VIEW.name());

    // Deletes the target path if it exists
    FsUtils.deleteInterpretIfExist(
        hdfsSiteConfig, coreSiteConfig, options.getInputPath(), datasetId, attempt, types);

    log.info("Adding step 1: Options");
    UnaryOperator<String> interpretPathFn =
        t -> PathBuilder.buildPathInterpretUsingInputPath(options, t, "*" + AVRO_EXTENSION);

    Pipeline p = Pipeline.create(options);

    log.info("Adding step 2: Reading AVROs");
    // Core
    BasicTransform basicTransform = BasicTransform.builder().create();
    MetadataTransform metadataTransform = MetadataTransform.builder().create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.builder().create();
    TaxonomyTransform taxonomyTransform = TaxonomyTransform.builder().create();
    GrscicollTransform grscicollTransform = GrscicollTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();
    // Extension
    MultimediaTransform multimediaTransform = MultimediaTransform.builder().create();
    AudubonTransform audubonTransform = AudubonTransform.builder().create();
    ImageTransform imageTransform = ImageTransform.builder().create();

    log.info("Adding step 3: Creating beam pipeline");
    PCollectionView<MetadataRecord> metadataView =
        p.apply("Read Metadata", metadataTransform.read(interpretPathFn))
            .apply("Convert to view", View.asSingleton());

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

    // OccurrenceHdfsRecord

    log.info("Adding step 3: Converting into a OccurrenceHdfsRecord object");
    OccurrenceHdfsRecordTransform hdfsRecordTransform =
        OccurrenceHdfsRecordTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .temporalRecordTag(temporalTransform.getTag())
            .locationRecordTag(locationTransform.getTag())
            .taxonRecordTag(taxonomyTransform.getTag())
            .grscicollRecordTag(grscicollTransform.getTag())
            .multimediaRecordTag(multimediaTransform.getTag())
            .imageRecordTag(imageTransform.getTag())
            .audubonRecordTag(audubonTransform.getTag())
            .metadataView(metadataView)
            .build();

    KeyedPCollectionTuple
        // Core
        .of(basicTransform.getTag(), basicCollection)
        .and(temporalTransform.getTag(), temporalCollection)
        .and(locationTransform.getTag(), locationCollection)
        .and(taxonomyTransform.getTag(), taxonCollection)
        .and(grscicollTransform.getTag(), grscicollCollection)
        // Extension
        .and(multimediaTransform.getTag(), multimediaCollection)
        .and(imageTransform.getTag(), imageCollection)
        .and(audubonTransform.getTag(), audubonCollection)
        // Raw
        .and(verbatimTransform.getTag(), verbatimCollection)
        // Apply
        .apply("Group hdfs objects", CoGroupByKey.create())
        .apply("Merge to HdfsRecord", hdfsRecordTransform.converter())
        .apply(hdfsRecordTransform.write(pathFn.apply(OCCURRENCE), numberOfShards));

    // Table records
    PCollection<KV<String, CoGbkResult>> tableCollection =
        KeyedPCollectionTuple
            // Join
            .of(basicTransform.getTag(), basicCollection)
            .and(verbatimTransform.getTag(), verbatimCollection)
            // Apply
            .apply("Group table objects", CoGroupByKey.create());

    // MeasurementOrFact
    MeasurementOrFactTableTransform measurementOrFactTableTransform =
        MeasurementOrFactTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to MeasurementOrFact", measurementOrFactTableTransform.converter())
        .apply(
            measurementOrFactTableTransform.write(
                pathFn.apply(MEASUREMENT_OR_FACT_TABLE.name()), numberOfShards));

    // Identification
    IdentificationTableTransform identificationTableTransform =
        IdentificationTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to Identification", identificationTableTransform.converter())
        .apply(
            identificationTableTransform.write(
                pathFn.apply(IDENTIFICATION_TABLE.name()), numberOfShards));

    // ResourceRelation
    ResourceRelationshipTableTransform resourceRelationTableTransform =
        ResourceRelationshipTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to ResourceRelation", resourceRelationTableTransform.converter())
        .apply(
            resourceRelationTableTransform.write(
                pathFn.apply(RESOURCE_RELATIONSHIP_TABLE.name()), numberOfShards));

    // Amplification
    AmplificationTableTransform amplificationTableTransform =
        AmplificationTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to Amplification", amplificationTableTransform.converter())
        .apply(
            amplificationTableTransform.write(
                pathFn.apply(AMPLIFICATION_TABLE.name()), numberOfShards));

    // Cloning
    CloningTableTransform cloningTableTransform =
        CloningTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to Cloning", cloningTableTransform.converter())
        .apply(cloningTableTransform.write(pathFn.apply(CLONING_TABLE.name()), numberOfShards));

    // GelImage
    GelImageTableTransform gelImageTableTransform =
        GelImageTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to GelImage", gelImageTableTransform.converter())
        .apply(gelImageTableTransform.write(pathFn.apply(GEL_IMAGE_TABLE.name()), numberOfShards));

    // Loan
    LoanTableTransform loanTableTransform =
        LoanTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to Loan", loanTableTransform.converter())
        .apply(loanTableTransform.write(pathFn.apply(LOAN_TABLE.name()), numberOfShards));

    // MaterialSample
    MaterialSampleTableTransform materialSampleTableTransform =
        MaterialSampleTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to MaterialSample", materialSampleTableTransform.converter())
        .apply(
            materialSampleTableTransform.write(
                pathFn.apply(MATERIAL_SAMPLE_TABLE.name()), numberOfShards));

    // Permit
    PermitTableTransform permitTableTransform =
        PermitTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to Permit", permitTableTransform.converter())
        .apply(permitTableTransform.write(pathFn.apply(PERMIT_TABLE.name()), numberOfShards));

    // Preparation
    PreparationTableTransform preparationTableTransform =
        PreparationTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to Preparation", preparationTableTransform.converter())
        .apply(
            preparationTableTransform.write(
                pathFn.apply(PREPARATION_TABLE.name()), numberOfShards));

    // Preservation
    PreservationTableTransform preservationTableTransform =
        PreservationTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to Preservation", preservationTableTransform.converter())
        .apply(
            preservationTableTransform.write(
                pathFn.apply(PRESERVATION_TABLE.name()), numberOfShards));

    // MeasurementScore
    GermplasmMeasurementScoreTableTransform measurementScoreTableTransform =
        GermplasmMeasurementScoreTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to MeasurementScore", measurementScoreTableTransform.converter())
        .apply(
            measurementScoreTableTransform.write(
                pathFn.apply(GERMPLASM_MEASUREMENT_SCORE_TABLE.name()), numberOfShards));

    // MeasurementTrait
    GermplasmMeasurementTraitTableTransform measurementTraitTableTransform =
        GermplasmMeasurementTraitTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to MeasurementTrait", measurementTraitTableTransform.converter())
        .apply(
            measurementTraitTableTransform.write(
                pathFn.apply(GERMPLASM_MEASUREMENT_TRAIT_TABLE.name()), numberOfShards));

    // MeasurementTrial
    GermplasmMeasurementTrialTableTransform measurementTrialTableTransform =
        GermplasmMeasurementTrialTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to MeasurementTrial", measurementTrialTableTransform.converter())
        .apply(
            measurementTrialTableTransform.write(
                pathFn.apply(GERMPLASM_MEASUREMENT_TRIAL_TABLE.name()), numberOfShards));

    // GermplasmAccession
    GermplasmAccessionTableTransform accessionTableTransform =
        GermplasmAccessionTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to GermplasmAccession", accessionTableTransform.converter())
        .apply(
            accessionTableTransform.write(
                pathFn.apply(GERMPLASM_ACCESSION_TABLE.name()), numberOfShards));

    // ExtendedMeasurementOrFact
    ExtendedMeasurementOrFactTableTransform extendedMeasurementOrFactTableTransform =
        ExtendedMeasurementOrFactTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply(
            "Convert to ExtendedMeasurementOrFact",
            extendedMeasurementOrFactTableTransform.converter())
        .apply(
            extendedMeasurementOrFactTableTransform.write(
                pathFn.apply(EXTENDED_MEASUREMENT_OR_FACT_TABLE.name()), numberOfShards));

    // ChronometricAge
    ChronometricAgeTableTransform chronometricAgeTableTransform =
        ChronometricAgeTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to ChronometricAge", chronometricAgeTableTransform.converter())
        .apply(
            chronometricAgeTableTransform.write(
                pathFn.apply(CHRONOMETRIC_AGE_TABLE.name()), numberOfShards));

    // ChronometricDate
    ChronometricDateTableTransform chronometricDateTableTransform =
        ChronometricDateTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to ChronometricDate", chronometricDateTableTransform.converter())
        .apply(
            chronometricDateTableTransform.write(
                pathFn.apply(CHRONOMETRIC_DATE_TABLE.name()), numberOfShards));

    // References
    ReferenceTableTransform referencesTableTransform =
        ReferenceTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to References", referencesTableTransform.converter())
        .apply(
            referencesTableTransform.write(pathFn.apply(REFERENCE_TABLE.name()), numberOfShards));

    // Identifier
    IdentifierTableTransform identifierTableTransform =
        IdentifierTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to Identifier", identifierTableTransform.converter())
        .apply(
            identifierTableTransform.write(pathFn.apply(IDENTIFIER_TABLE.name()), numberOfShards));

    log.info("Running the pipeline");
    PipelineResult result = p.run();

    if (PipelineResult.State.DONE == result.waitUntilFinish()) {
      SharedLockUtils.doHdfsPrefixLock(options, () -> HdfsViewAvroUtils.move(options));
    }

    // Metrics
    MetricsHandler.saveCountersToInputPathFile(options, result.metrics());

    log.info("Pipeline has been finished");
  }
}
