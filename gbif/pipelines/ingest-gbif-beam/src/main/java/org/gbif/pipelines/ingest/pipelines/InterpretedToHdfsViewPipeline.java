package org.gbif.pipelines.ingest.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AMPLIFICATION_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.CHRONOMETRIC_AGE_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.CHRONOMETRIC_DATE_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.CLONING_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.EXTENDED_MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.GEL_IMAGE_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.GERMPLASM_ACCESSION_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IDENTIFICATION_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IDENTIFIER_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.LOAN_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MATERIAL_SAMPLE_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_SCORE_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_TRAIT_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_TRIAL_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.PERMIT_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.PREPARATION_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.PRESERVATION_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.REFERENCES_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.RESOURCE_RELATION_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;
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
import org.gbif.pipelines.core.converters.AmplificationTableConverter;
import org.gbif.pipelines.core.converters.ChronometricAgeTableConverter;
import org.gbif.pipelines.core.converters.ChronometricDateTableConverter;
import org.gbif.pipelines.core.converters.CloningTableConverter;
import org.gbif.pipelines.core.converters.ExtendedMeasurementOrFactTableConverter;
import org.gbif.pipelines.core.converters.GelImageTableConverter;
import org.gbif.pipelines.core.converters.GermplasmAccessionTableConverter;
import org.gbif.pipelines.core.converters.GermplasmMeasurementScoreTableConverter;
import org.gbif.pipelines.core.converters.GermplasmMeasurementTraitTableConverter;
import org.gbif.pipelines.core.converters.GermplasmMeasurementTrialTableConverter;
import org.gbif.pipelines.core.converters.IdentificationTableConverter;
import org.gbif.pipelines.core.converters.IdentifierTableConverter;
import org.gbif.pipelines.core.converters.LoanTableConverter;
import org.gbif.pipelines.core.converters.MaterialSampleTableConverter;
import org.gbif.pipelines.core.converters.MeasurementOrFactTableConverter;
import org.gbif.pipelines.core.converters.PermitTableConverter;
import org.gbif.pipelines.core.converters.PreparationTableConverter;
import org.gbif.pipelines.core.converters.PreservationTableConverter;
import org.gbif.pipelines.core.converters.ReferenceTableConverter;
import org.gbif.pipelines.core.converters.ResourceRelationshipTableConverter;
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
import org.gbif.pipelines.io.avro.extension.AmplificationTable;
import org.gbif.pipelines.io.avro.extension.ChronometricAgeTable;
import org.gbif.pipelines.io.avro.extension.ChronometricDateTable;
import org.gbif.pipelines.io.avro.extension.CloningTable;
import org.gbif.pipelines.io.avro.extension.ExtendedMeasurementOrFactTable;
import org.gbif.pipelines.io.avro.extension.GelImageTable;
import org.gbif.pipelines.io.avro.extension.GermplasmAccessionTable;
import org.gbif.pipelines.io.avro.extension.GermplasmMeasurementScoreTable;
import org.gbif.pipelines.io.avro.extension.GermplasmMeasurementTraitTable;
import org.gbif.pipelines.io.avro.extension.GermplasmMeasurementTrialTable;
import org.gbif.pipelines.io.avro.extension.IdentificationTable;
import org.gbif.pipelines.io.avro.extension.IdentifierTable;
import org.gbif.pipelines.io.avro.extension.LoanTable;
import org.gbif.pipelines.io.avro.extension.MaterialSampleTable;
import org.gbif.pipelines.io.avro.extension.MeasurementOrFactTable;
import org.gbif.pipelines.io.avro.extension.PermitTable;
import org.gbif.pipelines.io.avro.extension.PreparationTable;
import org.gbif.pipelines.io.avro.extension.PreservationTable;
import org.gbif.pipelines.io.avro.extension.ReferenceTable;
import org.gbif.pipelines.io.avro.extension.ResourceRelationshipTable;
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
import org.gbif.pipelines.transforms.table.OccurrenceHdfsRecordTransform;
import org.gbif.pipelines.transforms.table.TableTransform;
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
    Set<String> types =
        RecordType.getAllTables().stream().map(RecordType::name).collect(Collectors.toSet());

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
    TableTransform<MeasurementOrFactTable> measurementOrFactTableTransform =
        TableTransform.<MeasurementOrFactTable>builder()
            .converterFn(MeasurementOrFactTableConverter::convert)
            .clazz(MeasurementOrFactTable.class)
            .counterName(MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to MeasurementOrFact", measurementOrFactTableTransform.converter())
        .apply(
            measurementOrFactTableTransform.write(
                pathFn.apply(RecordType.MEASUREMENT_OR_FACT_TABLE.name()), numberOfShards));

    // Identification
    TableTransform<IdentificationTable> identificationTableTransform =
        TableTransform.<IdentificationTable>builder()
            .converterFn(IdentificationTableConverter::convert)
            .clazz(IdentificationTable.class)
            .counterName(IDENTIFICATION_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to Identification", identificationTableTransform.converter())
        .apply(
            identificationTableTransform.write(
                pathFn.apply(RecordType.IDENTIFICATION_TABLE.name()), numberOfShards));

    // ResourceRelation
    TableTransform<ResourceRelationshipTable> resourceRelationTableTransform =
        TableTransform.<ResourceRelationshipTable>builder()
            .converterFn(ResourceRelationshipTableConverter::convert)
            .clazz(ResourceRelationshipTable.class)
            .counterName(RESOURCE_RELATION_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to ResourceRelation", resourceRelationTableTransform.converter())
        .apply(
            resourceRelationTableTransform.write(
                pathFn.apply(RecordType.RESOURCE_RELATIONSHIP_TABLE.name()), numberOfShards));

    // Amplification
    TableTransform<AmplificationTable> amplificationTableTransform =
        TableTransform.<AmplificationTable>builder()
            .converterFn(AmplificationTableConverter::convert)
            .clazz(AmplificationTable.class)
            .counterName(AMPLIFICATION_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to Amplification", amplificationTableTransform.converter())
        .apply(
            amplificationTableTransform.write(
                pathFn.apply(RecordType.AMPLIFICATION_TABLE.name()), numberOfShards));

    // Cloning
    TableTransform<CloningTable> cloningTableTransform =
        TableTransform.<CloningTable>builder()
            .converterFn(CloningTableConverter::convert)
            .clazz(CloningTable.class)
            .counterName(CLONING_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to Cloning", cloningTableTransform.converter())
        .apply(
            cloningTableTransform.write(
                pathFn.apply(RecordType.CLONING_TABLE.name()), numberOfShards));

    // GelImage
    TableTransform<GelImageTable> gelImageTableTransform =
        TableTransform.<GelImageTable>builder()
            .converterFn(GelImageTableConverter::convert)
            .clazz(GelImageTable.class)
            .counterName(GEL_IMAGE_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to GelImage", gelImageTableTransform.converter())
        .apply(
            gelImageTableTransform.write(
                pathFn.apply(RecordType.GEL_IMAGE_TABLE.name()), numberOfShards));

    // Loan
    TableTransform<LoanTable> loanTableTransform =
        TableTransform.<LoanTable>builder()
            .converterFn(LoanTableConverter::convert)
            .clazz(LoanTable.class)
            .counterName(LOAN_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to Loan", loanTableTransform.converter())
        .apply(
            loanTableTransform.write(pathFn.apply(RecordType.LOAN_TABLE.name()), numberOfShards));

    // MaterialSample
    TableTransform<MaterialSampleTable> materialSampleTableTransform =
        TableTransform.<MaterialSampleTable>builder()
            .converterFn(MaterialSampleTableConverter::convert)
            .clazz(MaterialSampleTable.class)
            .counterName(MATERIAL_SAMPLE_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to MaterialSample", materialSampleTableTransform.converter())
        .apply(
            materialSampleTableTransform.write(
                pathFn.apply(RecordType.MATERIAL_SAMPLE_TABLE.name()), numberOfShards));

    // Permit
    TableTransform<PermitTable> permitTableTransform =
        TableTransform.<PermitTable>builder()
            .converterFn(PermitTableConverter::convert)
            .clazz(PermitTable.class)
            .counterName(PERMIT_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to Permit", permitTableTransform.converter())
        .apply(
            permitTableTransform.write(
                pathFn.apply(RecordType.PERMIT_TABLE.name()), numberOfShards));

    // Preparation
    TableTransform<PreparationTable> preparationTableTransform =
        TableTransform.<PreparationTable>builder()
            .converterFn(PreparationTableConverter::convert)
            .clazz(PreparationTable.class)
            .counterName(PREPARATION_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to Preparation", preparationTableTransform.converter())
        .apply(
            preparationTableTransform.write(
                pathFn.apply(RecordType.PREPARATION_TABLE.name()), numberOfShards));

    // Preservation
    TableTransform<PreservationTable> preservationTableTransform =
        TableTransform.<PreservationTable>builder()
            .converterFn(PreservationTableConverter::convert)
            .clazz(PreservationTable.class)
            .counterName(PRESERVATION_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to Preservation", preservationTableTransform.converter())
        .apply(
            preservationTableTransform.write(
                pathFn.apply(RecordType.PRESERVATION_TABLE.name()), numberOfShards));

    // MeasurementScore
    TableTransform<GermplasmMeasurementScoreTable> measurementScoreTableTransform =
        TableTransform.<GermplasmMeasurementScoreTable>builder()
            .converterFn(GermplasmMeasurementScoreTableConverter::convert)
            .clazz(GermplasmMeasurementScoreTable.class)
            .counterName(MEASUREMENT_SCORE_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to MeasurementScore", measurementScoreTableTransform.converter())
        .apply(
            measurementScoreTableTransform.write(
                pathFn.apply(RecordType.GERMPLASM_MEASUREMENT_SCORE_TABLE.name()), numberOfShards));

    // MeasurementTrait
    TableTransform<GermplasmMeasurementTraitTable> measurementTraitTableTransform =
        TableTransform.<GermplasmMeasurementTraitTable>builder()
            .converterFn(GermplasmMeasurementTraitTableConverter::convert)
            .clazz(GermplasmMeasurementTraitTable.class)
            .counterName(MEASUREMENT_TRAIT_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to MeasurementTrait", measurementTraitTableTransform.converter())
        .apply(
            measurementTraitTableTransform.write(
                pathFn.apply(RecordType.GERMPLASM_MEASUREMENT_TRAIT_TABLE.name()), numberOfShards));

    // MeasurementTrial
    TableTransform<GermplasmMeasurementTrialTable> measurementTrialTableTransform =
        TableTransform.<GermplasmMeasurementTrialTable>builder()
            .converterFn(GermplasmMeasurementTrialTableConverter::convert)
            .clazz(GermplasmMeasurementTrialTable.class)
            .counterName(MEASUREMENT_TRIAL_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to MeasurementTrial", measurementTrialTableTransform.converter())
        .apply(
            measurementTrialTableTransform.write(
                pathFn.apply(RecordType.GERMPLASM_MEASUREMENT_TRIAL_TABLE.name()), numberOfShards));

    // GermplasmAccession
    TableTransform<GermplasmAccessionTable> accessionTableTransform =
        TableTransform.<GermplasmAccessionTable>builder()
            .converterFn(GermplasmAccessionTableConverter::convert)
            .clazz(GermplasmAccessionTable.class)
            .counterName(GERMPLASM_ACCESSION_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to GermplasmAccession", accessionTableTransform.converter())
        .apply(
            accessionTableTransform.write(
                pathFn.apply(RecordType.GERMPLASM_ACCESSION_TABLE.name()), numberOfShards));

    // ExtendedMeasurementOrFact
    TableTransform<ExtendedMeasurementOrFactTable> extendedMeasurementOrFactTableTransform =
        TableTransform.<ExtendedMeasurementOrFactTable>builder()
            .converterFn(ExtendedMeasurementOrFactTableConverter::convert)
            .clazz(ExtendedMeasurementOrFactTable.class)
            .counterName(EXTENDED_MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply(
            "Convert to ExtendedMeasurementOrFact",
            extendedMeasurementOrFactTableTransform.converter())
        .apply(
            extendedMeasurementOrFactTableTransform.write(
                pathFn.apply(RecordType.EXTENDED_MEASUREMENT_OR_FACT_TABLE.name()),
                numberOfShards));

    // ChronometricAge
    TableTransform<ChronometricAgeTable> chronometricAgeTableTransform =
        TableTransform.<ChronometricAgeTable>builder()
            .converterFn(ChronometricAgeTableConverter::convert)
            .clazz(ChronometricAgeTable.class)
            .counterName(CHRONOMETRIC_AGE_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to ChronometricAge", chronometricAgeTableTransform.converter())
        .apply(
            chronometricAgeTableTransform.write(
                pathFn.apply(RecordType.CHRONOMETRIC_AGE_TABLE.name()), numberOfShards));

    // ChronometricDate
    TableTransform<ChronometricDateTable> chronometricDateTableTransform =
        TableTransform.<ChronometricDateTable>builder()
            .converterFn(ChronometricDateTableConverter::convert)
            .clazz(ChronometricDateTable.class)
            .counterName(CHRONOMETRIC_DATE_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to ChronometricDate", chronometricDateTableTransform.converter())
        .apply(
            chronometricDateTableTransform.write(
                pathFn.apply(RecordType.CHRONOMETRIC_DATE_TABLE.name()), numberOfShards));

    // References
    TableTransform<ReferenceTable> referencesTableTransform =
        TableTransform.<ReferenceTable>builder()
            .converterFn(ReferenceTableConverter::convert)
            .clazz(ReferenceTable.class)
            .counterName(REFERENCES_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to References", referencesTableTransform.converter())
        .apply(
            referencesTableTransform.write(
                pathFn.apply(RecordType.REFERENCE_TABLE.name()), numberOfShards));

    // Identifier
    TableTransform<IdentifierTable> identifierTableTransform =
        TableTransform.<IdentifierTable>builder()
            .converterFn(IdentifierTableConverter::convert)
            .clazz(IdentifierTable.class)
            .counterName(IDENTIFIER_TABLE_RECORDS_COUNT)
            .extendedRecordTag(verbatimTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .build();

    tableCollection
        .apply("Convert to Identifier", identifierTableTransform.converter())
        .apply(
            identifierTableTransform.write(
                pathFn.apply(RecordType.IDENTIFIER_TABLE.name()), numberOfShards));

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
