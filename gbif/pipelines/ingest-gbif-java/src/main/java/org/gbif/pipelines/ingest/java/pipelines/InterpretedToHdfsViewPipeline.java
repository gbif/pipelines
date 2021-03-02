package org.gbif.pipelines.ingest.java.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.*;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;
import static org.gbif.pipelines.ingest.java.transforms.InterpretedAvroReader.readAvroAsFuture;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
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
import org.gbif.pipelines.core.converters.IdentificationTableConverter;
import org.gbif.pipelines.core.converters.IdentifierTableConverter;
import org.gbif.pipelines.core.converters.LoanTableConverter;
import org.gbif.pipelines.core.converters.MaterialSampleTableConverter;
import org.gbif.pipelines.core.converters.MeasurementOrFactTableConverter;
import org.gbif.pipelines.core.converters.MeasurementScoreTableConverter;
import org.gbif.pipelines.core.converters.MeasurementTraitTableConverter;
import org.gbif.pipelines.core.converters.MeasurementTrialTableConverter;
import org.gbif.pipelines.core.converters.PermitTableConverter;
import org.gbif.pipelines.core.converters.PreparationTableConverter;
import org.gbif.pipelines.core.converters.PreservationTableConverter;
import org.gbif.pipelines.core.converters.ReferencesTableConverter;
import org.gbif.pipelines.core.converters.ResourceRelationTableConverter;
import org.gbif.pipelines.ingest.java.metrics.IngestMetricsBuilder;
import org.gbif.pipelines.ingest.java.transforms.OccurrenceHdfsRecordConverter;
import org.gbif.pipelines.ingest.java.transforms.TableConverter;
import org.gbif.pipelines.ingest.java.transforms.TableRecordWriter;
import org.gbif.pipelines.ingest.utils.HdfsViewAvroUtils;
import org.gbif.pipelines.ingest.utils.SharedLockUtils;
import org.gbif.pipelines.io.avro.AmplificationTable;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ChronometricAgeTable;
import org.gbif.pipelines.io.avro.ChronometricDateTable;
import org.gbif.pipelines.io.avro.CloningTable;
import org.gbif.pipelines.io.avro.ExtendedMeasurementOrFactTable;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GelImageTable;
import org.gbif.pipelines.io.avro.GermplasmAccessionTable;
import org.gbif.pipelines.io.avro.IdentificationTable;
import org.gbif.pipelines.io.avro.IdentifierTable;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LoanTable;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MaterialSampleTable;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactTable;
import org.gbif.pipelines.io.avro.MeasurementScoreTable;
import org.gbif.pipelines.io.avro.MeasurementTraitTable;
import org.gbif.pipelines.io.avro.MeasurementTrialTable;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.io.avro.PermitTable;
import org.gbif.pipelines.io.avro.PreparationTable;
import org.gbif.pipelines.io.avro.PreservationTable;
import org.gbif.pipelines.io.avro.ReferencesTable;
import org.gbif.pipelines.io.avro.ResourceRelationTable;
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
 *      {@link MeasurementOrFactRecord},
 *      {@link TaxonRecord},
 *      {@link GrscicollRecord},
 *      {@link LocationRecord}
 *    2) Joins avro files
 *    3) Converts to a {@link OccurrenceHdfsRecord} based on the input files
 *    4) Moves the produced files to a directory where the latest version of HDFS records are kept
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -jar target/ingest-gbif-java-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.java.pipelines.InterpretedToHdfsViewPipeline some.properties
 *
 * or pass all parameters:
 *
 * java -jar target/ingest-gbif-java-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.java.pipelines.InterpretedToHdfsViewPipeline \
 * --datasetId=4725681f-06af-4b1e-8fff-e31e266e0a8f \
 * --attempt=1 \
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
    run(args);
  }

  public static void run(String[] args) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) {
    ExecutorService executor = Executors.newWorkStealingPool();
    try {
      run(options, executor);
    } finally {
      executor.shutdown();
    }
  }

  public static void run(String[] args, ExecutorService executor) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options, executor);
  }

  @SneakyThrows
  public static void run(InterpretationPipelineOptions options, ExecutorService executor) {

    MDC.put("datasetKey", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", StepType.INTERPRETED_TO_INDEX.name());

    log.info("Init metrics");
    IngestMetrics metrics = IngestMetricsBuilder.createInterpretedToHdfsViewMetrics();

    log.info("Creating pipeline");

    // Reading all avro files in parallel
    CompletableFuture<Map<String, MetadataRecord>> metadataMapFeature =
        readAvroAsFuture(options, executor, MetadataTransform.builder().create());

    CompletableFuture<Map<String, ExtendedRecord>> verbatimMapFeature =
        readAvroAsFuture(options, executor, VerbatimTransform.create());

    CompletableFuture<Map<String, BasicRecord>> basicMapFeature =
        readAvroAsFuture(options, executor, BasicTransform.builder().create());

    CompletableFuture<Map<String, TemporalRecord>> temporalMapFeature =
        readAvroAsFuture(options, executor, TemporalTransform.builder().create());

    CompletableFuture<Map<String, LocationRecord>> locationMapFeature =
        readAvroAsFuture(options, executor, LocationTransform.builder().create());

    CompletableFuture<Map<String, TaxonRecord>> taxonMapFeature =
        readAvroAsFuture(options, executor, TaxonomyTransform.builder().create());

    CompletableFuture<Map<String, GrscicollRecord>> grscicollMapFeature =
        readAvroAsFuture(options, executor, GrscicollTransform.builder().create());

    CompletableFuture<Map<String, MultimediaRecord>> multimediaMapFeature =
        readAvroAsFuture(options, executor, MultimediaTransform.builder().create());

    CompletableFuture<Map<String, ImageRecord>> imageMapFeature =
        readAvroAsFuture(options, executor, ImageTransform.builder().create());

    CompletableFuture<Map<String, AudubonRecord>> audubonMapFeature =
        readAvroAsFuture(options, executor, AudubonTransform.builder().create());

    String id = options.getDatasetId() + '_' + options.getAttempt();
    Map<String, BasicRecord> basicRecordMap = basicMapFeature.get();

    // OccurrenceHdfsRecord
    Function<BasicRecord, Optional<OccurrenceHdfsRecord>> occurrenceHdfsRecordFn =
        OccurrenceHdfsRecordConverter.builder()
            .metrics(metrics)
            .metadata(metadataMapFeature.get().values().iterator().next())
            .verbatimMap(verbatimMapFeature.get())
            .temporalMap(temporalMapFeature.get())
            .locationMap(locationMapFeature.get())
            .taxonMap(taxonMapFeature.get())
            .grscicollMap(grscicollMapFeature.get())
            .multimediaMap(multimediaMapFeature.get())
            .imageMap(imageMapFeature.get())
            .audubonMap(audubonMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<OccurrenceHdfsRecord>builder()
        .recordFunction(occurrenceHdfsRecordFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(
            PathBuilder.buildFilePathHdfsViewUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(OccurrenceHdfsRecord.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // MeasurementOrFactTable
    Function<BasicRecord, Optional<MeasurementOrFactTable>> measurementOrFactFn =
        TableConverter.<MeasurementOrFactTable>builder()
            .metrics(metrics)
            .converterFn(MeasurementOrFactTableConverter::convert)
            .counterName(MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<MeasurementOrFactTable>builder()
        .recordFunction(measurementOrFactFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(PathBuilder.buildFilePathMoftUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(MeasurementOrFactTable.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // IdentificationTable
    Function<BasicRecord, Optional<IdentificationTable>> identificationFn =
        TableConverter.<IdentificationTable>builder()
            .metrics(metrics)
            .converterFn(IdentificationTableConverter::convert)
            .counterName(IDENTIFICATION_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<IdentificationTable>builder()
        .recordFunction(identificationFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(PathBuilder.buildFilePathMoftUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(IdentificationTable.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // ResourceRelationTable
    Function<BasicRecord, Optional<ResourceRelationTable>> resourceRelationFn =
        TableConverter.<ResourceRelationTable>builder()
            .metrics(metrics)
            .converterFn(ResourceRelationTableConverter::convert)
            .counterName(RESOURCE_RELATION_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<ResourceRelationTable>builder()
        .recordFunction(resourceRelationFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(PathBuilder.buildFilePathMoftUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(ResourceRelationTable.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // AmplificationTable
    Function<BasicRecord, Optional<AmplificationTable>> moftFn =
        TableConverter.<AmplificationTable>builder()
            .metrics(metrics)
            .converterFn(AmplificationTableConverter::convert)
            .counterName(AMPLIFICATION_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<AmplificationTable>builder()
        .recordFunction(moftFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(PathBuilder.buildFilePathMoftUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(AmplificationTable.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // CloningTable
    Function<BasicRecord, Optional<CloningTable>> cloningFn =
        TableConverter.<CloningTable>builder()
            .metrics(metrics)
            .converterFn(CloningTableConverter::convert)
            .counterName(CLONING_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<CloningTable>builder()
        .recordFunction(cloningFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(PathBuilder.buildFilePathMoftUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(CloningTable.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // GelImageTable
    Function<BasicRecord, Optional<GelImageTable>> gelImageFn =
        TableConverter.<GelImageTable>builder()
            .metrics(metrics)
            .converterFn(GelImageTableConverter::convert)
            .counterName(GEL_IMAGE_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<GelImageTable>builder()
        .recordFunction(gelImageFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(PathBuilder.buildFilePathMoftUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(GelImageTable.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // LoanTable
    Function<BasicRecord, Optional<LoanTable>> loanFn =
        TableConverter.<LoanTable>builder()
            .metrics(metrics)
            .converterFn(LoanTableConverter::convert)
            .counterName(LOAN_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<LoanTable>builder()
        .recordFunction(loanFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(PathBuilder.buildFilePathMoftUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(LoanTable.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // MaterialSampleTable
    Function<BasicRecord, Optional<MaterialSampleTable>> materialSampleFn =
        TableConverter.<MaterialSampleTable>builder()
            .metrics(metrics)
            .converterFn(MaterialSampleTableConverter::convert)
            .counterName(MATERIAL_SAMPLE_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<MaterialSampleTable>builder()
        .recordFunction(materialSampleFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(PathBuilder.buildFilePathMoftUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(MaterialSampleTable.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // PermitTable
    Function<BasicRecord, Optional<PermitTable>> permitFn =
        TableConverter.<PermitTable>builder()
            .metrics(metrics)
            .converterFn(PermitTableConverter::convert)
            .counterName(PERMIT_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<PermitTable>builder()
        .recordFunction(permitFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(PathBuilder.buildFilePathMoftUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(PermitTable.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // PreparationTable
    Function<BasicRecord, Optional<PreparationTable>> preparationFn =
        TableConverter.<PreparationTable>builder()
            .metrics(metrics)
            .converterFn(PreparationTableConverter::convert)
            .counterName(PREPARATION_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<PreparationTable>builder()
        .recordFunction(preparationFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(PathBuilder.buildFilePathMoftUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(PreparationTable.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // PreservationTable
    Function<BasicRecord, Optional<PreservationTable>> preservationFn =
        TableConverter.<PreservationTable>builder()
            .metrics(metrics)
            .converterFn(PreservationTableConverter::convert)
            .counterName(PRESERVATION_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<PreservationTable>builder()
        .recordFunction(preservationFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(PathBuilder.buildFilePathMoftUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(PreservationTable.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // MeasurementScoreTable
    Function<BasicRecord, Optional<MeasurementScoreTable>> measurementScoreFn =
        TableConverter.<MeasurementScoreTable>builder()
            .metrics(metrics)
            .converterFn(MeasurementScoreTableConverter::convert)
            .counterName(MEASUREMENT_SCORE_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<MeasurementScoreTable>builder()
        .recordFunction(measurementScoreFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(PathBuilder.buildFilePathMoftUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(MeasurementScoreTable.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // MeasurementTraitTable
    Function<BasicRecord, Optional<MeasurementTraitTable>> measurementTraitFn =
        TableConverter.<MeasurementTraitTable>builder()
            .metrics(metrics)
            .converterFn(MeasurementTraitTableConverter::convert)
            .counterName(MEASUREMENT_TRAIT_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<MeasurementTraitTable>builder()
        .recordFunction(measurementTraitFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(PathBuilder.buildFilePathMoftUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(MeasurementTraitTable.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // MeasurementTrialTable
    Function<BasicRecord, Optional<MeasurementTrialTable>> measurementTrialFn =
        TableConverter.<MeasurementTrialTable>builder()
            .metrics(metrics)
            .converterFn(MeasurementTrialTableConverter::convert)
            .counterName(MEASUREMENT_TRIAL_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<MeasurementTrialTable>builder()
        .recordFunction(measurementTrialFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(PathBuilder.buildFilePathMoftUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(MeasurementTrialTable.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // GermplasmAccessionTable
    Function<BasicRecord, Optional<GermplasmAccessionTable>> germplasmAccessionFn =
        TableConverter.<GermplasmAccessionTable>builder()
            .metrics(metrics)
            .converterFn(GermplasmAccessionTableConverter::convert)
            .counterName(GERMPLASM_ACCESSION_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<GermplasmAccessionTable>builder()
        .recordFunction(germplasmAccessionFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(PathBuilder.buildFilePathMoftUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(GermplasmAccessionTable.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // ExtendedMeasurementOrFactTable
    Function<BasicRecord, Optional<ExtendedMeasurementOrFactTable>> extendedMeasurementOrFactFn =
        TableConverter.<ExtendedMeasurementOrFactTable>builder()
            .metrics(metrics)
            .converterFn(ExtendedMeasurementOrFactTableConverter::convert)
            .counterName(EXTENDED_MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<ExtendedMeasurementOrFactTable>builder()
        .recordFunction(extendedMeasurementOrFactFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(PathBuilder.buildFilePathMoftUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(ExtendedMeasurementOrFactTable.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // ChronometricAgeTable
    Function<BasicRecord, Optional<ChronometricAgeTable>> chronometricAgeFn =
        TableConverter.<ChronometricAgeTable>builder()
            .metrics(metrics)
            .converterFn(ChronometricAgeTableConverter::convert)
            .counterName(CHRONOMETRIC_AGE_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<ChronometricAgeTable>builder()
        .recordFunction(chronometricAgeFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(PathBuilder.buildFilePathMoftUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(ChronometricAgeTable.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // ChronometricDateTable
    Function<BasicRecord, Optional<ChronometricDateTable>> chronometricDateFn =
        TableConverter.<ChronometricDateTable>builder()
            .metrics(metrics)
            .converterFn(ChronometricDateTableConverter::convert)
            .counterName(CHRONOMETRIC_DATE_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<ChronometricDateTable>builder()
        .recordFunction(chronometricDateFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(PathBuilder.buildFilePathMoftUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(ChronometricDateTable.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // ReferencesTable
    Function<BasicRecord, Optional<ReferencesTable>> referencesFn =
        TableConverter.<ReferencesTable>builder()
            .metrics(metrics)
            .converterFn(ReferencesTableConverter::convert)
            .counterName(REFERENCES_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<ReferencesTable>builder()
        .recordFunction(referencesFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(PathBuilder.buildFilePathMoftUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(ReferencesTable.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // IdentifierTable
    Function<BasicRecord, Optional<IdentifierTable>> identifierFn =
        TableConverter.<IdentifierTable>builder()
            .metrics(metrics)
            .converterFn(IdentifierTableConverter::convert)
            .counterName(IDENTIFIER_TABLE_RECORDS_COUNT)
            .verbatimMap(verbatimMapFeature.get())
            .build()
            .getFn();

    TableRecordWriter.<IdentifierTable>builder()
        .recordFunction(identifierFn)
        .basicRecords(basicRecordMap.values())
        .targetTempPath(PathBuilder.buildFilePathMoftUsingInputPath(options, id + AVRO_EXTENSION))
        .schema(IdentifierTable.getClassSchema())
        .executor(executor)
        .options(options)
        .build()
        .write();

    // Move files
    Mutex.Action action = () -> HdfsViewAvroUtils.move(options);
    if (options.getProperties() != null) {
      SharedLockUtils.doHdfsPrefixLock(options, action);
    } else {
      action.execute();
    }

    MetricsHandler.saveCountersToInputPathFile(options, metrics.getMetricsResult());
    log.info("Pipeline has been finished - {}", LocalDateTime.now());
  }
}
