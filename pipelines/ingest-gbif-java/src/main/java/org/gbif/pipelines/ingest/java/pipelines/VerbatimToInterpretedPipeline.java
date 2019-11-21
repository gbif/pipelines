package org.gbif.pipelines.ingest.java.pipelines;

import java.io.OutputStream;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.converters.converter.SyncDataFileWriter;
import org.gbif.converters.converter.SyncDataFileWriterBuilder;
import org.gbif.pipelines.ingest.java.transforms.DefaultValuesTransform;
import org.gbif.pipelines.ingest.java.transforms.ExtendedRecordReader;
import org.gbif.pipelines.ingest.java.transforms.UniqueGbifIdTransform;
import org.gbif.pipelines.ingest.options.BasePipelineOptions;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.MetadataTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.MDC;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.converters.converter.FsUtils.createParentDirectories;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VerbatimToInterpretedPipeline {

  public static void main(String[] args) throws Exception {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) throws Exception {
    ExecutorService executor = Executors.newWorkStealingPool();
    try {
      run(options, executor);
    } finally {
      executor.shutdown();
    }
  }

  public static void run(InterpretationPipelineOptions options, ExecutorService executor) throws Exception {

    log.info("Pipeline has been started - {}", LocalDateTime.now());

    String datasetId = options.getDatasetId();
    Integer attempt = options.getAttempt();
    boolean tripletValid = options.isTripletValid();
    boolean occurrenceIdValid = options.isOccurrenceIdValid();
    boolean useExtendedRecordId = options.isUseExtendedRecordId();
    String endPointType = options.getEndPointType();
    Set<String> types = options.getInterpretationTypes();
    String targetPath = options.getTargetPath();
    String hdfsConfig = options.getHdfsSiteConfig();
    Properties properties = FsUtils.readPropertiesFile(options.getHdfsSiteConfig(), options.getProperties());

    FsUtils.deleteInterpretIfExist(hdfsConfig, targetPath, datasetId, attempt, types);

    MDC.put("datasetId", datasetId);
    MDC.put("attempt", attempt.toString());
    MDC.put("step", StepType.VERBATIM_TO_INTERPRETED.name());

    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    UnaryOperator<String> pathFn = t -> FsUtils.buildPathInterpretUsingTargetPath(options, t, id);

    log.info("Creating pipeline transforms");

    // Core
    MetadataTransform metadataTransform = MetadataTransform.create(properties, endPointType, attempt);
    BasicTransform basicTransform = BasicTransform.create(properties, datasetId, tripletValid, occurrenceIdValid, useExtendedRecordId);
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.create();
    TaxonomyTransform taxonomyTransform = TaxonomyTransform.create(properties);
    LocationTransform locationTransform = LocationTransform.create(properties);
    // Extension
    MeasurementOrFactTransform measurementOrFactTransform = MeasurementOrFactTransform.create();
    MultimediaTransform multimediaTransform = MultimediaTransform.create();
    AudubonTransform audubonTransform = AudubonTransform.create();
    ImageTransform imageTransform = ImageTransform.create();

    log.info("Init pipeline transforms");
    // Core
    metadataTransform.setup();
    basicTransform.setup();
    taxonomyTransform.setup();
    locationTransform.setup();

    Path verbatimPath = new Path(pathFn.apply(verbatimTransform.getBaseName()));
    Path metadataPath = new Path(pathFn.apply(metadataTransform.getBaseName()));
    Path basicPath = new Path(pathFn.apply(basicTransform.getBaseName()));
    Path basicInvalidPath = new Path(pathFn.apply(basicTransform.getBaseInvalidName()));
    Path temporalPath = new Path(pathFn.apply(temporalTransform.getBaseName()));
    Path multimediaPath = new Path(pathFn.apply(multimediaTransform.getBaseName()));
    Path imagePath = new Path(pathFn.apply(imageTransform.getBaseName()));
    Path audubonPath = new Path(pathFn.apply(audubonTransform.getBaseName()));
    Path measurementPath = new Path(pathFn.apply(measurementOrFactTransform.getBaseName()));
    Path taxonomyPath = new Path(pathFn.apply(taxonomyTransform.getBaseName()));
    Path locationPath = new Path(pathFn.apply(locationTransform.getBaseName()));

    FileSystem metadataFs = createParentDirectories(metadataPath, hdfsConfig);
    FileSystem verbatimFs = createParentDirectories(verbatimPath, hdfsConfig);
    FileSystem basicFs = createParentDirectories(basicPath, hdfsConfig);
    FileSystem basicInvalidFs = createParentDirectories(basicInvalidPath, hdfsConfig);
    FileSystem temporalFs = createParentDirectories(temporalPath, hdfsConfig);
    FileSystem multimediaFs = createParentDirectories(multimediaPath, hdfsConfig);
    FileSystem imageFs = createParentDirectories(imagePath, hdfsConfig);
    FileSystem audubonFs = createParentDirectories(audubonPath, hdfsConfig);
    FileSystem measurementFs = createParentDirectories(measurementPath, hdfsConfig);
    FileSystem taxonFs = createParentDirectories(taxonomyPath, hdfsConfig);
    FileSystem locationFs = createParentDirectories(locationPath, hdfsConfig);

    try (
        SyncDataFileWriter<ExtendedRecord> verbatimWriter =
            createSyncDataFileWriter(options, ExtendedRecord.getClassSchema(), verbatimFs.create(verbatimPath));
        SyncDataFileWriter<MetadataRecord> metadataWriter =
            createSyncDataFileWriter(options, MetadataRecord.getClassSchema(), metadataFs.create(metadataPath));
        SyncDataFileWriter<BasicRecord> basicWriter =
            createSyncDataFileWriter(options, BasicRecord.getClassSchema(), basicFs.create(basicPath));
        SyncDataFileWriter<BasicRecord> basicInvalidWriter =
            createSyncDataFileWriter(options, BasicRecord.getClassSchema(), basicInvalidFs.create(basicInvalidPath));
        SyncDataFileWriter<TemporalRecord> temporalWriter =
            createSyncDataFileWriter(options, TemporalRecord.getClassSchema(), temporalFs.create(temporalPath));
        SyncDataFileWriter<MultimediaRecord> multimediaWriter =
            createSyncDataFileWriter(options, MultimediaRecord.getClassSchema(), multimediaFs.create(multimediaPath));
        SyncDataFileWriter<ImageRecord> imageWriter =
            createSyncDataFileWriter(options, ImageRecord.getClassSchema(), imageFs.create(imagePath));
        SyncDataFileWriter<AudubonRecord> audubonWriter =
            createSyncDataFileWriter(options, AudubonRecord.getClassSchema(), audubonFs.create(audubonPath));
        SyncDataFileWriter<MeasurementOrFactRecord> measurementWriter =
            createSyncDataFileWriter(options, MeasurementOrFactRecord.getClassSchema(), measurementFs.create(measurementPath));
        SyncDataFileWriter<TaxonRecord> taxonWriter =
            createSyncDataFileWriter(options, TaxonRecord.getClassSchema(), taxonFs.create(taxonomyPath));
        SyncDataFileWriter<LocationRecord> locationWriter =
            createSyncDataFileWriter(options, LocationRecord.getClassSchema(), locationFs.create(locationPath))
    ) {

      // Create MetadataRecord
      MetadataRecord mdr = metadataTransform.processElement(options.getDatasetId())
          .orElseThrow(() -> new IllegalArgumentException("MetadataRecord can't be null"));
      metadataWriter.append(mdr);

      // Read DWCA and replace default values
      Map<String, ExtendedRecord> erMap = ExtendedRecordReader.readUniqueRecords(options.getInputPath());
      DefaultValuesTransform.create(properties, datasetId).replaceDefaultValues(erMap);

      // Filter GBIF id duplicates
      UniqueGbifIdTransform gbifIdTransform =
          UniqueGbifIdTransform.builder()
              .executor(executor)
              .erMap(erMap)
              .basicTransform(basicTransform)
              .build()
              .run();

      // Create interpretation
      Consumer<ExtendedRecord> interpretAllFn = er -> {
        BasicRecord br = gbifIdTransform.getBrInvalidMap().get(er.getId());
        if (br == null) {
          verbatimWriter.append(er);
          temporalTransform.processElement(er).ifPresent(temporalWriter::append);
          multimediaTransform.processElement(er).ifPresent(multimediaWriter::append);
          imageTransform.processElement(er).ifPresent(imageWriter::append);
          audubonTransform.processElement(er).ifPresent(audubonWriter::append);
          measurementOrFactTransform.processElement(er).ifPresent(measurementWriter::append);
          taxonomyTransform.processElement(er).ifPresent(taxonWriter::append);
          locationTransform.processElement(er, mdr).ifPresent(locationWriter::append);
        } else {
          basicInvalidWriter.append(br);
        }
      };

      // Run async writing for BasicRecords
      Stream<CompletableFuture<Void>> streamBr = gbifIdTransform.getBrMap().values()
          .stream()
          .map(v -> CompletableFuture.runAsync(() -> basicWriter.append(v), executor));

      // Run async interpretation and writing for all records
      Stream<CompletableFuture<Void>> streamAll = erMap.values()
          .stream()
          .map(v -> CompletableFuture.runAsync(() -> interpretAllFn.accept(v), executor));

      CompletableFuture[] futures = Stream.concat(streamBr, streamAll).toArray(CompletableFuture[]::new);
      CompletableFuture.allOf(futures).get();

    } catch (Exception e) {
      log.error("Failed performing conversion on {}", e.getMessage());
      throw new IllegalStateException("Failed performing conversion on ", e);
    } finally {
      basicTransform.tearDown();
      taxonomyTransform.tearDown();
      locationTransform.tearDown();
    }

    log.info("Pipeline has been finished - {}", LocalDateTime.now());
  }

  @SneakyThrows
  private static <T> SyncDataFileWriter<T> createSyncDataFileWriter(BasePipelineOptions options, Schema schema,
      OutputStream stream) {
    return SyncDataFileWriterBuilder.builder()
        .schema(schema)
        .codec(options.getAvroCompressionType())
        .outputStream(stream)
        .syncInterval(options.getAvroSyncInterval())
        .build()
        .createSyncDataFileWriter();
  }

}
