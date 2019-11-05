package org.gbif.pipelines.ingest.java;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.UnaryOperator;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.converters.converter.DataFileWriteBuilder;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.something.ExtendedRecordReader;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.ingest.utils.MetricsHandler;
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
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.beam.sdk.PipelineResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.MDC;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VerbatimToInterpretedPipeline {

  public static void main(String[] args) throws Exception {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) throws Exception {

    String datasetId = options.getDatasetId();
    Integer attempt = options.getAttempt();
    boolean tripletValid = options.isTripletValid();
    boolean occurrenceIdValid = options.isOccurrenceIdValid();
    boolean useExtendedRecordId = options.isUseExtendedRecordId();
    String endPointType = options.getEndPointType();
    Set<String> types = options.getInterpretationTypes();
    String targetPath = options.getTargetPath();
    String hdfsSiteConfig = options.getHdfsSiteConfig();
    Properties properties = FsUtils.readPropertiesFile(options.getHdfsSiteConfig(), options.getProperties());

    ExecutorService executor = Executors.newFixedThreadPool(6);

    FsUtils.deleteInterpretIfExist(hdfsSiteConfig, targetPath, datasetId, attempt, types);

    MDC.put("datasetId", datasetId);
    MDC.put("attempt", attempt.toString());
    MDC.put("step", StepType.VERBATIM_TO_INTERPRETED.name());

    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    UnaryOperator<String> pathFn = t -> FsUtils.buildPathInterpretUsingTargetPath(options, t, id);

    log.info("Creating a pipeline transforms");

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

    log.info("Init a pipeline transforms");
    // Core
    metadataTransform.setup();
    basicTransform.setup();
    taxonomyTransform.setup();
    locationTransform.setup();

    DataFileWriter<MetadataRecord> metadataRecordAvroWriter =
        createAvroWriter(MetadataRecord.getClassSchema(), pathFn.apply(metadataTransform.getBaseName()),
            hdfsSiteConfig);
    DataFileWriter<ExtendedRecord> extendedRecordAvroWriter =
        createAvroWriter(ExtendedRecord.getClassSchema(), pathFn.apply(verbatimTransform.getBaseName()),
            hdfsSiteConfig);
    DataFileWriter<BasicRecord> basicRecordAvroWriter =
        createAvroWriter(BasicRecord.getClassSchema(), pathFn.apply(basicTransform.getBaseName()),
            hdfsSiteConfig);
    DataFileWriter<TemporalRecord> temporalRecordAvroWriter =
        createAvroWriter(TemporalRecord.getClassSchema(), pathFn.apply(temporalTransform.getBaseName()),
            hdfsSiteConfig);
    DataFileWriter<MultimediaRecord> multimediaRecordAvroWriter =
        createAvroWriter(MultimediaRecord.getClassSchema(), pathFn.apply(multimediaTransform.getBaseName()),
            hdfsSiteConfig);
    DataFileWriter<ImageRecord> imageRecordAvroWriter =
        createAvroWriter(ImageRecord.getClassSchema(), pathFn.apply(imageTransform.getBaseName()),
            hdfsSiteConfig);
    DataFileWriter<AudubonRecord> audubonRecordAvroWriter =
        createAvroWriter(AudubonRecord.getClassSchema(), pathFn.apply(audubonTransform.getBaseName()),
            hdfsSiteConfig);
    DataFileWriter<MeasurementOrFactRecord> measurementOrFactRecordAvroWriter =
        createAvroWriter(MeasurementOrFactRecord.getClassSchema(), pathFn.apply(measurementOrFactTransform.getBaseName()),
            hdfsSiteConfig);
    DataFileWriter<LocationRecord> locationRecordAvroWriter =
        createAvroWriter(LocationRecord.getClassSchema(), pathFn.apply(locationTransform.getBaseName()),
            hdfsSiteConfig);
    DataFileWriter<TaxonRecord> taxonRecordAvroWriter =
        createAvroWriter(TaxonRecord.getClassSchema(), pathFn.apply(taxonomyTransform.getBaseName()),
            hdfsSiteConfig);


    Optional<MetadataRecord> mdr = metadataTransform.processElement(options.getDatasetId());
    metadataRecordAvroWriter.append(mdr.get());

    Map<String, ExtendedRecord> erMap = ExtendedRecordReader.readUniqueRecords(options.getInputPath());

    List<CompletableFuture<Void>> futures = new ArrayList<>(erMap.size() * 10);
    erMap.forEach((k, v) -> {
      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        // Verbatim
        futures.add(CompletableFuture.runAsync(() -> {
          try {
            extendedRecordAvroWriter.append(v);
          } catch (IOException e) {
            log.error(e.getLocalizedMessage());
          }
        }, executor));

        // Basic
        futures.add(CompletableFuture.runAsync(() -> {
          try {
            basicRecordAvroWriter.append(basicTransform.processElement(v).get());
          } catch (IOException e) {
            log.error(e.getLocalizedMessage());
          }
        }, executor));

        // Temporal
        futures.add(CompletableFuture.runAsync(() -> {
          try {
            temporalRecordAvroWriter.append(temporalTransform.processElement(v).get());
          } catch (IOException e) {
            log.error(e.getLocalizedMessage());
          }
        }, executor));

        // Multimedia
        futures.add(CompletableFuture.runAsync(() -> {
          try {
            multimediaRecordAvroWriter.append(multimediaTransform.processElement(v).get());
          } catch (IOException e) {
            log.error(e.getLocalizedMessage());
          }
        }, executor));

        // Image
        futures.add(CompletableFuture.runAsync(() -> {
          try {
            imageRecordAvroWriter.append(imageTransform.processElement(v).get());
          } catch (IOException e) {
            log.error(e.getLocalizedMessage());
          }
        }, executor));

        // Audubon
        futures.add(CompletableFuture.runAsync(() -> {
          try {
            audubonRecordAvroWriter.append(audubonTransform.processElement(v).get());
          } catch (IOException e) {
            log.error(e.getLocalizedMessage());
          }
        }, executor));

        // Measurement
        futures.add(CompletableFuture.runAsync(() -> {
          try {
            measurementOrFactRecordAvroWriter.append(measurementOrFactTransform.processElement(v).get());
          } catch (IOException e) {
            log.error(e.getLocalizedMessage());
          }
        }, executor));

        // Taxonomy
        futures.add(CompletableFuture.runAsync(() -> {
          try {
            taxonRecordAvroWriter.append(taxonomyTransform.processElement(v).get());
          } catch (IOException e) {
            log.error(e.getLocalizedMessage());
          }
        }, executor));

        // Location
        futures.add(CompletableFuture.runAsync(() -> {
          try {
            locationRecordAvroWriter.append(locationTransform.processElement(v).get());
          } catch (IOException e) {
            log.error(e.getLocalizedMessage());
          }
        }, executor));
      }, executor);
      futures.add(future);
    });

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

    metadataRecordAvroWriter.close();
    extendedRecordAvroWriter.close();
    basicRecordAvroWriter.close();
    temporalRecordAvroWriter.close();
    multimediaRecordAvroWriter.close();
    imageRecordAvroWriter.close();
    audubonRecordAvroWriter.close();
    measurementOrFactRecordAvroWriter.close();
    locationRecordAvroWriter.close();
    taxonRecordAvroWriter.close();

    basicTransform.tearDown();
    taxonomyTransform.tearDown();
    locationTransform.tearDown();

    PipelineResult result = null;
    MetricsHandler.saveCountersToTargetPathFile(options, result);

    log.info("Deleting beam temporal folders");
    String tempPath = String.join("/", targetPath, datasetId, attempt.toString());
    FsUtils.deleteDirectoryByPrefix(hdfsSiteConfig, tempPath, ".temp-beam");

    log.info("Pipeline has been finished");

  }

  private static <T> DataFileWriter<T> createAvroWriter(Schema schema, String outputPath, String hdfsSiteConfig) throws Exception {
    Path path = new Path(outputPath);
    FileSystem fs = org.gbif.converters.converter.FsUtils.createParentDirectories(path, hdfsSiteConfig);
    BufferedOutputStream outputStream = new BufferedOutputStream(fs.create(path));
    return DataFileWriteBuilder.builder()
        .schema(schema)
        .codec(CodecFactory.snappyCodec())
        .outputStream(outputStream)
        .syncInterval(2 * 1024 * 1024)
        .build()
        .createDataFileWriter();
  }

}
