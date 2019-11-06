package org.gbif.pipelines.ingest.java.pipelines;

import java.io.BufferedOutputStream;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.UnaryOperator;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.converters.converter.DataFileWriteBuilder;
import org.gbif.converters.parser.xml.parsing.extendedrecord.SyncDataFileWriter;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.java.transforms.ExtendedRecordReader;
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

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.MDC;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
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

    System.out.println(LocalDateTime.now());

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

    ExecutorService executor = Executors.newFixedThreadPool(6);

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

    log.info("Init a pipeline transforms");
    // Core
    metadataTransform.setup();
    basicTransform.setup();
    taxonomyTransform.setup();
    locationTransform.setup();

    Path verbatimPath = new Path(pathFn.apply(verbatimTransform.getBaseName()));
    Path metadataPath = new Path(pathFn.apply(metadataTransform.getBaseName()));
    Path basicPath = new Path(pathFn.apply(basicTransform.getBaseName()));
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
    FileSystem temporalFs = createParentDirectories(temporalPath, hdfsConfig);
    FileSystem multimediaFs = createParentDirectories(multimediaPath, hdfsConfig);
    FileSystem imageFs = createParentDirectories(imagePath, hdfsConfig);
    FileSystem audubonFs = createParentDirectories(audubonPath, hdfsConfig);
    FileSystem measurementFs = createParentDirectories(measurementPath, hdfsConfig);
    FileSystem taxonFs = createParentDirectories(taxonomyPath, hdfsConfig);
    FileSystem locationFs = createParentDirectories(locationPath, hdfsConfig);

    try (
        // Verbatim
        BufferedOutputStream verbatimOutputStream = new BufferedOutputStream(verbatimFs.create(verbatimPath));
        DataFileWriter<ExtendedRecord> verbatimDataFileWriter =
            DataFileWriteBuilder.builder()
                .schema(ExtendedRecord.getClassSchema())
                .codec(CodecFactory.snappyCodec())
                .outputStream(verbatimOutputStream)
                .syncInterval(options.getAvroSyncInterval())
                .build()
                .createDataFileWriter();
        // Metadata
        BufferedOutputStream metadataOutputStream = new BufferedOutputStream(metadataFs.create(metadataPath));
        DataFileWriter<MetadataRecord> metadataDataFileWriter =
            DataFileWriteBuilder.builder()
                .schema(MetadataRecord.getClassSchema())
                .codec(CodecFactory.snappyCodec())
                .outputStream(metadataOutputStream)
                .syncInterval(options.getAvroSyncInterval())
                .build()
                .createDataFileWriter();
        // Basic
        BufferedOutputStream basicOutputStream = new BufferedOutputStream(basicFs.create(basicPath));
        DataFileWriter<BasicRecord> basicDataFileWriter =
            DataFileWriteBuilder.builder()
                .schema(BasicRecord.getClassSchema())
                .codec(CodecFactory.snappyCodec())
                .outputStream(basicOutputStream)
                .syncInterval(options.getAvroSyncInterval())
                .build()
                .createDataFileWriter();
        // Temporal
        BufferedOutputStream temporalOutputStream = new BufferedOutputStream(temporalFs.create(temporalPath));
        DataFileWriter<TemporalRecord> temporalDataFileWriter =
            DataFileWriteBuilder.builder()
                .schema(TemporalRecord.getClassSchema())
                .codec(CodecFactory.snappyCodec())
                .outputStream(temporalOutputStream)
                .syncInterval(options.getAvroSyncInterval())
                .build()
                .createDataFileWriter();
        // Multimedia
        BufferedOutputStream multimediaOutputStream = new BufferedOutputStream(multimediaFs.create(multimediaPath));
        DataFileWriter<MultimediaRecord> multimediaDataFileWriter =
            DataFileWriteBuilder.builder()
                .schema(MultimediaRecord.getClassSchema())
                .codec(CodecFactory.snappyCodec())
                .outputStream(multimediaOutputStream)
                .syncInterval(options.getAvroSyncInterval())
                .build()
                .createDataFileWriter();
        // Image
        BufferedOutputStream imageOutputStream = new BufferedOutputStream(imageFs.create(imagePath));
        DataFileWriter<ImageRecord> imageDataFileWriter =
            DataFileWriteBuilder.builder()
                .schema(ImageRecord.getClassSchema())
                .codec(CodecFactory.snappyCodec())
                .outputStream(imageOutputStream)
                .syncInterval(options.getAvroSyncInterval())
                .build()
                .createDataFileWriter();
        // Audubon
        BufferedOutputStream audubonOutputStream = new BufferedOutputStream(audubonFs.create(audubonPath));
        DataFileWriter<AudubonRecord> audubonDataFileWriter =
            DataFileWriteBuilder.builder()
                .schema(AudubonRecord.getClassSchema())
                .codec(CodecFactory.snappyCodec())
                .outputStream(audubonOutputStream)
                .syncInterval(options.getAvroSyncInterval())
                .build()
                .createDataFileWriter();
        // Measurement
        BufferedOutputStream measurementOutputStream = new BufferedOutputStream(measurementFs.create(measurementPath));
        DataFileWriter<MeasurementOrFactRecord> measurementDataFileWriter =
            DataFileWriteBuilder.builder()
                .schema(MeasurementOrFactRecord.getClassSchema())
                .codec(CodecFactory.snappyCodec())
                .outputStream(measurementOutputStream)
                .syncInterval(options.getAvroSyncInterval())
                .build()
                .createDataFileWriter();
        // Taxonomy
        BufferedOutputStream taxonomyOutputStream = new BufferedOutputStream(taxonFs.create(taxonomyPath));
        DataFileWriter<TaxonRecord> taxonomyDataFileWriter =
            DataFileWriteBuilder.builder()
                .schema(TaxonRecord.getClassSchema())
                .codec(CodecFactory.snappyCodec())
                .outputStream(taxonomyOutputStream)
                .syncInterval(options.getAvroSyncInterval())
                .build()
                .createDataFileWriter();
        // Location
        BufferedOutputStream locationOutputStream = new BufferedOutputStream(locationFs.create(locationPath));
        DataFileWriter<LocationRecord> locationDataFileWriter =
            DataFileWriteBuilder.builder()
                .schema(LocationRecord.getClassSchema())
                .codec(CodecFactory.snappyCodec())
                .outputStream(locationOutputStream)
                .syncInterval(options.getAvroSyncInterval())
                .build()
                .createDataFileWriter();
    ) {

      SyncDataFileWriter<MetadataRecord> metadataWriter = new SyncDataFileWriter<>(metadataDataFileWriter);
      SyncDataFileWriter<ExtendedRecord> verbatimWriter = new SyncDataFileWriter<>(verbatimDataFileWriter);
      SyncDataFileWriter<BasicRecord> basicWriter = new SyncDataFileWriter<>(basicDataFileWriter);
      SyncDataFileWriter<TemporalRecord> temporalWriter = new SyncDataFileWriter<>(temporalDataFileWriter);
      SyncDataFileWriter<MultimediaRecord> multimediaWriter = new SyncDataFileWriter<>(multimediaDataFileWriter);
      SyncDataFileWriter<ImageRecord> imageWriter = new SyncDataFileWriter<>(imageDataFileWriter);
      SyncDataFileWriter<AudubonRecord> audubonWriter = new SyncDataFileWriter<>(audubonDataFileWriter);
      SyncDataFileWriter<MeasurementOrFactRecord> measurementWriter = new SyncDataFileWriter<>(measurementDataFileWriter);
      SyncDataFileWriter<TaxonRecord> taxonWriter = new SyncDataFileWriter<>(taxonomyDataFileWriter);
      SyncDataFileWriter<LocationRecord> locationWriter = new SyncDataFileWriter<>(locationDataFileWriter);

      Optional<MetadataRecord> mdr = metadataTransform.processElement(options.getDatasetId());
      metadataWriter.append(mdr.get());

      CompletableFuture[] futures = ExtendedRecordReader.readUniqueRecords(options.getInputPath())
          .values()
          .stream()
          .map(v ->
              CompletableFuture.runAsync(() -> {
                // Verbatim
                verbatimWriter.append(v);
                // Basic
                basicTransform.processElement(v).ifPresent(basicWriter::append);
                // Temporal
                temporalTransform.processElement(v).ifPresent(temporalWriter::append);
                // Multimedia
                multimediaTransform.processElement(v).ifPresent(multimediaWriter::append);
                // Image
                imageTransform.processElement(v).ifPresent(imageWriter::append);
                // Audubon
                audubonTransform.processElement(v).ifPresent(audubonWriter::append);
                // Measurement
                measurementOrFactTransform.processElement(v).ifPresent(measurementWriter::append);
                // Taxonomy
                taxonomyTransform.processElement(v).ifPresent(taxonWriter::append);
                // Location
                locationTransform.processElement(v, mdr.get()).ifPresent(locationWriter::append);

              }, executor))
          .toArray(CompletableFuture[]::new);

      CompletableFuture.allOf(futures).get();

    } catch (Exception e) {
      log.error("Failed performing conversion on {}", e.getMessage());
      throw new IllegalStateException("Failed performing conversion on ", e);
    } finally {
      basicTransform.tearDown();
      taxonomyTransform.tearDown();
      locationTransform.tearDown();
    }

    log.info("Pipeline has been finished");
    System.out.println(LocalDateTime.now());

    // TODO: FIX
    System.exit(0);
  }

}
