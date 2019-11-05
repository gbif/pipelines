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
import lombok.AllArgsConstructor;
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
    String hdfsConfig = options.getHdfsSiteConfig();
    Properties properties = FsUtils.readPropertiesFile(options.getHdfsSiteConfig(), options.getProperties());

    ExecutorService executor = Executors.newFixedThreadPool(6);

    FsUtils.deleteInterpretIfExist(hdfsConfig, targetPath, datasetId, attempt, types);

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

    CustomDataFileWriter<MetadataRecord> metadataWriter =
        createAvroWriter(MetadataRecord.getClassSchema(), pathFn.apply(metadataTransform.getBaseName()), hdfsConfig);
    CustomDataFileWriter<ExtendedRecord> verbatimWriter =
        createAvroWriter(ExtendedRecord.getClassSchema(), pathFn.apply(verbatimTransform.getBaseName()), hdfsConfig);
    CustomDataFileWriter<BasicRecord> basicWriter =
        createAvroWriter(BasicRecord.getClassSchema(), pathFn.apply(basicTransform.getBaseName()), hdfsConfig);
    CustomDataFileWriter<TemporalRecord> temporalWriter =
        createAvroWriter(TemporalRecord.getClassSchema(), pathFn.apply(temporalTransform.getBaseName()), hdfsConfig);
    CustomDataFileWriter<MultimediaRecord> multimediaWriter =
        createAvroWriter(MultimediaRecord.getClassSchema(), pathFn.apply(multimediaTransform.getBaseName()), hdfsConfig);
    CustomDataFileWriter<ImageRecord> imageWriter =
        createAvroWriter(ImageRecord.getClassSchema(), pathFn.apply(imageTransform.getBaseName()), hdfsConfig);
    CustomDataFileWriter<AudubonRecord> audubonWriter =
        createAvroWriter(AudubonRecord.getClassSchema(), pathFn.apply(audubonTransform.getBaseName()), hdfsConfig);
    CustomDataFileWriter<MeasurementOrFactRecord> measurementWriter =
        createAvroWriter(MeasurementOrFactRecord.getClassSchema(), pathFn.apply(measurementOrFactTransform.getBaseName()), hdfsConfig);
    CustomDataFileWriter<LocationRecord> locationWriter =
        createAvroWriter(LocationRecord.getClassSchema(), pathFn.apply(locationTransform.getBaseName()), hdfsConfig);
    CustomDataFileWriter<TaxonRecord> taxonWriter =
        createAvroWriter(TaxonRecord.getClassSchema(), pathFn.apply(taxonomyTransform.getBaseName()), hdfsConfig);

    Optional<MetadataRecord> mdr = metadataTransform.processElement(options.getDatasetId());
    metadataWriter.append(mdr.get());

    Map<String, ExtendedRecord> erMap = ExtendedRecordReader.readUniqueRecords(options.getInputPath());

    List<CompletableFuture<Void>> futures = new ArrayList<>(erMap.size() * 10);
    erMap.forEach((k, v) -> {
      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        // Verbatim
        futures.add(CompletableFuture.runAsync(() -> verbatimWriter.append(v), executor));
        // Basic
        futures.add(CompletableFuture.runAsync(() -> basicTransform.processElement(v).ifPresent(basicWriter::append), executor));
        // Temporal
        futures.add(CompletableFuture.runAsync(() -> temporalTransform.processElement(v).ifPresent(temporalWriter::append), executor));
        // Multimedia
        futures.add(CompletableFuture.runAsync(() -> multimediaTransform.processElement(v).ifPresent(multimediaWriter::append), executor));
        // Image
        futures.add(CompletableFuture.runAsync(() -> imageTransform.processElement(v).ifPresent(imageWriter::append), executor));
        // Audubon
        futures.add(CompletableFuture.runAsync(() -> audubonTransform.processElement(v).ifPresent(audubonWriter::append), executor));
        // Measurement
        futures.add(CompletableFuture.runAsync(() ->  measurementOrFactTransform.processElement(v).ifPresent(measurementWriter::append), executor));
        // Taxonomy
        futures.add(CompletableFuture.runAsync(() -> taxonomyTransform.processElement(v).ifPresent(taxonWriter::append), executor));
        // Location
        futures.add(CompletableFuture.runAsync(() -> locationTransform.processElement(v, mdr.get()).ifPresent(locationWriter::append), executor));

      }, executor);
      futures.add(future);
    });

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

    metadataWriter.close();
    verbatimWriter.close();
    basicWriter.close();
    temporalWriter.close();
    multimediaWriter.close();
    imageWriter.close();
    audubonWriter.close();
    measurementWriter.close();
    locationWriter.close();
    taxonWriter.close();

    basicTransform.tearDown();
    taxonomyTransform.tearDown();
    locationTransform.tearDown();

    log.info("Pipeline has been finished");

    // TODO: FIX
    System.exit(0);
  }

  private static <T> CustomDataFileWriter<T> createAvroWriter(Schema schema, String outputPath, String hdfsSiteConfig)
      throws Exception {
    Path path = new Path(outputPath);
    FileSystem fs = org.gbif.converters.converter.FsUtils.createParentDirectories(path, hdfsSiteConfig);
    BufferedOutputStream outputStream = new BufferedOutputStream(fs.create(path));
    return CustomDataFileWriter.create(
        DataFileWriteBuilder.builder()
            .schema(schema)
            .codec(CodecFactory.snappyCodec())
            .outputStream(outputStream)
            .syncInterval(2 * 1024 * 1024)
            .build()
            .createDataFileWriter()
    );
  }

  @AllArgsConstructor(staticName = "create")
  static class CustomDataFileWriter<T> {

    private final DataFileWriter<T> dataFileWriter;

    /** Synchronized append method, helps avoid the ArrayIndexOutOfBoundsException */
    public synchronized void append(T record) {
      try {
        dataFileWriter.append(record);
      } catch (IOException ex) {
        log.error(ex.getLocalizedMessage());
        throw new RuntimeException(ex.getMessage(), ex);
      }
    }

    public void close() throws IOException {
      dataFileWriter.close();
    }
  }

}
