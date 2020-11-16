package au.org.ala.pipelines.beam;

import au.com.bytecode.opencsv.CSVReader;
import au.org.ala.pipelines.options.AllDatasetsPipelinesOptions;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.DumpDatasetSize;
import au.org.ala.utils.ValidationUtils;
import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.LocationFeatureRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.transforms.specific.LocationFeatureTransform;
import org.jetbrains.annotations.NotNull;
import org.slf4j.MDC;

/** Pipeline that adds a sampling AVRO extension to the stored interpretation. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALASamplingToAvroPipeline {

  public static void main(String[] args) throws Exception {
    VersionInfo.print();
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "sample-avro");
    AllDatasetsPipelinesOptions options =
        PipelinesOptionsFactory.create(AllDatasetsPipelinesOptions.class, combinedArgs);
    options.setMetaFileName(ValidationUtils.SAMPLING_METRICS);
    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", "SAMPLING_AVRO");

    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
  }

  public static void run(AllDatasetsPipelinesOptions options) throws Exception {

    // delete metrics if it exists
    MetricsHandler.deleteMetricsFile(options);

    log.info("Creating a pipeline from options");
    Pipeline pipeline = Pipeline.create(options);

    // Path equivalent to /data/pipelines-data/dr893/1/sampling
    String samplingPath = ALAFsUtils.buildPathSamplingDownloadsUsingTargetPath(options);
    log.info("Reading sampling from " + samplingPath);

    // Path equivalent to /data/pipelines-data/dr893/1/sampling/australia_spatial
    // String outputPath = ALAFsUtils.buildPathSamplingOutputUsingTargetPath(options);
    // log.info("Outputting results to " + outputPath);

    FileSystem fs =
        FsUtils.getFileSystem(
            options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getInputPath());

    // load sampling into KV collection of (lat,lng string -> Map of Sampling)
    PCollection<KV<String, Map<String, String>>> alaSampling =
        loadSamplingIntoPCollection(pipeline, samplingPath, fs);

    // generate AVRO for dataset
    if (options.getDatasetId() == null || "all".equals(options.getDatasetId())) {

      Map<String, Long> datasetCounts =
          DumpDatasetSize.readDatasetCounts(fs, options.getInputPath());
      log.info("Generating AVRO for all datasets. Dataset count {}", datasetCounts.size());

      for (Map.Entry<String, Long> datasetCount : datasetCounts.entrySet()) {
        log.info(
            "Generating AVRO for dataset {} - {} records - target path {}",
            datasetCount.getKey(),
            datasetCount.getValue(),
            options.getTargetPath());

        generateForDataset(
            pipeline,
            options.getTargetPath(),
            datasetCount.getKey(),
            options.getAttempt(),
            getSamplingOutputPath(options, datasetCount.getKey()),
            alaSampling);
      }
    } else {
      log.info("Generating AVRO for dataset {}", options.getDatasetId());
      generateForDataset(
          pipeline,
          options.getTargetPath(),
          options.getDatasetId(),
          options.getAttempt(),
          getSamplingOutputPath(options, options.getDatasetId()),
          alaSampling);
    }

    log.info("Running the pipeline");
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Pipeline has been finished");
  }

  @NotNull
  public static String getSamplingOutputPath(
      AllDatasetsPipelinesOptions options, String datasetId) {
    return String.join(
        "/",
        options.getTargetPath(),
        datasetId,
        options.getAttempt().toString(),
        "sampling",
        "location_feature",
        "location_feature");
  }

  private static void generateForDataset(
      Pipeline pipeline,
      String targetPath,
      String datasetId,
      Integer attempt,
      String outputPath,
      PCollection<KV<String, Map<String, String>>> alaSampling) {

    // Location transform output
    String alaRecordDirectoryPath =
        String.join(
            "/",
            targetPath,
            datasetId.trim(),
            attempt.toString(),
            "interpreted",
            PipelinesVariables.Pipeline.Interpretation.RecordType.LOCATION.name().toLowerCase());

    // Filter records without lat/long, create map LatLng -> ExtendedRecord.getId()
    PCollection<KV<String, String>> latLngID =
        pipeline
            .apply(AvroIO.read(LocationRecord.class).from(alaRecordDirectoryPath + "/*.avro"))
            .apply(
                Filter.by(
                    lr -> lr.getDecimalLatitude() != null && lr.getDecimalLongitude() != null))
            .apply(ParDo.of(new LocationRecordFcn()));

    // Create tuple tags
    final TupleTag<String> latLngIDTag = new TupleTag<>();
    final TupleTag<Map<String, String>> alaSamplingTag = new TupleTag<>();

    // Join collections by LatLng string
    PCollection<KV<String, CoGbkResult>> results =
        KeyedPCollectionTuple.of(latLngIDTag, latLngID)
            .and(alaSamplingTag, alaSampling)
            .apply(CoGroupByKey.create());

    // Create LocationFeatureRecord collection which contains samples
    PCollection<LocationFeatureRecord> locationFeatureRecordPCollection =
        results.apply(
            ParDo.of(
                new DoFn<KV<String, CoGbkResult>, LocationFeatureRecord>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    KV<String, CoGbkResult> e = c.element();
                    Iterator<String> idIter = e.getValue().getAll(latLngIDTag).iterator();
                    Map<String, String> sampling = e.getValue().getOnly(alaSamplingTag);

                    while (idIter.hasNext()) {
                      String id = idIter.next();
                      LocationFeatureRecord aur =
                          LocationFeatureRecord.newBuilder().setItems(sampling).setId(id).build();
                      c.output(aur);
                    }
                  }
                }));

    // Write out LocationFeatureRecord collection to AVRO files
    LocationFeatureTransform locationFeatureTransform = LocationFeatureTransform.builder().create();
    locationFeatureRecordPCollection.apply(
        "Write sampling to avro", locationFeatureTransform.write(outputPath));
  }

  private static PCollection<KV<String, Map<String, String>>> loadSamplingIntoPCollection(
      Pipeline p, String samplingPath, FileSystem fs) {

    final String[] columnHeaders = getColumnHeaders(fs, samplingPath);

    // Read from download sampling CSV files
    PCollection<String> lines = p.apply(TextIO.read().from(samplingPath + "/*.csv"));

    // Read in sampling from downloads CSV files, and key it on LatLng -> sampling
    PCollection<KV<String, Map<String, String>>> alaSampling =
        lines.apply(
            ParDo.of(
                new DoFn<String, KV<String, Map<String, String>>>() {
                  @ProcessElement
                  public void processElement(
                      @Element String sampling,
                      OutputReceiver<KV<String, Map<String, String>>> out) {
                    Map<String, String> parsedSampling = new HashMap<String, String>();
                    try {
                      // skip the header
                      if (!sampling.startsWith("latitude")) {
                        // need headers as a side input
                        CSVReader csvReader = new CSVReader(new StringReader(sampling));
                        String[] line = csvReader.readNext();
                        // first two columns are latitude,longitude
                        for (int i = 2; i < columnHeaders.length; i++) {
                          if (StringUtils.trimToNull(line[i]) != null) {
                            parsedSampling.put(columnHeaders[i], line[i]);
                          }
                        }

                        String latLng = line[0] + "," + line[1];
                        KV<String, Map<String, String>> aur = KV.of(latLng, parsedSampling);

                        out.output(aur);
                        csvReader.close();
                      }
                    } catch (Exception e) {
                      throw new RuntimeException(e.getMessage());
                    }
                  }
                }));
    return alaSampling;
  }

  @NotNull
  private static String[] getColumnHeaders(FileSystem fs, String samplingPath) {

    try {
      // obtain column header
      if (ALAFsUtils.exists(fs, samplingPath)) {

        Collection<String> samplingFiles = ALAFsUtils.listPaths(fs, samplingPath);

        if (!samplingFiles.isEmpty()) {

          // read the first line of the first sampling file
          String samplingFilePath = samplingFiles.iterator().next();
          String columnHeaderString =
              new BufferedReader(
                      new InputStreamReader(ALAFsUtils.openInputStream(fs, samplingFilePath)))
                  .readLine();
          return columnHeaderString.split(",");

        } else {
          throw new RuntimeException(
              "Sampling directory found, but is empty. Has sampling from spatial-service been ran ? Missing dir: "
                  + samplingPath);
        }
      } else {
        throw new RuntimeException(
            "Sampling directory cant be found. Has sampling from spatial-service been ran ? Missing dir: "
                + samplingPath);
      }
    } catch (IOException e) {
      throw new RuntimeException(
          "Problem reading sampling from: " + samplingPath + " - " + e.getMessage(), e);
    }
  }

  /** Function to create ALAUUIDRecords. */
  static class LocationRecordFcn extends DoFn<LocationRecord, KV<String, String>> {

    @ProcessElement
    public void processElement(
        @Element LocationRecord locationRecord, OutputReceiver<KV<String, String>> out) {
      try {
        KV<String, String> kv =
            KV.of(
                locationRecord.getDecimalLatitude() + "," + locationRecord.getDecimalLongitude(),
                locationRecord.getId());
        out.output(kv);
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage());
      }
    }
  }
}
