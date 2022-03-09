package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.options.AllDatasetsPipelinesOptions;
import au.org.ala.pipelines.options.SamplingPipelineOptions;
import au.org.ala.pipelines.util.SamplingUtils;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.sampling.Layer;
import au.org.ala.sampling.SamplingService;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.IndexRecord;
import org.gbif.pipelines.io.avro.SampleRecord;
import org.slf4j.MDC;

/**
 * A pipeline that exports a unique set of coordinates for a dataset into CSV for downstream
 * sampling.
 *
 * <p>This pipeline can only be ran after the {@link ALAVerbatimToInterpretedPipeline} has been ran
 * as it relies on the output of the LocationTransform.
 *
 * <p>In addition, this pipeline will check configured sampling service for newly available layers.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SamplingPipeline {

  public static void main(String[] args) throws Exception {
    VersionInfo.print();
    CombinedYamlConfiguration conf = new CombinedYamlConfiguration(args);
    String[] combinedArgs = conf.toArgs("general", "sampling");
    SamplingPipelineOptions options =
        PipelinesOptionsFactory.create(SamplingPipelineOptions.class, combinedArgs);
    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", "SAMPLING");
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
    // FIXME: Issue logged here: https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/105
    System.exit(0);
  }

  public static void run(SamplingPipelineOptions options) throws Exception {

    FileSystem fs =
        FileSystemFactory.getInstance(options.getHdfsSiteConfig(), options.getCoreSiteConfig())
            .getFs(options.getInputPath());

    log.info("Checking for new layers in the system");
    SamplingService samplingService = SamplingUtils.initSamplingService(options.getBaseUrl());

    boolean newLayersAvailable = newLayersAddedSinceLastSample(samplingService, options, fs);

    if (newLayersAvailable) {
      if (options.getDeleteSamplingForNewLayers()) {
        // delete existing sampling output
        String samplingDir = SamplingUtils.getSamplingDirectoryPath(options);
        log.info(
            "New layers are available. Deleting current sampling cache to enable full refresh");
        FsUtils.deleteIfExist(
            options.getHdfsSiteConfig(), options.getCoreSiteConfig(), samplingDir);
      } else {
        log.warn(
            "New layers are available. You have configured deleteSamplingForNewLayers {}",
            options.getDeleteSamplingForNewLayers());
        log.warn("These means previously sampled records will no have values for new layers.");
      }
    }

    // Initialise pipeline
    Pipeline p = Pipeline.create(options);

    log.info("Adding step 1: Get unique coordinates");
    PCollection<KV<String, String>> latLngs =
        ALAFsUtils.loadIndexRecords(options, p)
            .apply(Filter.by(ir -> ir.getLatLng() != null))
            .apply(
                MapElements.via(
                    new SimpleFunction<IndexRecord, String>() {
                      @Override
                      public String apply(IndexRecord input) {
                        return input.getLatLng();
                      }
                    }))
            .apply(Distinct.create())
            .apply(
                MapElements.via(
                    new SimpleFunction<String, KV<String, String>>() {
                      @Override
                      public KV<String, String> apply(String input) {
                        return KV.of(input, input);
                      }
                    }));

    log.info("Adding step 2: Get sampled points");
    PCollection<KV<String, String>> sampledPoints =
        loadSampleRecords(options, p)
            .apply(
                MapElements.via(
                    new SimpleFunction<SampleRecord, KV<String, String>>() {
                      @Override
                      public KV<String, String> apply(SampleRecord input) {
                        return KV.of(input.getLatLng(), input.getLatLng());
                      }
                    }));

    log.info("Create join collection");
    PCollection<String> nonSampledLatLng =
        org.apache.beam.sdk.extensions.joinlibrary.Join.leftOuterJoin(
                latLngs, sampledPoints, "NOT_SAMPLED")
            .apply(
                Filter.by(
                    (SerializableFunction<KV<String, KV<String, String>>, Boolean>)
                        input -> input.getValue().getValue().equals("NOT_SAMPLED")))
            .apply(Keys.create());

    String outputPath = PathBuilder.buildDatasetAttemptPath(options, "latlng", false);
    if (options.getDatasetId() == null || "all".equalsIgnoreCase(options.getDatasetId())) {
      outputPath = options.getAllDatasetsInputPath() + "/latlng";
    }

    // delete previous runs
    ALAFsUtils.deleteIfExist(fs, outputPath);
    ALAFsUtils.createDirectory(fs, outputPath);

    nonSampledLatLng.apply(TextIO.write().to(outputPath + "/latlng.csv").withoutSharding());

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());
  }

  /**
   * Return sample records as a collection. Returns an empty collection if no sampling records
   * found.
   *
   * @param options
   * @param p
   * @return
   */
  private static PCollection<SampleRecord> loadSampleRecords(
      AllDatasetsPipelinesOptions options, Pipeline p) {

    FileSystem fs =
        FileSystemFactory.getInstance(options.getHdfsSiteConfig(), options.getCoreSiteConfig())
            .getFs(options.getAllDatasetsInputPath());

    String samplingDir = ALAFsUtils.buildPathSamplingUsingTargetPath(options);
    boolean hasSampling = ALAFsUtils.existsAndNonEmpty(fs, samplingDir);

    log.info("Has sampling from previous runs {}", hasSampling);

    if (hasSampling) {
      String samplingPath = String.join("/", samplingDir, "*.avro");
      log.info("Loading sampling from {}", samplingPath);
      return p.apply(AvroIO.read(SampleRecord.class).from(samplingPath));
    } else {
      return p.apply(Create.empty(AvroCoder.of(SampleRecord.class)));
    }
  }

  public static boolean newLayersAddedSinceLastSample(
      SamplingService samplingService, SamplingPipelineOptions options, FileSystem fs)
      throws Exception {

    Long lastSamplingTime = SamplingUtils.samplingLastRan(options, fs);

    // Workflow 1:  Checking for new layers
    List<Layer> layers = samplingService.getLayers().execute().body();

    List<String> layersFiltered =
        Objects.requireNonNull(layers).stream()
            .filter(
                layer ->
                    layer.getDt_added() != null && (layer.getDt_added() / 1000) > lastSamplingTime)
            .map(l -> String.valueOf(l.getId()))
            .collect(Collectors.toList());

    log.info("New layers = " + layersFiltered);

    return !layersFiltered.isEmpty();
  }
}
