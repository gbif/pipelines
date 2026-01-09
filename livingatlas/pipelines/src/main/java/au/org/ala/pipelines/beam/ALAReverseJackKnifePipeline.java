package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.jackknife.JackKnife;
import au.org.ala.pipelines.options.JackKnifePipelineOptions;
import au.org.ala.pipelines.transforms.JackKnifeOutlierTransform;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.IndexRecord;
import org.gbif.pipelines.io.avro.JackKnifeModelRecord;
import org.gbif.pipelines.io.avro.JackKnifeOutlierRecord;
import org.gbif.pipelines.io.avro.SampleRecord;
import org.slf4j.MDC;

/** Pipeline that adds a taxon jackknife values AVRO extension to the stored interpretation. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAReverseJackKnifePipeline {
  public static final String OUTLIER_RECORDS_COUNT = "outlierRecordsCount";
  public static final String JACKKNIFE_MODELS_COUNT = "modelRecordsCount";

  public static void main(String[] args) throws IOException {
    VersionInfo.print();
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "jackKnife");
    JackKnifePipelineOptions options =
        PipelinesOptionsFactory.create(JackKnifePipelineOptions.class, combinedArgs);
    options.setMetaFileName(ValidationUtils.JACKKNIFE_METRICS);

    // JackKnife is run across all datasets.
    options.setDatasetId("*");

    MDC.put("datasetId", "*");
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", "JACKKNIFE");

    PipelinesOptionsFactory.registerHdfs(options);

    run(options);
  }

  public static void run(JackKnifePipelineOptions options) {

    log.info("Creating a pipeline from options");
    Pipeline pipeline = Pipeline.create(options);

    // JackKnife output locations
    String outliersPath = String.join("/", options.getJackKnifePath(), "outliers");
    String modelsPath = String.join("/", options.getJackKnifePath(), "models");
    String jackknifePath = String.join("/", options.getJackKnifePath());

    log.info("1. Delete existing jackknife outliers, models and metrics.");
    deletePreviousValidation(options, options.getJackKnifePath());

    String[] layers = options.getLayers().split(",");
    if (layers.length == 0 || StringUtils.isEmpty(layers[0])) {
      log.error("JackKnife cannot be run. No layer features provided.");
      return;
    }

    Integer minSampleThreshold = options.getMinSampleThreshold();

    // Load Samples
    String samplingPath =
        String.join("/", ALAFsUtils.buildPathSamplingUsingTargetPath(options), "*.avro");
    log.info("Loading sampling from {}", samplingPath);
    PCollection<SampleRecord> sampleRecords =
        pipeline.apply(AvroIO.read(SampleRecord.class).from(samplingPath));

    // Load IndexRecords
    PCollection<IndexRecord> indexRecordsCollection =
        pipeline.apply(
            AvroIO.read(IndexRecord.class)
                .from(
                    String.join(
                        "/", options.getAllDatasetsInputPath(), "index-record", "*/*.avro")));

    // Convert to KV <LatLng, KV<ID, taxonID>>
    PCollection<KV<String, KV<String, String>>> recordsWithCoordinatesKeyedLatng =
        indexRecordsCollection.apply(
            ParDo.of(
                new DoFn<IndexRecord, KV<String, KV<String, String>>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    IndexRecord e = c.element();
                    String taxonID = e.getTaxonID();
                    String latlng = e.getLatLng();
                    if (latlng != null && taxonID != null) {
                      c.output(KV.of(latlng, KV.of(e.getId(), taxonID)));
                    }
                  }
                }));

    // Convert to KV <LatLng, ArrayOfLayerValues>
    PCollection<KV<String, Double[]>> sampleRecordsKeyedLatng =
        sampleRecords.apply(
            ParDo.of(
                new DoFn<SampleRecord, KV<String, Double[]>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    SampleRecord e = c.element();
                    Double[] values = new Double[layers.length];
                    int countNull = 0;
                    for (int i = 0; i < layers.length; i++) {
                      values[i] = e.getDoubles().getOrDefault(layers[i], Double.NaN);
                      if (values[i] == null) {
                        countNull++;
                      }
                    }
                    String latlng = e.getLatLng();
                    if (latlng != null && countNull < layers.length) {
                      c.output(KV.of(latlng, values));
                    }
                  }
                }));

    // Join collections by LatLng string
    PCollection<KV<String, KV<KV<String, String>, Double[]>>> results =
        Join.innerJoin(recordsWithCoordinatesKeyedLatng, sampleRecordsKeyedLatng);

    // Group by speciesID
    PCollection<KV<String, Iterable<KV<String, Double[]>>>> groups =
        results
            .apply(
                ParDo.of(
                    new DoFn<
                        KV<String, KV<KV<String, String>, Double[]>>,
                        KV<String, KV<String, Double[]>>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        KV<String, KV<KV<String, String>, Double[]>> e = c.element();
                        try {
                          Double[] sampling = e.getValue().getValue();
                          KV<String, String> ir = e.getValue().getKey();

                          String recordID = ir.getKey();
                          String speciesID = ir.getValue();
                          c.output(KV.of(speciesID, KV.of(recordID, sampling)));
                        } catch (Exception ex) {

                        }
                      }
                    }))
            .apply(GroupByKey.create());

    // Calculate and apply JackKnife for each SpeciesID
    final TupleTag<JackKnifeModelRecord> jackKnifeModelRecordTag =
        new TupleTag<JackKnifeModelRecord>() {};
    final TupleTag<JackKnifeOutlierRecord> jackKnifeOutlierRecordTag =
        new TupleTag<JackKnifeOutlierRecord>() {};

    PCollectionTuple jackknife =
        groups.apply(
            ParDo.of(
                    new DoFn<KV<String, Iterable<KV<String, Double[]>>>, JackKnifeModelRecord>() {

                      private final Counter counterModels =
                          Metrics.counter(
                              ALAReverseJackKnifePipeline.class, JACKKNIFE_MODELS_COUNT);
                      private final Counter counterOutliers =
                          Metrics.counter(ALAReverseJackKnifePipeline.class, OUTLIER_RECORDS_COUNT);

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        // Build jacknife model.

                        KV<String, Iterable<KV<String, Double[]>>> e = c.element();
                        Iterator<KV<String, Double[]>> valuesIter = e.getValue().iterator();

                        // Values for each layer.
                        List<Double>[] values = new ArrayList[layers.length];
                        for (int i = 0; i < layers.length; i++) {
                          values[i] = new ArrayList<>();
                        }

                        // Record IDs.
                        List<String> ids = new ArrayList<>();

                        // Remap IDs and layer values.
                        while (valuesIter.hasNext()) {
                          KV<String, Double[]> v = valuesIter.next();
                          ids.add(v.getKey());
                          for (int i = 0; i < layers.length; i++) {
                            values[i].add(v.getValue()[i]);
                          }
                        }

                        // Generate jacknife models for each layer.
                        List<double[]> jackKnifeModels = new ArrayList<>(layers.length);
                        for (int i = 0; i < layers.length; i++) {
                          double[] model = null;
                          try {
                            model =
                                JackKnife.jackknife(
                                    values[i].toArray(new Double[0]), minSampleThreshold);
                            if (model != null) {
                              JackKnifeModelRecord jkmr =
                                  JackKnifeModelRecord.newBuilder()
                                      .setTaxonId(e.getKey())
                                      .setFeature(layers[i])
                                      .setMin(model[0])
                                      .setMax(model[1])
                                      .setCount(
                                          (int)
                                              values[i].stream()
                                                  .filter(v -> !Double.isNaN(v))
                                                  .count())
                                      .build();
                              c.output(jackKnifeModelRecordTag, jkmr);
                              counterModels.inc();
                            }
                          } catch (Exception ex) {
                            log.error(
                                "Failed to build jackknife model for "
                                    + e.getKey()
                                    + " "
                                    + layers[i],
                                ex.getMessage());
                          } finally {
                            jackKnifeModels.add(model);
                          }
                        }

                        // Apply jacknife model to produce ID -> list of outliers
                        for (int i = 0; i < ids.size(); i++) {
                          List<String> outliers = null;

                          for (int j = 0; j < layers.length; j++) {
                            double[] model = jackKnifeModels.get(j);
                            if (model != null) {
                              Double v = values[j].get(i);
                              if (v != null && (model[0] > v || model[1] < v)) {
                                if (outliers == null) {
                                  outliers = new ArrayList<>();
                                }
                                outliers.add(layers[j]);
                              }
                            }
                          }

                          // Collect only IDs containing outlier layers.
                          if (outliers != null) {
                            JackKnifeOutlierRecord jor =
                                JackKnifeOutlierRecord.newBuilder()
                                    .setItems(outliers)
                                    .setId(ids.get(i))
                                    .build();
                            c.output(jackKnifeOutlierRecordTag, jor);
                            counterOutliers.inc();
                          }
                        }
                      }
                    })
                .withOutputTags(
                    jackKnifeModelRecordTag, TupleTagList.of(jackKnifeOutlierRecordTag)));

    // Write out ID -> list of outliers to disk
    JackKnifeOutlierTransform jkot = JackKnifeOutlierTransform.builder().create();
    jackknife
        .get(jackKnifeOutlierRecordTag)
        .apply(
            "Write jackknife outliers to avro",
            jkot.write(outliersPath + "/outliers").withSuffix(".avro"));

    jackknife
        .get(jackKnifeModelRecordTag)
        .apply(
            "Write jackknife models to avro",
            AvroIO.write(JackKnifeModelRecord.class)
                .to(modelsPath + "/models")
                .withSuffix(".avro"));

    log.info("2. Run pipeline for jackknife.");
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    String path = jackknifePath + "/metrics.yaml";
    String countersInfo = MetricsHandler.getCountersInfo(result.metrics());
    FileSystem fs =
        FsUtils.getFileSystem(
            HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()), path);
    try {
      FsUtils.createFile(fs, path, countersInfo);
      log.info("Metadata was written to a file - {}", path);
    } catch (IOException ex) {
      log.warn("Write pipelines metadata file", ex);
    }

    log.info("3. Pipeline has been finished");
  }

  public static void deletePreviousValidation(
      JackKnifePipelineOptions options, String jackknifePath) {
    // delete output directories
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());
    FsUtils.deleteIfExist(hdfsConfigs, jackknifePath + "/outliers");
    FsUtils.deleteIfExist(hdfsConfigs, jackknifePath + "/models");

    // delete metrics
    FsUtils.deleteIfExist(hdfsConfigs, jackknifePath + "/metrics.yaml");
  }
}
