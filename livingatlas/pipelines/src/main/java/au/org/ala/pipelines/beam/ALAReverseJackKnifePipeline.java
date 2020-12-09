package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import au.org.ala.pipelines.jackknife.JackKnife;
import au.org.ala.pipelines.options.JackKnifePipelineOptions;
import au.org.ala.pipelines.transforms.ALATaxonomyTransform;
import au.org.ala.pipelines.transforms.JackKnifeOutlierTransform;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang.StringUtils;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.JackKnifeModelRecord;
import org.gbif.pipelines.io.avro.JackKnifeOutlierRecord;
import org.gbif.pipelines.io.avro.LocationFeatureRecord;
import org.gbif.pipelines.transforms.specific.LocationFeatureTransform;
import org.slf4j.MDC;

/** Pipeline that adds a taxon jackknife values AVRO extension to the stored interpretation. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAReverseJackKnifePipeline {
  public static final String OUTLIER_RECORDS_COUNT = "outlierRecordsCount";
  public static final String JACKKNIFE_MODELS_COUNT = "modelRecordsCount";

  public static void main(String[] args) throws FileNotFoundException {
    VersionInfo.print();
    String[] combinedArgs =
        new CombinedYamlConfiguration(args).toArgs("general", "sample-avro", "jackKnife");
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
    Pipeline p = Pipeline.create(options);

    // JackKnife output locations
    String outliersPath = String.join("/", options.getPath(), "outliers");
    String modelsPath = String.join("/", options.getPath(), "models");
    String metricsPath = String.join("/", options.getPath());

    // Path to LocationFeatureRecord
    // /data/pipelines-data/dr893/1/sampling/australia_spatial
    String locationPath = ALAFsUtils.buildPathSamplingOutputUsingTargetPath(options) + "*.avro";

    UnaryOperator<String> inputPathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, t, "*" + AVRO_EXTENSION);
    ALATaxonomyTransform alaTaxonomyTransform = ALATaxonomyTransform.builder().create();

    log.info("1. Delete existing jackknife outliers, models and metrics.");
    deletePreviousValidation(options, options.getPath());

    // Filter records without speciesID or with match issue, create map
    // ALATaxonRecord.getSpeciesID() -> ALATaxonRecord.getId().
    PCollection<KV<String, String>> species =
        p.apply("Read Taxon", alaTaxonomyTransform.read(inputPathFn))
            .apply(
                ParDo.of(
                    new DoFn<ALATaxonRecord, KV<String, String>>() {
                      @ProcessElement
                      public void processElement(
                          @Element ALATaxonRecord alaTaxonRecord,
                          OutputReceiver<KV<String, String>> out) {
                        // Only include occurrences that are identified at the SPECIES taxon rank
                        // without an identification issue.
                        if (StringUtils.isNotEmpty(alaTaxonRecord.getTaxonConceptID())
                            && alaTaxonRecord.getRankID() == 7000) {
                          KV<String, String> kv =
                              KV.of(alaTaxonRecord.getId(), alaTaxonRecord.getTaxonConceptID());
                          out.output(kv);
                        }
                      }
                    }));

    String[] layers = options.getLayers().split(",");

    if (layers.length == 0 || StringUtils.isEmpty(layers[0])) {
      log.error("JackKnife cannot be run. No layer features provided.");
      return;
    }

    Integer minSampleThreshold = options.getMinSampleThreshold();

    LocationFeatureTransform locationFeatureTransform = LocationFeatureTransform.builder().create();

    // Filter records without location features, create map EnvironmentalValues ->
    // LocationFeatureRecord.getId()
    PCollection<KV<String, Double[]>> values =
        p.apply("read location features", locationFeatureTransform.read(locationPath))
            .apply(
                ParDo.of(
                    new DoFn<LocationFeatureRecord, KV<String, Double[]>>() {
                      @ProcessElement
                      public void processElement(
                          @Element LocationFeatureRecord locationFeatureRecord,
                          OutputReceiver<KV<String, Double[]>> out) {
                        Double[] values = new Double[layers.length];

                        int valid = 0;
                        for (int i = 0; i < layers.length; i++) {
                          String s = locationFeatureRecord.getItems().get(layers[i]);
                          try {
                            if (StringUtils.isNotEmpty(s)) {
                              values[i] = Double.parseDouble(s);
                              valid++;
                            } else {
                              values[i] = null;
                            }
                          } catch (Exception e) {
                            values[i] = null;
                          }
                        }

                        if (valid > 0) {
                          KV<String, Double[]> kv = KV.of(locationFeatureRecord.getId(), values);
                          out.output(kv);
                        }
                      }
                    }));

    // Create tuple tags
    final TupleTag<String> idTag = new TupleTag<>();
    final TupleTag<Double[]> valuesTag = new TupleTag<>();

    // Join collections by ID
    PCollection<KV<String, CoGbkResult>> results =
        KeyedPCollectionTuple.of(idTag, species)
            .and(valuesTag, values)
            .apply(CoGroupByKey.create());

    // Group by speciesID
    PCollection<KV<String, Iterable<KV<String, Double[]>>>> groups =
        results
            .apply(
                ParDo.of(
                    new DoFn<KV<String, CoGbkResult>, KV<String, KV<String, Double[]>>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        KV<String, CoGbkResult> e = c.element();
                        try {
                          String speciesID = e.getValue().getOnly(idTag);
                          Double[] sampling = e.getValue().getOnly(valuesTag);

                          c.output(KV.of(speciesID, KV.of(e.getKey(), sampling)));
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
                          try {
                            double[] model =
                                JackKnife.jackknife(
                                    values[i].toArray(new Double[values[i].size()]),
                                    minSampleThreshold);
                            jackKnifeModels.add(model);
                            if (model != null) {
                              JackKnifeModelRecord jkmr =
                                  JackKnifeModelRecord.newBuilder()
                                      .setTaxonId(e.getKey())
                                      .setFeature(layers[i])
                                      .setMin(model[0])
                                      .setMax(model[1])
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
                                new JackKnifeOutlierRecord()
                                    .newBuilder()
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
    PipelineResult result = p.run();
    result.waitUntilFinish();

    MetricsHandler.saveCountersToFile(
        options.getHdfsSiteConfig(),
        options.getCoreSiteConfig(),
        metricsPath + "/metrics.yaml",
        result.metrics());

    log.info("3. Pipeline has been finished");
  }

  public static void deletePreviousValidation(
      JackKnifePipelineOptions options, String outliersPath) {
    // delete output directories
    FsUtils.deleteIfExist(
        options.getHdfsSiteConfig(), options.getCoreSiteConfig(), outliersPath + "/outliers");
    FsUtils.deleteIfExist(
        options.getHdfsSiteConfig(), options.getCoreSiteConfig(), outliersPath + "/models");

    // delete metrics
    FsUtils.deleteIfExist(
        options.getHdfsSiteConfig(), options.getCoreSiteConfig(), outliersPath + "/metrics.yaml");
  }
}
