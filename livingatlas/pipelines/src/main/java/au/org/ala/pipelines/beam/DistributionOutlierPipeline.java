package au.org.ala.pipelines.beam;

import static org.apache.beam.sdk.extensions.joinlibrary.Join.leftOuterJoin;

import au.org.ala.pipelines.options.AllDatasetsPipelinesOptions;
import au.org.ala.pipelines.options.DistributionOutlierPipelineOptions;
import au.org.ala.pipelines.transforms.DistributionOutlierTransform;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.StringUtils;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.io.avro.*;
import org.slf4j.MDC;

/**
 * A pipeline that calculate distance to the expert distribution layers (EDL)
 *
 * <p>distanceOutOfELD: 0 -> inside of EDL, -1: -> No EDLs. >0 -> out of EDL
 *
 * <p>Example: java au.org.ala.pipelines.beam.DistributionOutlierPipeline
 * --config=/data/la-pipelines/config/la-pipelines.yaml --fsPath=/data
 *
 * <p>Running with Jar java -cp ../pipelines/target/pipelines-2.10.0-SNAPSHOT-shaded.jar
 * au.org.ala.pipelines.beam.DistributionOutlierPipeline
 * --config=/data/la-pipelines/config/la-pipelines.yaml,la-pipelines-local.yaml --fsPath=/data
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DistributionOutlierPipeline {

  static String INDEX_RECORD_COUNTER = "IndexRecordCounter";
  static String EXISTING_RECORD_COUNTER = "ExistingRecordCounter";
  static String NEW_RECORD_COUNTER = "NewRecordCounter";

  public static void main(String[] args) throws Exception {
    VersionInfo.print();
    CombinedYamlConfiguration conf = new CombinedYamlConfiguration(args);
    String[] combinedArgs = conf.toArgs("general", "outlier");

    DistributionOutlierPipelineOptions options =
        PipelinesOptionsFactory.create(DistributionOutlierPipelineOptions.class, combinedArgs);
    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", "OUTLIER");
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
    System.exit(0);
  }

  public static void run(DistributionOutlierPipelineOptions options) throws Exception {

    Counter newRecordCounter = Metrics.counter(IndexRecord.class, NEW_RECORD_COUNTER);
    Counter existingRecordCounter = Metrics.counter(IndexRecord.class, EXISTING_RECORD_COUNTER);
    Counter indexRecordCounter = Metrics.counter(IndexRecord.class, INDEX_RECORD_COUNTER);

    Pipeline p = Pipeline.create(options);

    log.info("Adding step 1: Read all index records");
    PCollection<IndexRecord> indexRecords =
        ALAFsUtils.loadIndexRecords(options, p)
            .apply(
                "Filter out records without id/species/taxon/location",
                Filter.by(
                    it ->
                        !StringUtils.isEmpty(it.getTaxonID())
                            && !StringUtils.isEmpty(it.getLatLng())
                            && !StringUtils.isEmpty(it.getId())));

    if (options.isAddDebugCounts()) {
      log.info("Adding step 1a: Adding idnex record count metric");
      indexRecords
          .apply(Count.globally())
          .apply(
              MapElements.via(
                  new SimpleFunction<Long, Long>() {
                    @Override
                    public Long apply(Long input) {
                      indexRecordCounter.inc(input);
                      return input;
                    }
                  }));
    }

    DistributionOutlierTransform distributionTransform =
        new DistributionOutlierTransform(options.getBaseUrl());

    log.info("Adding step 2: Create UUID -> IndexRecords");
    PCollection<KV<String, IndexRecord>> kvIndexRecords =
        indexRecords.apply(
            WithKeys.<String, IndexRecord>of(it -> it.getId())
                .withKeyType(TypeDescriptors.strings()));

    log.info("Adding step 3: Loading existing outliers records");
    PCollection<DistributionOutlierRecord> existingOutliers = loadExistingRecords(options, p);

    if (options.isAddDebugCounts()) {
      log.info("Adding step 3a: Adding existing record count metric");
      existingOutliers
          .apply(Count.globally())
          .apply(
              MapElements.via(
                  new SimpleFunction<Long, Long>() {
                    @Override
                    public Long apply(Long input) {
                      existingRecordCounter.inc(input);
                      return input;
                    }
                  }));
    }

    log.info("Adding step 4: Create UUID -> Boolean");
    PCollection<KV<String, Boolean>> kvExistingOutliers =
        existingOutliers
            .apply(MapElements.into(TypeDescriptors.strings()).via(dor -> dor.getId()))
            .apply(Distinct.create())
            .apply(
                MapElements.via(
                    new SimpleFunction<String, KV<String, Boolean>>() {
                      @Override
                      public KV<String, Boolean> apply(String input) {
                        return KV.of(input, true);
                      }
                    }));

    log.info("Adding step 5: Generate list of records not marked as distribution outliers");
    PCollection<IndexRecord> newAddedIndexRecords =
        leftOuterJoin(kvIndexRecords, kvExistingOutliers, false)
            .apply(
                Filter.by(
                    (SerializableFunction<KV<String, KV<IndexRecord, Boolean>>, Boolean>)
                        input -> !input.getValue().getValue())) // Choose outlier-not-exist
            .apply(
                ParDo.of(
                    new DoFn<KV<String, KV<IndexRecord, Boolean>>, IndexRecord>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        newRecordCounter.inc();
                        KV<IndexRecord, Boolean> kv = c.element().getValue();
                        c.output(kv.getKey());
                      }
                    }));

    log.info("Adding step 6: calculating outliers index");
    PCollection<DistributionOutlierRecord> kvRecords =
        newAddedIndexRecords
            .apply("Key by species", distributionTransform.toKv())
            .apply("Grouping by species", GroupByKey.create())
            .apply(
                "Calculating outliers based on the species",
                distributionTransform.calculateOutlier())
            .apply("Flatten records", Flatten.iterables());

    DateFormat df = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");
    String ts = df.format(new Date());

    // Create output path
    // default: {fsPath}/pipelines-outlier
    // or {fsPath}/pipelines-outlier/{datasetId}
    String outputPath = ALAFsUtils.buildPathOutlierUsingTargetPath(options, false);
    log.info("Adding step 8: Writing to " + outputPath + "/outlier_" + ts + "*.avro");
    kvRecords.apply(
        "Write to file",
        AvroIO.write(DistributionOutlierRecord.class)
            .to(outputPath + "/outlier_" + ts)
            .withSuffix(".avro"));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    log.info("Writing metrics.....");
    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());
    log.info("Writing metrics written.");
  }

  private static PCollection<DistributionOutlierRecord> loadExistingRecords(
      AllDatasetsPipelinesOptions options, Pipeline p) {

    String outlierPath = ALAFsUtils.getOutlierTargetPath(options);
    log.info("Existing outlier cache path {}", outlierPath);
    return p.apply(
        AvroIO.read(DistributionOutlierRecord.class)
            .from(outlierPath)
            .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW));
  }
}
