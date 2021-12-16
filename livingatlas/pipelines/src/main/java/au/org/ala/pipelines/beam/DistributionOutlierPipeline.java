package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.options.DistributionOutlierPipelineOptions;
import au.org.ala.pipelines.transforms.DistributionOutlierTransform;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
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

  public static void main(String[] args) throws Exception {
    log.debug("debug test");
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

    // Create output path
    // default: {fsPath}/pipelines-outlier
    // or {fsPath}/pipelines-outlier/{datasetId}
    String outputPath = ALAFsUtils.buildPathOutlierUsingTargetPath(options);

    log.info("Adding step 1: Collecting index records");
    Pipeline p = Pipeline.create(options);

    PCollection<IndexRecord> indexRecords = ALAFsUtils.loadIndexRecords(options, p);

    DistributionOutlierTransform distributionTransform =
        new DistributionOutlierTransform(options.getBaseUrl());

    log.info("Adding step 2: calculating outliers index");
    PCollection<DistributionOutlierRecord> kvRecords =
        indexRecords
            .apply(
                "Filter out records without id/species/taxon/location",
                Filter.by(
                    it ->
                        !StringUtils.isEmpty(it.getTaxonID())
                            && !StringUtils.isEmpty(it.getLatLng())
                            && !StringUtils.isEmpty(it.getId())))
            .apply("Key by species", distributionTransform.toKv())
            .apply("Grouping by species", GroupByKey.create())
            .apply(
                "Calculating outliers based on the species",
                distributionTransform.calculateOutlier())
            .apply("Flatten records", Flatten.iterables());

    log.info("Adding step 3: writing to outliers");

    kvRecords.apply(
        "Write to file",
        AvroIO.write(DistributionOutlierRecord.class)
            .to(outputPath + "/outlier")
            .withoutSharding()
            .withSuffix(".avro"));
    // Checking purpose.
    if (System.getenv("test") != null) {
      kvRecords
          .apply("to String", distributionTransform.flatToString())
          .apply(
              "Write to text",
              TextIO.write().to(outputPath + "/outlier").withoutSharding().withSuffix(".txt"));
    }

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
  }
}
