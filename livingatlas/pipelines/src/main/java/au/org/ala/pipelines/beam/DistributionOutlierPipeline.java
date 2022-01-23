package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.options.AllDatasetsPipelinesOptions;
import au.org.ala.pipelines.options.DistributionOutlierPipelineOptions;
import au.org.ala.pipelines.transforms.DistributionOutlierTransform;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.factory.FileSystemFactory;
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
  static List<String> WORKLOGS = new ArrayList();

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

    // Create output path
    // default: {fsPath}/pipelines-outlier
    // or {fsPath}/pipelines-outlier/{datasetId}
    String outputPath = ALAFsUtils.buildPathOutlierUsingTargetPath(options, false);

    log.info("Adding step 1: Collecting index records");
    Pipeline p = Pipeline.create(options);

    PCollection<IndexRecord> indexRecords =
        ALAFsUtils.loadIndexRecords(options, p)
            .apply(
                "Filter out records without id/species/taxon/location",
                Filter.by(
                    it ->
                        !StringUtils.isEmpty(it.getTaxonID())
                            && !StringUtils.isEmpty(it.getLatLng())
                            && !StringUtils.isEmpty(it.getId())));

    indexRecords
        .apply(Count.globally())
        .apply(
            MapElements.via(
                new SimpleFunction<Long, Long>() {
                  @Override
                  public Long apply(Long input) {
                    log.info("Number of indexed records loaded: " + input);
                    WORKLOGS.add("Number of indexed records loaded: " + input);
                    return input;
                  }
                }));

    DistributionOutlierTransform distributionTransform =
        new DistributionOutlierTransform(options.getBaseUrl());

    log.info("Adding step 2: Loading existing outliers records");
    PCollection<DistributionOutlierRecord> exitedOutliers =
        loadExistingRecords(options, p, outputPath);

    exitedOutliers
        .apply(Count.globally())
        .apply(
            MapElements.via(
                new SimpleFunction<Long, Long>() {
                  @Override
                  public Long apply(Long input) {
                    log.info("Number of existing outlier records: " + input);
                    WORKLOGS.add("Number of existing outlier records: " + input);
                    return input;
                  }
                }));

    log.info("Adding step 3: Filtering out the records which already have outliers");
    PCollection<KV<String, IndexRecord>> kvIndexRecords =
        indexRecords.apply(
            WithKeys.<String, IndexRecord>of(it -> it.getId())
                .withKeyType(TypeDescriptors.strings()));

    PCollection<KV<String, Boolean>> kvExistingOutliers =
        exitedOutliers
            .apply(
                MapElements.via(
                    new SimpleFunction<DistributionOutlierRecord, String>() {
                      @Override
                      public String apply(DistributionOutlierRecord input) {
                        return input.getId();
                      }
                    }))
            .apply(Distinct.create())
            .apply(
                MapElements.via(
                    new SimpleFunction<String, KV<String, Boolean>>() {
                      @Override
                      public KV<String, Boolean> apply(String input) {
                        return KV.of(input, true);
                      }
                    }));

    PCollection<IndexRecord> newAddedIndexRecords =
        org.apache.beam.sdk.extensions.joinlibrary.Join.leftOuterJoin(
                kvIndexRecords, kvExistingOutliers, false)
            .apply(
                Filter.by(
                    (SerializableFunction<KV<String, KV<IndexRecord, Boolean>>, Boolean>)
                        input -> !input.getValue().getValue())) // Choose outlier-not-exist
            .apply(
                ParDo.of(
                    new DoFn<KV<String, KV<IndexRecord, Boolean>>, IndexRecord>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        KV<IndexRecord, Boolean> kv = c.element().getValue();
                        c.output(kv.getKey());
                      }
                    }));

    newAddedIndexRecords
        .apply(Count.globally())
        .apply(
            MapElements.via(
                new SimpleFunction<Long, Long>() {
                  @Override
                  public Long apply(Long input) {
                    log.info("Number of records to be calculated: " + input);
                    WORKLOGS.add("Number of records to be calculated:  " + input);
                    return input;
                  }
                }));

    log.info("Adding step 4: calculating outliers index");

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

    String targetFile = outputPath + "/outlier_" + ts;
    WORKLOGS.add("Output: outlier_" + ts + ".avro");
    log.info("Adding step 5: writing to " + targetFile + ".avro");
    kvRecords.apply(
        "Write to file",
        AvroIO.write(DistributionOutlierRecord.class)
            .to(targetFile)
            .withoutSharding()
            .withSuffix(".avro"));
    // Checking purpose.
    if (System.getenv("test") != null) {
      kvRecords
          .apply("to String", distributionTransform.flatToString())
          .apply(
              "Write to text", TextIO.write().to(targetFile).withoutSharding().withSuffix(".txt"));
    }

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    writeWorkLogs(options, outputPath);
  }

  /**
   * TODO: HDSF does not support file appending
   *
   * @param options
   * @param outputPath
   */
  private static void writeWorkLogs(AllDatasetsPipelinesOptions options, String outputPath) {
    try {
      WORKLOGS.add("-------------------------------\r\n");
      FileSystem fs =
          FileSystemFactory.getInstance(options.getHdfsSiteConfig(), options.getCoreSiteConfig())
              .getFs(outputPath);
      FSDataOutputStream os = null;
      if (fs.exists(new Path(outputPath + "/work_logs.txt"))) {
        os = fs.append(new Path(outputPath + "/work_logs.txt"));
      } else {
        os = fs.create(new Path(outputPath + "/work_logs.txt"));
      }
      InputStream is = new ByteArrayInputStream(String.join("\r\n", WORKLOGS).getBytes());
      IOUtils.copyBytes(is, os, 4096, true);
    } catch (Exception e) {
      log.warn("Cannot write work history, appending file may not supported? ignored! ");
    }
  }

  private static PCollection<DistributionOutlierRecord> loadExistingRecords(
      AllDatasetsPipelinesOptions options, Pipeline p, String outputPath) throws IOException {

    FileSystem fs =
        FileSystemFactory.getInstance(options.getHdfsSiteConfig(), options.getCoreSiteConfig())
            .getFs(outputPath);

    String outlierPath = ALAFsUtils.getOutlierTargetPath(options);
    boolean hasOutlier = ALAFsUtils.hasAvro(fs, outlierPath, false);
    log.debug("Try to Load existing outliers from {}", outlierPath);

    if (hasOutlier) {
      String samplingPath = String.join("/", outlierPath, "*.avro");
      return p.apply(AvroIO.read(DistributionOutlierRecord.class).from(samplingPath));
    } else {
      log.info("No existing outlier AVRO files under " + outlierPath);
      return p.apply(Create.empty(AvroCoder.of(DistributionOutlierRecord.class)));
    }
  }
}
