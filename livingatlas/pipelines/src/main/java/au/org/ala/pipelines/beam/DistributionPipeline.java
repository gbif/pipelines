package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.options.DistributionPipelineOptions;
import au.org.ala.pipelines.transforms.ALADistributionTransform;
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
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.*;
import org.slf4j.MDC;

/**
 * A pipeline that calculate distance to the expert distribution layers (EDL)
 *
 * <p>distanceOutOfELD: 0 -> inside of EDL, -1: -> No EDLs. >0 -> out of EDL
 *
 * <p>* --datasetId=0057a720-17c9-4658-971e-9578f3577cf5 * --attempt=1 * --runner=SparkRunner *
 * --targetPath=/some/path/to/output/ *
 * --inputPath=/some/path/to/output/0057a720-17c9-4658-971e-9578f3577cf5/1/verbatim.avro
 *
 * <p>java -jar /data/pipelines-data/avro-tools-1.11.0.jar tojson distribution-00000-of-00005.avro
 * >5.json java -jar /data/pipelines-data/avro-tools-1.11.0.jar fromjson --schema-file schema.avsc
 * ./verbatim.json > verbatim.avro
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DistributionPipeline {

  public static void main(String[] args) throws Exception {
    VersionInfo.print();
    CombinedYamlConfiguration conf = new CombinedYamlConfiguration(args);
    String[] combinedArgs = conf.toArgs("general", "distribution");

    DistributionPipelineOptions options =
        PipelinesOptionsFactory.create(DistributionPipelineOptions.class, combinedArgs);
    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", "DISTRIBUTION");
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
    System.exit(0);
  }

  public static void run(DistributionPipelineOptions options) throws Exception {

    //Create output path
    // default: {fsPath}/pipelines-outlier
    // or {fsPath}/pipelines-outlier/{datasetId}
    String outputPath = ALAFsUtils.buildPathOutlierUsingTargetPath(options);


    log.info("Adding step 1: Collecting index records");
    Pipeline p = Pipeline.create(options);

    PCollection<IndexRecord> indexRecords = ALAFsUtils.loadIndexRecords(options, p);

    ALADistributionTransform distributionTransform =
        new ALADistributionTransform(options.getBaseUrl());

    log.info("Adding step 2: calculating outliers index");
    PCollection<ALADistributionRecord> kvRecords =
        indexRecords
            .apply("Key by species", distributionTransform.toKv())
            .apply("Grouping by species", GroupByKey.create())
            .apply(
                "Calculating outliers based on the species",
                distributionTransform.calculateOutlier())
            .apply("Flatten records", Flatten.iterables());

     kvRecords.apply( "Write to file",AvroIO.write(ALADistributionRecord.class).to(outputPath+"/outliers").withoutSharding().withSuffix(".avro"));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
  }
}
