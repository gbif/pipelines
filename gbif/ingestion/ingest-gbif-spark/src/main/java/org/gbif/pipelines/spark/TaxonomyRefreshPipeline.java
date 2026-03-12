/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.pipelines.spark;

import static org.gbif.pipelines.spark.ArgsConstants.*;
import static org.gbif.pipelines.spark.Directories.*;
import static org.gbif.pipelines.spark.OccurrenceInterpretationPipeline.*;
import static org.gbif.pipelines.spark.util.MetricsUtil.writeMetricsYaml;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.ThreadContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.spark.pojo.Occurrence;
import org.gbif.pipelines.transform.MultiTaxonomyTransform;

/** Main class for the Spark pipeline that just reruns taxonomy interpretation. */
@Slf4j
public class TaxonomyRefreshPipeline {

  static final ObjectMapper MAPPER = new ObjectMapper();

  public static final String METRICS_FILENAME = "taxonomy.yml";

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = APP_NAME_ARG, description = "Application name", required = true)
    private String appName;

    @Parameter(names = DATASET_ID_ARG, description = "Dataset ID", required = true)
    private String datasetId;

    @Parameter(names = ATTEMPT_ID_ARG, description = "Attempt number", required = true)
    private int attempt;

    @Parameter(
        names = CONFIG_PATH_ARG,
        description = "Path to YAML configuration file",
        required = true)
    private String config = "/tmp/pipelines-spark.yaml";

    @Parameter(
        names = SPARK_MASTER_ARG,
        description = "Spark master - there for local dev only",
        required = false)
    private String master;

    @Parameter(
        names = {"--help", "-h"},
        help = true,
        description = "Show usage")
    private boolean help;
  }

  public static void main(String[] argsv) throws Exception {
    Args args = new Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true);
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = loadConfig(args.config);
    String datasetId = args.datasetId;
    int attempt = args.attempt;

    /* ############ standard init block ########## */
    SparkSession spark =
        getSparkSession(
            args.master, args.appName, config, TaxonomyRefreshPipeline::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);
    /* ############ standard init block - end ########## */

    runTaxonomy(spark, fileSystem, config, datasetId, attempt, 1);

    fileSystem.close();
    spark.stop();
    spark.close();
    System.exit(0);
  }

  public static void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    sparkBuilder.config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.1");
  }

  public static void runTaxonomy(
      SparkSession spark,
      FileSystem fs,
      PipelinesConfig config,
      String datasetId,
      int attempt,
      int numOfShards) {

    long start = System.currentTimeMillis();

    ThreadContext.put("datasetKey", datasetId);

    String outputPath = String.format("%s/%s/%d", config.getOutputPath(), datasetId, attempt);

    // a single call to the registry to get the dataset metadata
    final MetadataRecord metadata = getMetadataRecord(config, datasetId, attempt);

    Dataset<Occurrence> simpleRecords = loadSimpleRecords(spark, outputPath);

    Dataset<Occurrence> interpreted =
        runTaxonomyTransform(spark, config, simpleRecords, outputPath);

    // write parquet for elastic
    toJson(interpreted, metadata, numOfShards)
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(outputPath + "/" + OCCURRENCE_JSON);

    // write parquet for hdfs view
    toHdfs(interpreted, metadata, numOfShards)
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(outputPath + "/" + OCCURRENCE_HDFS);

    long recordCount = simpleRecords.count();

    // write metrics to yaml
    writeMetricsYaml(
        fs,
        Map.of(PipelinesVariables.Metrics.CLUSTERING_RECORDS_COUNT, recordCount),
        outputPath + "/" + METRICS_FILENAME);

    log.info(
        "Finished taxonomy re-interpretation in {} secs, records: {}",
        (System.currentTimeMillis() - start) / 1000,
        recordCount);
  }

  private static Dataset<Occurrence> loadSimpleRecords(SparkSession spark, String outputPath) {
    return spark
        .read()
        .parquet(outputPath + "/" + SIMPLE_OCCURRENCE)
        .as(Encoders.bean(Occurrence.class));
  }

  /**
   * Runs all the transforms on the simple records to produce fully interpreted occurrence records.
   *
   * @param config The pipelines configuration.
   * @param simpleRecords The dataset of simple occurrence records.
   * @return The dataset of fully interpreted occurrence records.
   */
  private static Dataset<Occurrence> runTaxonomyTransform(
      SparkSession spark,
      PipelinesConfig config,
      Dataset<Occurrence> simpleRecords,
      String outputPath) {

    // Set up transform
    MultiTaxonomyTransform taxonomyTransform = MultiTaxonomyTransform.create(config);

    // Loop over all records and interpret them
    Dataset<Occurrence> interpreted =
        simpleRecords.map(
            (MapFunction<Occurrence, Occurrence>)
                simpleRecord -> {
                  ExtendedRecord er =
                      MAPPER.readValue(simpleRecord.getVerbatim(), ExtendedRecord.class);

                  // Apply taxonomy transforms
                  MultiTaxonRecord mtr = taxonomyTransform.convert(er);

                  return Occurrence.builder()
                      .id(simpleRecord.getId())
                      .identifier(simpleRecord.getIdentifier())
                      .verbatim(simpleRecord.getVerbatim())
                      .basic(simpleRecord.getBasic())
                      .taxon(MAPPER.writeValueAsString(mtr))
                      .location(simpleRecord.getLocation())
                      .grscicoll(simpleRecord.getGrscicoll())
                      .temporal(simpleRecord.getTemporal())
                      .dnaDerivedData(simpleRecord.getDnaDerivedData())
                      .multimedia(simpleRecord.getMultimedia())
                      .clustering(simpleRecord.getClustering())
                      .build();
                },
            Encoders.bean(Occurrence.class));

    // write simple interpreted records to disk
    interpreted.write().mode(SaveMode.Overwrite).parquet(outputPath + "/" + SIMPLE_OCCURRENCE);

    // re-load
    return spark
        .read()
        .parquet(outputPath + "/" + SIMPLE_OCCURRENCE)
        .as(Encoders.bean(Occurrence.class));
  }
}
