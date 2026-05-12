package org.gbif.pipelines.spark;

import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;

import com.beust.jcommander.JCommander;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.ThreadContext;
import org.apache.spark.sql.SparkSession;
import org.gbif.dp.descriptor.JacksonDataPackageParser;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.util.DataPackageConverter;
import org.gbif.pipelines.spark.util.PathUtil;
import org.gbif.pipelines.spark.util.PipelineArgs;
import org.gbif.pipelines.spark.util.PipelineRunner;

@Slf4j
public class DataPackageConversionPipeline {

  public static void main(String[] argsv) throws Exception {
    PipelineArgs args = new PipelineArgs();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true);
    jCommander.parse(argsv);
    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = loadConfig(args.config);
    PipelineRunner.run(
        args, config, null, (spark) -> runCopy(spark, config, args.datasetId, args.attempt));
  }

  public static void runCopy(
      SparkSession spark, PipelinesConfig config, String datasetId, int attempt)
      throws IOException {

    ThreadContext.put("datasetKey", datasetId);
    ThreadContext.put("attempt", String.valueOf(attempt));
    log.info("Starting copy pipeline for dataset {} attempt {}", datasetId, attempt);

    long start = System.currentTimeMillis();

    String source = PathUtil.crawlAttemptPath(config.getInputPath(), datasetId, attempt);
    String destination =
        PathUtil.interpretedAttemptPath(config.getOutputPath(), datasetId, attempt);

    log.info("Copying from {} to {}", source, destination);

    ObjectMapper mapper = new ObjectMapper();
    DataPackageConverter converter =
        new DataPackageConverter(new JacksonDataPackageParser(mapper), mapper);

    converter.convert(spark, Path.of(source), destination);

    log.info(
        "Copy pipeline completed for dataset {} attempt {} in {}ms",
        datasetId,
        attempt,
        System.currentTimeMillis() - start);
  }
}
