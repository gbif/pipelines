package org.gbif.pipelines.spark;

import com.beust.jcommander.JCommander;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.ThreadContext;
import org.apache.spark.sql.SparkSession;
import org.gbif.dp.descriptor.JacksonDataPackageParser;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.dwcdp.DataPackageConverter;
import org.gbif.pipelines.spark.util.MapperUtil;
import org.gbif.pipelines.spark.util.PathUtil;
import org.gbif.pipelines.spark.util.PipelineArgs;
import org.gbif.pipelines.spark.util.PipelineRunner;
import org.gbif.pipelines.spark.util.PipelinesConfigUtil;

@Slf4j
public class DataPackageConversionPipeline {

  private static void sparkExtraBuildOptions(
      SparkSession.Builder builder, PipelinesConfig pipelineConfig) {}

  public record CopyConfig(
      SparkSession spark,
      String inputBasePath,
      String outputBasePath,
      String datasetId,
      int attempt,
      long targetPartionByteSize) {}

  public static void main(String[] argsv) throws Exception {
    PipelineArgs args = new PipelineArgs();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true);
    jCommander.parse(argsv);
    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = PipelinesConfigUtil.loadConfig(args.config);
    long targetPartitionByteSize = config.getPartitionSizeInMB() * 1024 * 1024;

    PipelineRunner.run(
        args,
        config,
        DataPackageConversionPipeline::sparkExtraBuildOptions,
        (spark) ->
            runCopy(
                new CopyConfig(
                    spark,
                    config.getDwcdpNfsRepository(),
                    config.getOutputPath(),
                    args.datasetId,
                    args.attempt,
                    targetPartitionByteSize)));
  }

  public static void runCopy(CopyConfig copyConfig) throws IOException {

    ThreadContext.put("datasetKey", copyConfig.datasetId());
    ThreadContext.put("attempt", String.valueOf(copyConfig.attempt()));
    log.info(
        "Starting copy pipeline for dataset {} attempt {}",
        copyConfig.datasetId(),
        copyConfig.attempt());

    long start = System.currentTimeMillis();

    String source =
        PathUtil.crawlAttemptPath(
            copyConfig.inputBasePath(), copyConfig.datasetId(), copyConfig.attempt());
    String destination =
        PathUtil.interpretedAttemptPath(
            copyConfig.outputBasePath(), copyConfig.datasetId(), copyConfig.attempt());

    if (!Files.isDirectory(Path.of(source))) {
      log.debug("Source path {} not found with attempt, will try parent folder", source);
      source = Path.of(source).getParent().toString();
    }
    log.info("Copying from {} to {}", source, destination);

    DataPackageConverter converter =
        new DataPackageConverter(
            new JacksonDataPackageParser(MapperUtil.MAPPER),
            MapperUtil.MAPPER,
            copyConfig.targetPartionByteSize);

    converter.convert(copyConfig.spark(), Path.of(source), destination);

    log.info(
        "Copy pipeline completed for dataset {} attempt {} in {}ms",
        copyConfig.datasetId(),
        copyConfig.attempt(),
        System.currentTimeMillis() - start);
  }
}
