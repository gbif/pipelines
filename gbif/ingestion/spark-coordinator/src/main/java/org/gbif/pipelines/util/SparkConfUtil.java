package org.gbif.pipelines.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Builder;
import lombok.Data;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.SparkJobConfig;

public class SparkConfUtil {

  public static boolean evaluate(String expression, long value) {
    // Remove spaces
    expression = expression.replace(" ", "");

    Long lower = null;
    Long upper = null;
    boolean lowerInclusive = true;
    boolean upperInclusive = false;

    if (expression.contains("<=")
        && expression.contains("<")
        && expression.indexOf("<=") < expression.lastIndexOf("<")) {
      // A <= x < B
      String[] parts = expression.split("<=|<");
      lower = Long.parseLong(parts[0].replace("_", ""));
      upper = Long.parseLong(parts[2].replace("_", ""));
      lowerInclusive = true;
      upperInclusive = false;
    } else if (expression.contains("<=")) {
      // A <= x
      String[] parts = expression.split("<=");
      lower = Long.parseLong(parts[0].replace("_", ""));
      lowerInclusive = true;
    } else if (expression.contains("<")) {
      // x < B
      String[] parts = expression.split("<");
      upper = Long.parseLong(parts[1].replace("_", ""));
      upperInclusive = false;
    } else if (expression.contains(">")) {
      // x > B
      String[] parts = expression.split(">");
      lower = Long.parseLong(parts[1].replace("_", ""));
      lowerInclusive = false;
    } else {
      throw new IllegalArgumentException("Invalid expression: " + expression);
    }

    boolean ok = true;
    if (lower != null) {
      if (lowerInclusive) {
        ok = ok && (lower <= value);
      } else {
        ok = ok && (lower < value);
      }
    }
    if (upper != null) {
      if (upperInclusive) {
        ok = ok && (value <= upper);
      } else {
        ok = ok && (value < upper);
      }
    }

    return ok;
  }

  public static Conf createConf(
      PipelinesConfig pipelinesConfig,
      String datasetId,
      int attempt,
      String sparkAppName,
      long recordsNumber,
      List<String> extraArgs) {

    Map<String, SparkJobConfig> configs = pipelinesConfig.getProcessingConfigs();

    SparkJobConfig baseConf = null;
    String confDescription = null;
    if (recordsNumber < 0) {
      throw new IllegalArgumentException("Number of records must be greater than zero");
    }

    Set<String> expressions = configs.keySet();
    for (String expression : expressions) {
      if (evaluate(expression, recordsNumber)) {
        baseConf = configs.get(expression);
        confDescription = expression;
        break;
      }
    }

    if (baseConf == null) {
      throw new RuntimeException(
          String.format(
              "No base configuration found for dataset {%s}, records {%d}",
              datasetId, recordsNumber));
    }

    List<String> combinedArgs = new ArrayList<>(extraArgs);
    combinedArgs.add("--datasetId=" + datasetId);
    combinedArgs.add("--attempt=" + attempt);
    combinedArgs.add("--appName=" + sparkAppName);
    combinedArgs.add("--numberOfShards=" + baseConf.numberOfShards);
    combinedArgs.addAll(baseConf.getArgs());

    return Conf.builder()
        .description(confDescription)
        .args(combinedArgs)
        .numberOfShards(baseConf.numberOfShards)
        .driverMemoryOverheadFactor(baseConf.driverMemoryOverheadFactor)
        .driverCores(baseConf.driverCores)
        .executorMemoryOverheadFactor(baseConf.executorMemoryOverheadFactor)
        .executorInstances(baseConf.executorInstances)
        .executorCores(baseConf.executorCores)
        .defaultParallelism(baseConf.defaultParallelism)
        .driverMinCpu(baseConf.driverMinCpu)
        .driverMaxCpu(baseConf.driverMaxCpu)
        .driverLimitMemory(baseConf.driverLimitMemory)
        .executorMinCpu(baseConf.executorMinCpu)
        .executorMaxCpu(baseConf.executorMaxCpu)
        .executorLimitMemory(baseConf.executorLimitMemory)
        .build();
  }

  public static int getNumberOfShards(PipelinesConfig pipelinesConfig, Long recordsNumber) {
    Map<String, SparkJobConfig> configs = pipelinesConfig.getProcessingConfigs();

    SparkJobConfig baseConf = null;
    if (recordsNumber < 0) {
      throw new IllegalArgumentException("Number of records must be greater than zero");
    }

    Set<String> expressions = configs.keySet();
    for (String expression : expressions) {
      if (evaluate(expression, recordsNumber)) {
        baseConf = configs.get(expression);
        break;
      }
    }

    if (baseConf == null) {
      throw new RuntimeException(
          String.format("No base configuration found for  records {%d}", recordsNumber));
    }

    return baseConf.numberOfShards;
  }

  @Data
  @Builder
  public static class Conf {

    private final String description;

    // command line args
    private final List<String> args;

    public int numberOfShards;

    // spark settings
    public final String driverMemoryOverheadFactor;
    public final int driverCores;
    public final String executorMemoryOverheadFactor;
    public final int executorInstances;
    public final int executorCores;
    public final int defaultParallelism; // should be same as number of shards

    // kubernetes settings
    public final String driverMinCpu;
    public final String driverMaxCpu;
    public final String driverLimitMemory;

    public final String executorMinCpu;
    public final String executorMaxCpu;

    public final String executorLimitMemory;
  }
}
