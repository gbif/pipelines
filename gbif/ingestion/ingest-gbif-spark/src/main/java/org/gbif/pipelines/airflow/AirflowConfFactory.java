package org.gbif.pipelines.airflow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.SparkJobConfig;

@Getter
@Slf4j
public class AirflowConfFactory {

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
              "No base configuration found for dataset {}, records {}", datasetId, recordsNumber));
    }

    List<String> combinedArgs = new ArrayList<>(extraArgs);
    combinedArgs.add("--datasetId=" + datasetId);
    combinedArgs.add("--attempt=" + attempt);
    combinedArgs.add("--appName=" + sparkAppName);
    combinedArgs.addAll(baseConf.getArgs());

    return Conf.builder()
        .description(confDescription)
        .args(combinedArgs)
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
    if (lower != null) ok &= lowerInclusive ? (lower <= value) : (lower < value);
    if (upper != null) ok &= upperInclusive ? (value <= upper) : (value < upper);

    return ok;
  }

  @Data
  @Builder
  public static class Conf {

    private final String description;

    // command line args
    private final List<String> args;

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

  public static void main(String[] args) throws Exception {
    int recordNumber = 1234;

    System.out.println(evaluate("0 <= recordNumber < 5000", recordNumber)); // true
    System.out.println(evaluate("5000 <= recordNumber < 50_000", recordNumber)); // false
    System.out.println(evaluate("1000 <= recordNumber", recordNumber)); // true
    System.out.println(evaluate("recordNumber < 2000", recordNumber)); // true
    System.out.println(evaluate("recordNumber > 1000", recordNumber)); // true
    System.out.println(evaluate("recordNumber > 2000", recordNumber)); // false
    System.out.println(evaluate("recordCount < 100_000", 99999)); // false
    System.out.println(evaluate("recordCount < 100_000", 100_001)); // false
  }
}
