package org.gbif.pipelines.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Builder;
import lombok.Data;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.SparkJobConfig;

/**
 * Loads config for Spark jobs based on the number of records to process. The config is defined in
 * pipelines-config.yaml
 *
 * <p>Supported expression formats:
 *
 * <ul>
 *   <li>Compound: {@code A <= x < B}, {@code A < x < B}, {@code A <= x <= B}, {@code A < x <= B}
 *   <li>Compound (reversed): {@code B > x > A}, {@code B >= x > A}, {@code B > x >= A}, {@code B >=
 *       x >= A}
 *   <li>Single lower: {@code A <= x}, {@code A < x}, {@code x >= A}, {@code x > A}
 *   <li>Single upper: {@code x < B}, {@code x <= B}, {@code A > x}, {@code A >= x}
 * </ul>
 */
public class SparkConfUtil {

  // Matches <=, >=, <, > in that precedence order
  private static final Pattern OP_PATTERN = Pattern.compile("<=|>=|<|>");

  public static boolean evaluate(String expression, long value) {
    // Remove spaces
    expression = expression.replace(" ", "");

    Long lower = null;
    Long upper = null;
    boolean lowerInclusive = false;
    boolean upperInclusive = false;

    // Collect all operators
    Matcher matcher = OP_PATTERN.matcher(expression);
    List<String> ops = new ArrayList<>();
    while (matcher.find()) {
      ops.add(matcher.group());
    }

    // Split into operands (numeric values or variable placeholder)
    String[] parts = expression.split("<=|>=|<|>");

    if (ops.size() == 2) {
      // Compound expression: A OP x OP B  or  B OP x OP A
      String leftOp = ops.get(0);
      String rightOp = ops.get(1);
      boolean leftOpIsLess = leftOp.equals("<") || leftOp.equals("<=");
      boolean rightOpIsLess = rightOp.equals("<") || rightOp.equals("<=");

      if (leftOpIsLess && rightOpIsLess) {
        // A <= x < B  /  A < x < B  /  A <= x <= B  /  A < x <= B
        lower = Long.parseLong(parts[0].replace("_", ""));
        upper = Long.parseLong(parts[2].replace("_", ""));
        lowerInclusive = leftOp.equals("<=");
        upperInclusive = rightOp.equals("<=");
      } else if (!leftOpIsLess && !rightOpIsLess) {
        // B > x > A  /  B >= x > A  /  B > x >= A  /  B >= x >= A
        upper = Long.parseLong(parts[0].replace("_", ""));
        lower = Long.parseLong(parts[2].replace("_", ""));
        upperInclusive = leftOp.equals(">=");
        lowerInclusive = rightOp.equals(">=");
      } else {
        throw new IllegalArgumentException(
            "Mixed operator directions in compound expression: " + expression);
      }
    } else if (ops.size() == 1) {
      String op = ops.get(0);
      boolean leftIsNumber = isNumber(parts[0]);

      if (op.equals("<") || op.equals("<=")) {
        if (leftIsNumber) {
          // A < x  or  A <= x  → lower bound
          lower = Long.parseLong(parts[0].replace("_", ""));
          lowerInclusive = op.equals("<=");
        } else {
          // x < B  or  x <= B  → upper bound
          upper = Long.parseLong(parts[1].replace("_", ""));
          upperInclusive = op.equals("<=");
        }
      } else if (op.equals(">") || op.equals(">=")) {
        if (leftIsNumber) {
          // A > x  or  A >= x  → upper bound  (equivalent to x < A or x <= A)
          upper = Long.parseLong(parts[0].replace("_", ""));
          upperInclusive = op.equals(">=");
        } else {
          // x > A  or  x >= A  → lower bound
          lower = Long.parseLong(parts[1].replace("_", ""));
          lowerInclusive = op.equals(">=");
        }
      } else {
        throw new IllegalArgumentException("Invalid expression: " + expression);
      }
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
    validateConfigs(configs);

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

  private static void validateConfigs(Map<String, SparkJobConfig> configs) {
    for (SparkJobConfig config : configs.values()) {
      validateSparkJobConf(config, "config with description: " + config.getArgs());
    }
  }

  private static void validateSparkJobConf(SparkJobConfig conf, String sparkAppName) {

    validatePositive("numberOfShards", conf.getNumberOfShards(), conf, sparkAppName);

    if (!conf.standalone) {

      // check sensible values for spark properties
      validatePositive("executorInstances", conf.getExecutorInstances(), conf, sparkAppName);
      validatePositive("driverCores", conf.getDriverCores(), conf, sparkAppName);
      validatePositive("executorCores", conf.getExecutorCores(), conf, sparkAppName);
      validatePositive("defaultParallelism", conf.getDefaultParallelism(), conf, sparkAppName);

      validateNonBlank(
          "driverMemoryOverheadFactor", conf.getDriverMemoryOverheadFactor(), conf, sparkAppName);
      validateNonBlank(
          "executorMemoryOverheadFactor",
          conf.getExecutorMemoryOverheadFactor(),
          conf,
          sparkAppName);

      validateNonBlank("driverMinCpu", conf.getDriverMinCpu(), conf, sparkAppName);
      validateNonBlank("driverMaxCpu", conf.getDriverMaxCpu(), conf, sparkAppName);
      validateNonBlank("driverLimitMemory", conf.getDriverLimitMemory(), conf, sparkAppName);

      validateNonBlank("executorMinCpu", conf.getExecutorMinCpu(), conf, sparkAppName);
      validateNonBlank("executorMaxCpu", conf.getExecutorMaxCpu(), conf, sparkAppName);
      validateNonBlank("executorLimitMemory", conf.getExecutorLimitMemory(), conf, sparkAppName);
    }
  }

  private static void validatePositive(
      String fieldName, int value, SparkJobConfig conf, String sparkAppName) {
    if (value <= 0) {
      throw invalidConf(fieldName + " must be > 0, but was " + value, conf, sparkAppName);
    }
  }

  private static void validateNonBlank(
      String fieldName, String value, SparkJobConfig conf, String sparkAppName) {
    if (value == null || value.trim().isEmpty()) {
      throw invalidConf(fieldName + " must not be null/blank", conf, sparkAppName);
    }
  }

  private static IllegalStateException invalidConf(
      String reason, SparkJobConfig conf, String confName) {
    return new IllegalStateException(
        String.format("Invalid Spark config: %s. Config: %s", reason, confName));
  }

  /** Returns true if the string (after removing underscore separators) represents a valid long. */
  private static boolean isNumber(String s) {
    try {
      Long.parseLong(s.replace("_", ""));
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
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
