package org.gbif.pipelines.common.beam.utils;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.DIRECTORY_NAME;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.MEASUREMENT_OR_FACT_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE_HDFS_RECORD;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.common.beam.options.BasePipelineOptions;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PathBuilder {

  /** Build a {@link Path} from an array of string values using path separator. */
  public static Path buildPath(String... values) {
    return new Path(String.join(Path.SEPARATOR, values));
  }

  /**
   * Uses pattern for path - "{targetPath}/{datasetId}/{attempt}/{name}"
   *
   * @return string path
   */
  public static String buildDatasetAttemptPath(
      BasePipelineOptions options, String name, boolean isInput) {
    return buildPath(
            isInput ? options.getInputPath() : options.getTargetPath(),
            options.getDatasetId() == null || "all".equalsIgnoreCase(options.getDatasetId())
                ? "*"
                : options.getDatasetId(),
            options.getAttempt().toString(),
            name.toLowerCase())
        .toString();
  }

  /**
   * Uses pattern for path -
   * "{targetPath}/{datasetId}/{attempt}/interpreted/{name}/interpret-{uniqueId}"
   *
   * @return string path to interpretation
   */
  public static String buildPathInterpretUsingTargetPath(
      BasePipelineOptions options, String name, String uniqueId) {
    return buildPath(
            buildDatasetAttemptPath(options, DIRECTORY_NAME, false),
            name,
            PipelinesVariables.Pipeline.Interpretation.FILE_NAME + uniqueId)
        .toString();
  }

  /**
   * Uses pattern for path -
   * "{targetPath}/{datasetId}/{attempt}/interpreted/{name}/interpret-{uniqueId}"
   *
   * @return string path to interpretation
   */
  public static String buildPathInterpretUsingInputPath(
      BasePipelineOptions options, String name, String uniqueId) {
    return buildPath(
            buildDatasetAttemptPath(options, DIRECTORY_NAME, true),
            name,
            PipelinesVariables.Pipeline.Interpretation.FILE_NAME + uniqueId)
        .toString();
  }

  /**
   * Builds the target base path of the Occurrence hdfs view.
   *
   * @param options options pipeline options
   * @return path to the directory where the occurrence hdfs view is stored
   */
  public static String buildFilePathHdfsViewUsingInputPath(
      BasePipelineOptions options, String uniqueId) {
    return buildPath(
            buildPathHdfsViewUsingInputPath(options),
            PipelinesVariables.Pipeline.HdfsView.VIEW_OCCURRENCE + "_" + uniqueId)
        .toString();
  }

  /**
   * Builds the target base path of the Occurrence hdfs view.
   *
   * @param options options pipeline options
   * @return path to the directory where the occurrence hdfs view is stored
   */
  public static String buildPathHdfsViewUsingInputPath(BasePipelineOptions options) {
    return buildPath(
            buildDatasetAttemptPath(options, DIRECTORY_NAME, true),
            OCCURRENCE_HDFS_RECORD.name().toLowerCase())
        .toString();
  }

  /**
   * Builds the target base path of the MeasurementOrFactTable hdfs view.
   *
   * @param options options pipeline options
   * @return path to the directory where the MeasurementOrFactTable hdfs view is stored
   */
  public static String buildFilePathMoftUsingInputPath(
      BasePipelineOptions options, String uniqueId) {
    return buildPath(
            buildPathMoftUsingInputPath(options),
            PipelinesVariables.Pipeline.HdfsView.VIEW_MOFT + "_" + uniqueId)
        .toString();
  }

  /**
   * Builds the target base path of the MeasurementOrFactTable hdfs view.
   *
   * @param options options pipeline options
   * @return path to the directory where the MeasurementOrFactTable hdfs view is stored
   */
  public static String buildPathMoftUsingInputPath(BasePipelineOptions options) {
    return buildPath(
            buildDatasetAttemptPath(options, DIRECTORY_NAME, true),
            MEASUREMENT_OR_FACT_TABLE.name().toLowerCase())
        .toString();
  }

  /**
   * Gets temporary directory from options or returns default value.
   *
   * @return path to a temporary directory.
   */
  public static String getTempDir(BasePipelineOptions options) {
    return Strings.isNullOrEmpty(options.getTempLocation())
        ? buildPath(options.getTargetPath(), "tmp").toString()
        : options.getTempLocation();
  }
}
