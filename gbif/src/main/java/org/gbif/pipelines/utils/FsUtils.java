package org.gbif.pipelines.utils;

import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.assembling.interpretation.steps.PipelineTargetPaths;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;

import java.util.Arrays;
import java.util.StringJoiner;

/** Utility class to work with FS. */
public final class FsUtils {

  private static final String DATA_FILENAME = "interpreted";
  private static final String ISSUES_FOLDER = "issues";
  private static final String ISSUES_FILENAME = "issues";

  private FsUtils() {}

  /** Build a {@link Path} from an array of string values. */
  public static Path buildPath(String... values) {
    StringJoiner joiner = new StringJoiner(Path.SEPARATOR);
    Arrays.stream(values).forEach(joiner::add);
    return new Path(joiner.toString());
  }

  public static String buildPathString(String... values) {
    return buildPath(values).toString();
  }

  public static PipelineTargetPaths createPaths(
      DataProcessingPipelineOptions options, String interType) {
    PipelineTargetPaths paths = new PipelineTargetPaths();

    String targetDirectory = options.getDefaultTargetDirectory();
    String datasetId = options.getDatasetId();
    String attempt = options.getAttempt().toString();
    String type = interType.toLowerCase();

    String path = FsUtils.buildPathString(targetDirectory, datasetId, attempt, type);

    paths.setDataTargetPath(FsUtils.buildPathString(path, DATA_FILENAME));
    paths.setIssuesTargetPath(FsUtils.buildPathString(path, ISSUES_FOLDER, ISSUES_FILENAME));

    paths.setTempDir(options.getHdfsTempLocation());

    return paths;
  }
}
