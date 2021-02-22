package org.gbif.pipelines.ingest.utils;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_MOFT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_MOFT_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_OCCURRENCE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_OCCURRENCE_DIR;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.utils.FsUtils;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HdfsViewAvroUtils {

  /**
   * Copies all occurrence records into the directory from targetPath. Deletes pre-existing data of
   * the dataset being processed.
   */
  public static void move(InterpretationPipelineOptions options) {

    // OccurrenceHdfsRecord
    move(
        options,
        VIEW_OCCURRENCE_DIR,
        VIEW_OCCURRENCE,
        PathBuilder.buildFilePathHdfsViewUsingInputPath(options, "*.avro"));

    // MeasurementOrFactTable
    move(
        options,
        VIEW_MOFT_DIR,
        VIEW_MOFT,
        PathBuilder.buildFilePathMoftUsingInputPath(options, "*.avro"));
  }

  private static void move(
      InterpretationPipelineOptions options, String viewDir, String view, String filter) {
    String targetPath = options.getTargetPath();

    String deletePath =
        PathBuilder.buildPath(targetPath, viewDir, view + "_" + options.getDatasetId() + "_*")
            .toString();
    log.info("Deleting avro files {}", deletePath);
    FsUtils.deleteByPattern(
        options.getHdfsSiteConfig(), options.getCoreSiteConfig(), targetPath, deletePath);

    String movePath = PathBuilder.buildPath(targetPath, viewDir).toString();
    log.info("Moving files with pattern {} to {}", filter, movePath);
    FsUtils.moveDirectory(
        options.getHdfsSiteConfig(), options.getCoreSiteConfig(), movePath, filter);
    log.info("Files moved to {} directory", movePath);
  }
}
