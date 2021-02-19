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
    // Moving files to the directory of latest records
    String targetPath = options.getTargetPath();

    // Occurrence
    String deleteOccurrencePath =
        PathBuilder.buildPath(
                targetPath,
                VIEW_OCCURRENCE_DIR,
                VIEW_OCCURRENCE + "_" + options.getDatasetId() + "_*")
            .toString();
    log.info("Deleting avro files {}", deleteOccurrencePath);
    FsUtils.deleteByPattern(
        options.getHdfsSiteConfig(), options.getCoreSiteConfig(), targetPath, deleteOccurrencePath);
    String filterOccurrence = PathBuilder.buildFilePathHdfsViewUsingInputPath(options, "*.avro");

    log.info("Moving files with pattern {} to {}", filterOccurrence, targetPath);
    FsUtils.moveDirectory(
        options.getHdfsSiteConfig(), options.getCoreSiteConfig(), targetPath, filterOccurrence);
    log.info("Files moved to {} directory", targetPath);

    // MeasurementOrFactTable
    String deleteMoftPath =
        PathBuilder.buildPath(
                targetPath, VIEW_MOFT_DIR, VIEW_MOFT + "_" + options.getDatasetId() + "_*")
            .toString();
    log.info("Deleting avro files {}", deleteMoftPath);
    FsUtils.deleteByPattern(
        options.getHdfsSiteConfig(), options.getCoreSiteConfig(), targetPath, deleteMoftPath);
    String filterMoft = PathBuilder.buildFilePathMoftUsingInputPath(options, "*.avro");

    log.info("Moving files with pattern {} to {}", filterMoft, targetPath);
    FsUtils.moveDirectory(
        options.getHdfsSiteConfig(), options.getCoreSiteConfig(), targetPath, filterMoft);
    log.info("Files moved to {} directory", targetPath);
  }
}
