package org.gbif.pipelines.ingest.utils;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_OCCURRENCE;

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

    String deletePath =
        PathBuilder.buildPath(targetPath, VIEW_OCCURRENCE + "_" + options.getDatasetId() + "_*")
            .toString();
    log.info("Deleting avro files {}", deletePath);
    FsUtils.deleteByPattern(
        options.getHdfsSiteConfig(), options.getCoreSiteConfig(), targetPath, deletePath);
    String filter = PathBuilder.buildFilePathHdfsViewUsingInputPath(options, "*.avro");

    log.info("Moving files with pattern {} to {}", filter, targetPath);
    FsUtils.moveDirectory(
        options.getHdfsSiteConfig(), options.getCoreSiteConfig(), targetPath, filter);
    log.info("Files moved to {} directory", targetPath);
  }
}
