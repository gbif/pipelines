package org.gbif.pipelines.base.pipelines;

import org.gbif.pipelines.base.options.IndexingPipelineOptions;
import org.gbif.pipelines.base.options.PipelinesOptionsFactory;
import org.gbif.pipelines.base.utils.FsUtils;
import org.gbif.pipelines.base.utils.IndexingUtils;

/** TODO: DOC */
public class IndexingWithCreationPipeline {

  private IndexingWithCreationPipeline() {}

  /** TODO: DOC */
  public static void main(String[] args) {
    IndexingPipelineOptions options = PipelinesOptionsFactory.createIndexing(args);
    IndexingWithCreationPipeline.createAndRun(options);
  }

  /** TODO: DOC */
  public static void createAndRun(IndexingPipelineOptions options) {

    IndexingUtils.createIndex(options);

    IndexingPipeline.createAndRun(options);

    IndexingUtils.swapIndex(options);

    FsUtils.removeTmpDirecrory(options);
  }
}
