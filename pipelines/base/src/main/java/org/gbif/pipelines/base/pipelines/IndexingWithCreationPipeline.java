package org.gbif.pipelines.base.pipelines;

import org.gbif.pipelines.base.options.IndexingPipelineOptions;
import org.gbif.pipelines.base.options.PipelinesOptionsFactory;
import org.gbif.pipelines.base.utils.IndexingUtils;
import org.gbif.pipelines.estools.client.EsConfig;

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
    EsConfig config = EsConfig.from(options.getEsHosts());

    IndexingUtils.createIndex(options, config);

    IndexingPipeline.createAndRun(options);

    IndexingUtils.swapIndex(options, config);
    IndexingUtils.removeTmpDirecrory(options);
  }
}
