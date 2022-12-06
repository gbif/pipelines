package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ArchiveUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.common.beam.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;

/** Single runnable that can be used to export both occurrence and event based archives. */
@Slf4j
public class ElasticPipeline {
  public static void main(String[] args) throws Exception {
    VersionInfo.print();
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "elastic");
    EsIndexingPipelineOptions options =
        PipelinesOptionsFactory.create(EsIndexingPipelineOptions.class, combinedArgs);
    if (ArchiveUtils.isEventCore(options)) {
      log.info("Running events export pipeline");
      ALAEventToEsIndexPipeline.main(args);
    } else {
      log.info("Running occurrence export pipeline");
      ALAOccurrenceToEsIndexPipeline.main(args);
    }
  }
}
