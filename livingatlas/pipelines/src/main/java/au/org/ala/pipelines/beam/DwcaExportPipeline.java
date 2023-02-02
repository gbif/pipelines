package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.options.DwCAExportPipelineOptions;
import au.org.ala.pipelines.spark.PredicateExportDwCAPipeline;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ArchiveUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;

/** Single runnable that can be used to export both occurrence and event based archives. */
@Slf4j
public class DwcaExportPipeline {

  public static void main(String[] args) throws Exception {
    VersionInfo.print();
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "export");
    DwCAExportPipelineOptions options =
        PipelinesOptionsFactory.create(DwCAExportPipelineOptions.class, combinedArgs);

    if (ArchiveUtils.isEventCore(options) && options.getPredicateExportEnabled()) {
      log.info("Running events export pipeline");
      PredicateExportDwCAPipeline.main(args);
    } else {
      log.info("Running occurrence export pipeline");
      IndexRecordToDwcaPipeline.main(args);
    }
  }
}
