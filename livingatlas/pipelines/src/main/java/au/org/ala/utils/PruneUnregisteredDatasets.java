package au.org.ala.utils;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import au.org.ala.kvs.client.EntityReference;
import au.org.ala.kvs.client.retrofit.ALACollectoryServiceClient;
import au.org.ala.pipelines.options.DatasetListPipelineOptions;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;

@Parameters(separators = "=")
@Slf4j
public class PruneUnregisteredDatasets {

  @Parameter(
      names = "--dwcaImportPath",
      description = "Data directory for new archives e.g. /data/dwc-import")
  private String dwcaImportPath;

  @Parameter(
      names = "--inputPath",
      description = "Data directory for pipelines e.g. /data/pipelines-data")
  private String inputPath;

  @Parameter(
      names = "--hdfsSiteConfig",
      description = "The absolute path to a hdfs-site.xml with default.FS configuration")
  private String hdfsSiteConfig;

  @Parameter(
      names = "--coreSiteConfig",
      description = "The absolute path to a core-site.xml with default.FS configuration")
  private String coreSiteConfig;

  @Parameter(names = "--dry-run", description = "Run the scan and log what would be deleted")
  private boolean dryRun = false;

  @SneakyThrows
  public static void main(String[] args) {

    CombinedYamlConfiguration yaml = new CombinedYamlConfiguration(args);
    String[] combinedArgs = yaml.toArgs("general", "dataset-count-dump");

    DatasetListPipelineOptions options =
        PipelinesOptionsFactory.create(DatasetListPipelineOptions.class, combinedArgs);

    ALAPipelinesConfig config =
        ALAPipelinesConfigFactory.getInstance(
                HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()),
                options.getProperties())
            .get();

    PruneUnregisteredDatasets m = new PruneUnregisteredDatasets();
    JCommander jCommander = JCommander.newBuilder().acceptUnknownOptions(true).addObject(m).build();
    jCommander.parse(combinedArgs);

    if (m.inputPath == null) {
      jCommander.usage();
      System.exit(1);
    }
    m.run(config);
    System.exit(0);
  }

  @SneakyThrows
  public void run(ALAPipelinesConfig config) {

    ALACollectoryServiceClient wsClient = new ALACollectoryServiceClient(config.getCollectory());
    HdfsConfigs hdfsConfigs = HdfsConfigs.create(hdfsSiteConfig, coreSiteConfig);
    FileSystem fs = FileSystemFactory.getInstance(hdfsConfigs).getFs(inputPath);

    // get a list of all registered datasets
    List<String> uids =
        wsClient.listDataResources().stream()
            .map(EntityReference::getUid)
            .collect(Collectors.toList());

    // load all archives - return a map of <datasetId -> datasetInputPath>
    Map<String, String> archives = ALAFsUtils.listAllDatasets(hdfsConfigs, dwcaImportPath);

    Map<String, Long> datasets = DumpDatasetSize.readDatasetCounts(fs, inputPath);

    log.info(
        "Number of registered datasets = {}, number of datasets on disk = {}",
        uids.size(),
        archives.size());

    int count = 0;
    log.warn("Checking archive list");
    for (String uid : archives.keySet()) {
      if (!uids.contains(uid)) {
        // delete
        String filePath = dwcaImportPath + uid;
        log.warn("Deleting dataset {} at {}", uid, filePath);
        if (!dryRun) {
          ALAFsUtils.deleteIfExist(fs, filePath);
        }

        String filePath2 = inputPath + uid;
        log.warn("Deleting dataset {} at {}", uid, filePath2);
        if (!dryRun) {
          ALAFsUtils.deleteIfExist(fs, filePath2);
        }

        count++;
      }
    }

    log.warn("Checking dataset list");
    for (String uid : datasets.keySet()) {
      if (!uids.contains(uid)) {
        // delete
        String filePath = dwcaImportPath + uid;
        log.warn("Deleting dataset {} at {}", uid, filePath);
        if (!dryRun) {
          ALAFsUtils.deleteIfExist(fs, filePath);
        }

        String filePath2 = inputPath + uid;
        log.warn("Deleting dataset {} at {}", uid, filePath2);
        if (!dryRun) {
          ALAFsUtils.deleteIfExist(fs, filePath2);
        }

        count++;
      }
    }

    fs.close();

    log.info("Pruned {} datasets.", count);
  }
}
