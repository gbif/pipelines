package au.org.ala.utils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.FileWriter;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Parameters(separators = "=")
@Slf4j
public class DumpArchiveList {

  @Parameter(names = "--inputPath", description = "Comma-separated list of group names to be run")
  private String inputPath;

  @Parameter(names = "--targetPath", description = "Comma-separated list of group names to be run")
  private String targetPath;

  @Parameter(
      names = "--hdfsSiteConfig",
      description = "The absolute path to a hdfs-site.xml with default.FS configuration")
  private String hdfsSiteConfig;

  @Parameter(
      names = "--coreSiteConfig",
      description = "The absolute path to a core-site.xml with default.FS configuration")
  private String coreSiteConfig;

  @SneakyThrows
  public static void main(String[] args) {

    String[] combinedArgs =
        new CombinedYamlConfiguration(args).toArgs("general", "dataset-archive-list");

    DumpArchiveList m = new DumpArchiveList();
    JCommander jCommander = JCommander.newBuilder().acceptUnknownOptions(true).addObject(m).build();
    jCommander.parse(combinedArgs);

    if (m.inputPath == null || m.targetPath == null) {
      jCommander.usage();
      System.exit(1);
    }
    m.run();
  }

  @SneakyThrows
  public void run() {

    // load all datasets - return a map of <datasetId -> datasetInputPath>
    Map<String, String> datasets =
        ALAFsUtils.listAllDatasets(hdfsSiteConfig, coreSiteConfig, inputPath);

    // dump to file
    try (FileWriter fw = new FileWriter(targetPath)) {
      for (Map.Entry<String, String> entry : datasets.entrySet()) {
        fw.write(entry.getKey() + "," + entry.getValue() + "\n");
      }
      fw.flush();
    }
    log.info("Datasets listed: {}", datasets.size());
    log.info("List written to: {}", targetPath);
  }
}
