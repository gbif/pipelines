package au.org.ala.utils;

import static java.util.Collections.reverseOrder;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.*;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.jetbrains.annotations.NotNull;

@Parameters(separators = "=")
@Slf4j
public class DumpDatasetSize {

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
        new CombinedYamlConfiguration(args).toArgs("general", "dataset-count-dump");

    DumpDatasetSize m = new DumpDatasetSize();
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

    FileSystem fs =
        FileSystemFactory.getInstance(HdfsConfigs.create(hdfsSiteConfig, coreSiteConfig))
            .getFs(inputPath);
    Map<String, Long> counts = readDatasetCounts(fs, inputPath);

    // order by size descending
    List<Map.Entry<String, Long>> list = new ArrayList<>(counts.entrySet());
    list.sort(reverseOrder(Map.Entry.comparingByValue()));

    try (FileWriter fw = new FileWriter(targetPath)) {
      for (Map.Entry<String, Long> entry : list) {
        fw.write(entry.getKey() + "," + entry.getValue() + "\n");
      }
      fw.flush();
    }
    log.info("Dataset list of size {} written to {}", list.size(), targetPath);
  }

  @NotNull
  public static Map<String, Long> readDatasetCounts(FileSystem fs, String inputPath)
      throws IOException {
    Map<String, Long> counts = new HashMap<>();
    FileStatus[] fileStatuses = fs.listStatus(new Path(inputPath));
    log.info("Datasets counts from {}", inputPath);
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        String datasetID =
            fileStatus
                .getPath()
                .toString()
                .substring(fileStatus.getPath().toString().lastIndexOf('/') + 1);
        try {
          counts.put(datasetID, ValidationUtils.readVerbatimCount(fs, inputPath, datasetID, 1));
        } catch (Exception e) {
          log.error("Dataset count of {} failed", datasetID);
          throw (e);
        }
      }
    }
    return counts;
  }
}
