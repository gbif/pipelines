package au.org.ala.utils;

import static java.util.Collections.reverseOrder;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.*;
import org.gbif.pipelines.ingest.utils.FileSystemFactory;
import org.yaml.snakeyaml.Yaml;

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

  public static void main(String[] args) throws Exception {

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

  public void run() throws Exception {

    FileSystem fs = FileSystemFactory.getInstance(hdfsSiteConfig, coreSiteConfig).getFs(inputPath);
    Map<String, Long> counts = new HashMap<String, Long>();

    FileStatus[] fileStatuses = fs.listStatus(new Path(inputPath));
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        String datasetID =
            fileStatus
                .getPath()
                .toString()
                .substring(fileStatus.getPath().toString().lastIndexOf("/") + 1);
        Path metrics = new Path(fileStatus.getPath().toString() + "/1/dwca-metrics.yml");
        if (fs.exists(metrics)) {
          // read YAML
          Yaml yaml = new Yaml();
          // the YAML files created by metrics are UTF-16 encoded
          Map<String, Object> yamlObject =
              yaml.load(new InputStreamReader(fs.open(metrics), StandardCharsets.UTF_16));
          counts.put(
              datasetID,
              Long.parseLong(yamlObject.getOrDefault("archiveToErCountAttempted", 0L).toString()));
        }
      }
    }

    // order by size descending
    List<Map.Entry<String, Long>> list = new ArrayList<>(counts.entrySet());
    list.sort(reverseOrder(Map.Entry.comparingByValue()));

    FileWriter fw = new FileWriter(targetPath);
    for (Map.Entry<String, Long> entry : list) {
      fw.write(entry.getKey() + "," + entry.getValue() + "\n");
    }
    fw.flush();
    fw.close();
  }
}
