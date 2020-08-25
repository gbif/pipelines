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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.ingest.utils.FileSystemFactory;
import org.yaml.snakeyaml.Yaml;

@Parameters(separators = "=")
@Slf4j
public class DumpReadyToIndexList {

  @Parameter(names = "--inputPath", description = "Comma-separated list of group names to be run")
  private String inputPath;

  @Parameter(names = "--targetPath", description = "Comma-separated list of group names to be run")
  private String targetPath;

  @Parameter(
      names = "--fullReportPath",
      description = "Comma-separated list of group names to be run")
  private String fullReportPath;

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
        new CombinedYamlConfiguration(args).toArgs("general", "dataset-validated-dump");

    DumpReadyToIndexList m = new DumpReadyToIndexList();
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

    Map<String, Long> readyToIndexCounts = new HashMap<String, Long>();

    FileWriter reportWriter = new FileWriter(fullReportPath);

    reportWriter.write(
        String.join(", ", "datasetID", "recordCount", "verbatimLoaded", "uuidLoaded") + "\n");

    FileStatus[] fileStatuses = fs.listStatus(new Path(inputPath));
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        String datasetID =
            fileStatus
                .getPath()
                .toString()
                .substring(fileStatus.getPath().toString().lastIndexOf("/") + 1);

        // dataset found
        boolean verbatimLoaded = false;
        boolean uuidLoaded = false;
        Long recordCount = 0l;

        Path metrics = new Path(fileStatus.getPath().toString() + "/1/dwca-metrics.yml");
        verbatimLoaded = fs.exists(metrics);

        if (fs.exists(metrics)) {
          // read YAML
          Yaml yaml = new Yaml();
          // the YAML files created by metrics are UTF-16 encoded
          Map<String, Object> yamlObject =
              yaml.load(new InputStreamReader(fs.open(metrics), StandardCharsets.UTF_16));

          recordCount =
              Long.parseLong(yamlObject.getOrDefault("archiveToErCountAttempted", 0L).toString());
        }

        // check UUIDs are generated
        Path uuidMetrics = new Path(fileStatus.getPath().toString() + "/1/uuid-metrics.yml");
        uuidLoaded = fs.exists(uuidMetrics);

        reportWriter.write(
            String.join(
                    ", ",
                    datasetID,
                    recordCount.toString(),
                    Boolean.toString(verbatimLoaded),
                    Boolean.toString(uuidLoaded))
                + "\n");

        if (recordCount > 0 && verbatimLoaded && uuidLoaded) {
          readyToIndexCounts.put(datasetID, recordCount);
        }
      }
    }

    // order by size descending
    List<Map.Entry<String, Long>> list = new ArrayList<>(readyToIndexCounts.entrySet());
    list.sort(reverseOrder(Map.Entry.comparingByValue()));

    FileWriter fw = new FileWriter(targetPath);
    for (Map.Entry<String, Long> entry : list) {
      fw.write(entry.getKey() + "," + entry.getValue() + "\n");
    }
    fw.flush();
    fw.close();

    reportWriter.flush();
    reportWriter.close();
  }
}
