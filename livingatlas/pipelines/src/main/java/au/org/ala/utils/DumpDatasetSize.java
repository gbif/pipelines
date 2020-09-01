package au.org.ala.utils;

import static java.util.Collections.reverseOrder;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.*;
import org.gbif.pipelines.ingest.utils.FileSystemFactory;

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
        counts.put(
            datasetID, ValidationUtils.readVerbatimCount(fs, fileStatus.getPath().toString(), 1));
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
