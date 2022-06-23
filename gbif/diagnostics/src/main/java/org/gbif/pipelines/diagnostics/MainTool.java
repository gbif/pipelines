package org.gbif.pipelines.diagnostics;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.util.Arrays;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.diagnostics.tools.IdentifiersMigratorTool;
import org.gbif.pipelines.diagnostics.tools.RecordIDsByCrawlAttemptTool;
import org.gbif.pipelines.diagnostics.tools.RepairGbifIDLookupTool;
import org.gbif.pipelines.diagnostics.tools.Tool;

@Slf4j
@Builder
public class MainTool {

  @Parameter(
      names = "--tool",
      description =
          "Available tools: "
              + "MIGRATOR - when you need to migrate GBIF_ID from old occurrence_id to the new occurrence_id "
              + "REPAIR - when there are collisions with GBIF ID for a DWC dataset "
              + "LOOKUP - when you want to print out the values in occurrenceID and the triplet in each crawl attempt DwC-A ")
  @NotNull
  public Tool.CliTool tool;

  public void check(JCommander jc) {
    if (tool == null) {
      jc.usage();
      throw new IllegalArgumentException("--tool can't be null or empty");
    }
  }

  public static void main(String... argv) {

    MainTool main = MainTool.builder().build();
    int index = Arrays.binarySearch(argv, "--tool", String::compareTo);
    String[] mainArgv = {};
    if (index >= 0) {
      mainArgv = Arrays.copyOfRange(argv, index, index + 2);
    }
    main.check(parseArgs(main, mainArgv));

    Tool t;
    switch (main.tool) {
      case LOOKUP:
        t = RecordIDsByCrawlAttemptTool.builder().build();
        break;
      case REPAIR:
        t = RepairGbifIDLookupTool.builder().build();
        break;
      case MIGRATOR:
        t = IdentifiersMigratorTool.builder().build();
        break;
      default:
        throw new IllegalArgumentException(
            "Can't parse --tool key, use LOOKUP, REPAIR or MIGRATOR");
    }

    t.run(parseArgs(t, argv));
  }

  private static JCommander parseArgs(Object tool, String... argv) {
    JCommander jc = JCommander.newBuilder().addObject(tool).build();
    jc.parse(argv);

    if (tool instanceof Tool && ((Tool) tool).getHelp()) {
      jc.usage();
      System.exit(0);
    }
    return jc;
  }
}
