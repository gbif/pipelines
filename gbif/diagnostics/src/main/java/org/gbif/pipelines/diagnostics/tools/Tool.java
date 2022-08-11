package org.gbif.pipelines.diagnostics.tools;

import com.beust.jcommander.JCommander;
import lombok.Getter;

public interface Tool {
  enum CliTool {
    MIGRATOR(IdentifiersMigratorTool.builder().build()),
    REPAIR(RepairGbifIDLookupTool.builder().build()),
    LOOKUP(RecordIDsByCrawlAttemptTool.builder().build());

    @Getter private final Tool tool;

    CliTool(Tool tool) {
      this.tool = tool;
    }
  }

  default boolean getHelp() {
    return false;
  }

  default void check(JCommander jc) {}

  default void run() {}

  default void run(JCommander jc) {
    check(jc);
    run();
  }
}
