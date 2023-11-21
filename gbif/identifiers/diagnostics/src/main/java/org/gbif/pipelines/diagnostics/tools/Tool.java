package org.gbif.pipelines.diagnostics.tools;

import com.beust.jcommander.JCommander;
import lombok.AllArgsConstructor;
import lombok.Getter;

public interface Tool {
  @Getter
  @AllArgsConstructor
  enum CliTool {
    MIGRATOR(IdentifiersMigratorTool.builder().build()),
    REPAIR(RepairGbifIDLookupTool.builder().build()),
    LOOKUP(RecordIDsByCrawlAttemptTool.builder().build()),
    ID_LOOKUP(DatasetIDsLookupTool.builder().build());

    private final Tool tool;
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
