package org.gbif.pipelines.diagnostics.tools;

import com.beust.jcommander.JCommander;

public interface Tool {
  enum CliTool {
    MIGRATOR,
    REPAIR,
    LOOKUP
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
