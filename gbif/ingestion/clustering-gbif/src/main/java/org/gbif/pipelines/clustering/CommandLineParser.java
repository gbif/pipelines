package org.gbif.pipelines.clustering;

import java.util.Arrays;
import java.util.List;

/** Utility to read the named arguments. */
class CommandLineParser {

  static Cluster.ClusterBuilder parse(String[] args) {
    if (args.length != 16) {
      String provided = args.length == 0 ? "no args" : String.join(" ", args);
      throw new IllegalArgumentException("Incorrect configuration provided: " + provided);
    }
    Cluster.ClusterBuilder builder = nextOption(new Cluster.ClusterBuilder(), Arrays.asList(args));
    System.out.println("Clustering started with configuration: " + builder);
    return builder;
  }

  private static Cluster.ClusterBuilder nextOption(
      Cluster.ClusterBuilder builder, List<String> list) {
    if (list.isEmpty()) return builder;

    String option = list.get(0);
    List<String> tail = list.subList(1, list.size());
    if (option.startsWith("-")) {
      switch (option) {
        case "--hive-db":
          return nextOption(builder.hiveDB(tail.get(0)), tail.subList(1, tail.size()));
        case "--source-table":
          return nextOption(builder.sourceTable(tail.get(0)), tail.subList(1, tail.size()));
        case "--hive-table-prefix":
          return nextOption(builder.hiveTablePrefix(tail.get(0)), tail.subList(1, tail.size()));
        case "--hbase-table":
          return nextOption(builder.hbaseTable(tail.get(0)), tail.subList(1, tail.size()));
        case "--hbase-regions":
          return nextOption(
              builder.hbaseRegions(Integer.parseInt(tail.get(0))), tail.subList(1, tail.size()));
        case "--hbase-zk":
          return nextOption(builder.hbaseZK(tail.get(0)), tail.subList(1, tail.size()));
        case "--target-dir":
          return nextOption(builder.targetDir(tail.get(0)), tail.subList(1, tail.size()));
        case "--hash-count-threshold":
          return nextOption(
              builder.hashCountThreshold(Integer.parseInt(tail.get(0))),
              tail.subList(1, tail.size()));
        default:
          throw new IllegalArgumentException("Unknown option " + option);
      }
    } else {
      throw new IllegalArgumentException("Unknown option " + option);
    }
  }
}
