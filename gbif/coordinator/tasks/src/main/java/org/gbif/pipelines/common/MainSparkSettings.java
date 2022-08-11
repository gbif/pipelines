package org.gbif.pipelines.common;

public interface MainSparkSettings {

  int getParallelism();

  String getExecutorMemory();

  int getExecutorNumbers();
}
