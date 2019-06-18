package org.gbif.pipelines.hive;

import org.gbif.pipelines.io.avro.BasicRecord;

import java.util.Map;
import java.util.UUID;

public class DatasetInterpretation {

  private final UUID datasetKey;

  private final Map<? extends BasicRecord, String> interpretationPaths;

  private final String extendedRecordPath;

  public DatasetInterpretation(UUID datasetKey, Map<? extends BasicRecord, String> interpretationPaths,
                               String extendedRecordPath
  ) {
    this.datasetKey = datasetKey;
    this.interpretationPaths = interpretationPaths;
    this.extendedRecordPath = extendedRecordPath;
  }

  public UUID getDatasetKey() {
    return datasetKey;
  }

  public Map<? extends BasicRecord, String> getInterpretationPaths() {
    return interpretationPaths;
  }

  public String getExtendedRecordPath() {
    return extendedRecordPath;
  }
}
