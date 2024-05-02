package org.gbif.pipelines.common.airflow;

import java.util.UUID;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.model.pipelines.StepType;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AppName {

  public static String get(StepType type, UUID datasetKey, int attempt) {
    return String.join("_", type.name(), datasetKey.toString(), String.valueOf(attempt));
  }
}
