package org.gbif.pipelines.airflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class AirflowBody {

  @JsonProperty("dag_run_id")
  private final String dagRunId;

  private final AirflowConfFactory.Conf conf;
}
