package org.gbif.pipelines.airflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;
import org.gbif.pipelines.util.SparkConfUtil;

@Data
@Builder
public class AirflowBody {

  @JsonProperty("dag_run_id")
  private final String dagRunId;

  private final SparkConfUtil.Conf conf;
}
