package org.gbif.pipelines.spark.pojo;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

@Data
@Builder
@ToString
public class RawRecord {
  private String key;
  private String recordBody;
  private String hashValue;
  private Long createdDate;
}
