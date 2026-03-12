package org.gbif.pipelines.spark.pojo;

import lombok.Data;
import lombok.ToString;

@ToString
@Data
public class HdfsColumn {
  String select;
  String icebergCol;
}
