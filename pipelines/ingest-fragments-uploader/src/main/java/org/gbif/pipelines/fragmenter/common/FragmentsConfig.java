package org.gbif.pipelines.fragmenter.common;

import org.apache.hadoop.hbase.TableName;

import lombok.Getter;

@Getter
public class FragmentsConfig {

  public FragmentsConfig(String tableName) {
    this.tableName = TableName.valueOf(tableName);
  }

  public static FragmentsConfig create(String tableName) {
    return new FragmentsConfig(tableName);
  }

  private TableName tableName;

}
