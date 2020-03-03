package org.gbif.pipelines.fragmenter.common;

import org.apache.hadoop.hbase.TableName;

import lombok.Getter;

@Getter
public class FragmentsConfiguration {

  public FragmentsConfiguration(String tableName) {
    this.tableName = TableName.valueOf(tableName);
  }

  public static FragmentsConfiguration create(String tableName) {
    return new FragmentsConfiguration(tableName);
  }

  private TableName tableName;

}
