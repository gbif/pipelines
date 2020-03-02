package org.gbif.pipelines.fragmenter.habse;

import org.apache.hadoop.hbase.client.Row;

import lombok.AllArgsConstructor;

@AllArgsConstructor(staticName = "create")
public class FragmentRow implements Row {

  private String key;
  private String value;

  @Override
  public byte[] getRow() {
    return new byte[0];
  }

  @Override
  public int compareTo(Row row) {
    return 0;
  }
}
