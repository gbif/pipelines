package org.gbif.pipelines.fragmenter.habse;

import org.apache.hadoop.hbase.client.Row;

import lombok.NoArgsConstructor;

@NoArgsConstructor(staticName = "create")
public class FragmentRow implements Row {

  @Override
  public byte[] getRow() {
    return new byte[0];
  }

  @Override
  public int compareTo(Row row) {
    return 0;
  }
}
