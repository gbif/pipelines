package org.gbif.pipelines.fragmenter.common;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HbaseStore {

  private static final byte[] CF_BYTES = Bytes.toBytes("o");
  private static final byte[] V_BYTES = Bytes.toBytes("v");
  private static final byte[] A_BYTES = Bytes.toBytes("a");
  private static final byte[] D_BYTES = Bytes.toBytes("d");

  public static void putList(Table table, String datasetId, Integer attempt, Map<Long, String> fragmentsMap)
      throws IOException {

    List<Put> putList = fragmentsMap.entrySet().stream().map(x -> {
      Put put = new Put(Bytes.toBytes(x.getKey()));
      put.addColumn(CF_BYTES, D_BYTES, Bytes.toBytes(datasetId));
      put.addColumn(CF_BYTES, A_BYTES, Bytes.toBytes(attempt));
      put.addColumn(CF_BYTES, V_BYTES, Bytes.toBytes(x.getValue()));
      return put;
    }).collect(Collectors.toList());

    table.put(putList);
  }
}
