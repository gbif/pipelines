package org.gbif.pipelines.fragmenter.common;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HbaseStore {

  private static final byte[] FF_BYTES = Bytes.toBytes("o");
  private static final byte[] RQ_BYTES = Bytes.toBytes("r");
  private static final byte[] AQ_BYTES = Bytes.toBytes("a");
  private static final byte[] DQ_BYTES = Bytes.toBytes("d");

  @SneakyThrows
  public static void putList(Table table, String datasetId, Integer attempt, Map<Long, String> fragmentsMap) {

    List<Put> putList = fragmentsMap.entrySet().stream().map(x -> {
      Put put = new Put(Bytes.toBytes(x.getKey()));
      put.addColumn(FF_BYTES, DQ_BYTES, Bytes.toBytes(datasetId));
      put.addColumn(FF_BYTES, AQ_BYTES, Bytes.toBytes(attempt));
      put.addColumn(FF_BYTES, RQ_BYTES, Bytes.toBytes(x.getValue()));
      return put;
    }).collect(Collectors.toList());

    table.put(putList);
  }

  public static byte[] getFragmentFamily() {
    return FF_BYTES;
  }

  public static byte[] getRecordQualifier() {
    return RQ_BYTES;
  }

  public static byte[] getAttemptQualifier() {
    return AQ_BYTES;
  }

  public static byte[] getDatasetIdQualifier() {
    return DQ_BYTES;
  }
}
