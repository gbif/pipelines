package org.gbif.pipelines.fragmenter.common;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.pipelines.common.PipelinesException;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HbaseStore {

  private static final byte[] FF_BYTES = Bytes.toBytes("fragment");

  private static final byte[] DQ_BYTES = Bytes.toBytes("datasetKey");
  private static final byte[] AQ_BYTES = Bytes.toBytes("attempt");
  private static final byte[] PQ_BYTES = Bytes.toBytes("protocol");

  private static final byte[] RQ_BYTES = Bytes.toBytes("record");
  private static final byte[] DCQ_BYTES = Bytes.toBytes("dateCreated");
  private static final byte[] DUQ_BYTES = Bytes.toBytes("dateUpdated");
  private static final byte[] HVQ_BYTES = Bytes.toBytes("hashValue");

  @SneakyThrows
  public static void putRecords(
      Table table,
      String datasetKey,
      Integer attempt,
      EndpointType endpointType,
      List<RawRecord> fragmentsList) {

    List<Put> putList =
        fragmentsList.stream()
            .map(rr -> populateCreatedDate(table, rr))
            .map(
                rawRecord -> createFragmentPut(datasetKey, attempt, endpointType.name(), rawRecord))
            .collect(Collectors.toList());

    table.put(putList);
  }

  @SneakyThrows
  public static boolean isNewRawRecord(Table table, RawRecord raw) {
    try {
      Get get = createHashValueGet(raw.getKey());
      byte[] value = table.get(get).value();
      if (value != null) {
        return !raw.getHashValue().equals(new String(value, UTF_8));
      }
      return true;
    } catch (IOException ex) {
      throw new PipelinesException(ex);
    }
  }

  @SneakyThrows
  public static List<RawRecord> filterRecordsByHash(Table table, List<RawRecord> fragmentsMap) {
    return fragmentsMap
        .parallelStream()
        .filter(rr -> isNewRawRecord(table, rr))
        .collect(Collectors.toList());
  }

  @SneakyThrows
  public static RawRecord populateCreatedDate(Table table, RawRecord rawRecord) {

    Get get = createCreatedDateGet(rawRecord.getKey());

    byte[] value = table.get(get).value();
    if (value != null) {
      long createdDate = Bytes.toLong(value);
      rawRecord.setCreatedDate(createdDate);
    }

    return rawRecord;
  }

  public static Put createFragmentPut(
      String datasetKey, Integer attempt, String protocol, RawRecord rawRecord) {
    long timestampUpdated = Instant.now().toEpochMilli();
    Long timestampCreated =
        Optional.ofNullable(rawRecord.getCreatedDate()).orElse(timestampUpdated);

    Put put = new Put(Bytes.toBytes(rawRecord.getKey()));

    put.addColumn(FF_BYTES, DQ_BYTES, Bytes.toBytes(datasetKey));
    put.addColumn(FF_BYTES, AQ_BYTES, Bytes.toBytes(attempt));
    put.addColumn(FF_BYTES, PQ_BYTES, Bytes.toBytes(protocol));

    put.addColumn(FF_BYTES, DUQ_BYTES, Bytes.toBytes(timestampUpdated));
    put.addColumn(FF_BYTES, DCQ_BYTES, Bytes.toBytes(timestampCreated));
    put.addColumn(FF_BYTES, RQ_BYTES, Bytes.toBytes(rawRecord.getRecordBody()));
    put.addColumn(FF_BYTES, HVQ_BYTES, Bytes.toBytes(rawRecord.getHashValue()));
    return put;
  }

  private static Get createCreatedDateGet(String key) {
    Get get = new Get(Bytes.toBytes(key));
    get.addColumn(FF_BYTES, DCQ_BYTES);
    return get;
  }

  private static Get createHashValueGet(String key) {
    Get get = new Get(Bytes.toBytes(key));
    get.addColumn(FF_BYTES, HVQ_BYTES);
    return get;
  }

  public static byte[] getFragmentFamily() {
    return FF_BYTES;
  }

  public static byte[] getDatasetKeyQualifier() {
    return DQ_BYTES;
  }

  public static byte[] getAttemptQualifier() {
    return AQ_BYTES;
  }

  public static byte[] getRecordQualifier() {
    return RQ_BYTES;
  }

  public static byte[] getProtocolQualifier() {
    return PQ_BYTES;
  }

  public static byte[] getDateCreatedQualifier() {
    return DCQ_BYTES;
  }

  public static byte[] getDateUpdatedQualifier() {
    return DUQ_BYTES;
  }

  public static byte[] getHashValueQualifier() {
    return HVQ_BYTES;
  }
}
