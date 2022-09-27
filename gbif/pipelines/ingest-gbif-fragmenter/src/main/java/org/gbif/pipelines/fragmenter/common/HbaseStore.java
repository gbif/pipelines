package org.gbif.pipelines.fragmenter.common;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;
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
      Map<String, RawRecord> fragmentsMap) {

    Map<String, Long> dateMap = getCreatedDateMap(table, fragmentsMap);

    List<Put> putList =
        fragmentsMap.entrySet().stream()
            .map(
                es ->
                    createFragmentPut(
                        datasetKey,
                        attempt,
                        endpointType.name(),
                        es.getKey(),
                        es.getValue(),
                        dateMap.get(es.getKey())))
            .collect(Collectors.toList());

    table.put(putList);
  }

  @SneakyThrows
  public static Map<String, RawRecord> filterRecordsByHash(
      Table table, Map<String, RawRecord> fragmentsMap) {

    BiPredicate<String, RawRecord> prFn =
        (key, raw) -> {
          try {
            Get get = createHashValueGet(key);
            byte[] value = table.get(get).value();
            if (value != null) {
              return !raw.getHashValue().equals(new String(value, UTF_8));
            }
            return true;
          } catch (IOException ex) {
            throw new PipelinesException(ex);
          }
        };

    return fragmentsMap
        .entrySet()
        .parallelStream()
        .filter(es -> prFn.test(es.getKey(), es.getValue()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x, y) -> y));
  }

  private static Map<String, Long> getCreatedDateMap(
      Table table, Map<String, RawRecord> fragmentsMap) throws IOException {

    Map<String, Long> createdDateMap = new HashMap<>();

    for (String key : fragmentsMap.keySet()) {
      Get get = createCreatedDateGet(key);
      byte[] value = table.get(get).value();
      if (value != null) {
        createdDateMap.put(key, Bytes.toLong(value));
      }
    }

    return createdDateMap;
  }

  private static Put createFragmentPut(
      String datasetKey,
      Integer attempt,
      String protocol,
      String key,
      RawRecord rawRecord,
      Long created) {
    long timestampUpdated = Instant.now().toEpochMilli();
    long timestampCreated = Optional.ofNullable(created).orElse(timestampUpdated);

    Put put = new Put(Bytes.toBytes(key));

    put.addColumn(FF_BYTES, DQ_BYTES, Bytes.toBytes(datasetKey));
    put.addColumn(FF_BYTES, AQ_BYTES, Bytes.toBytes(attempt));
    put.addColumn(FF_BYTES, PQ_BYTES, Bytes.toBytes(protocol));

    put.addColumn(FF_BYTES, DCQ_BYTES, Bytes.toBytes(timestampCreated));
    put.addColumn(FF_BYTES, DUQ_BYTES, Bytes.toBytes(timestampUpdated));
    put.addColumn(FF_BYTES, RQ_BYTES, Bytes.toBytes(rawRecord.getRecord()));
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
