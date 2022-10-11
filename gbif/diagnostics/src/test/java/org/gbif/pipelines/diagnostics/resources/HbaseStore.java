package org.gbif.pipelines.diagnostics.resources;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.keygen.hbase.Columns;
import org.gbif.pipelines.keygen.hbase.HBaseStore;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HbaseStore {

  @SneakyThrows
  public static void putRecords(HBaseStore<String> lookupTableStore, Set<KV> kvSet) {
    kvSet.forEach(kv -> lookupTableStore.putLong(kv.key, Columns.LOOKUP_KEY_COLUMN, kv.value));
  }

  public static void putRecords(HBaseStore<String> lookupTableStore, KV... kv) {
    putRecords(lookupTableStore, new HashSet<>(Arrays.asList(kv)));
  }

  @AllArgsConstructor(staticName = "create")
  public static class KV {
    private final String key;
    private final long value;
  }
}
