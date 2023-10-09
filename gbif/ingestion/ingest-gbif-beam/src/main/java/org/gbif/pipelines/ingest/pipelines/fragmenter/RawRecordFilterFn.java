package org.gbif.pipelines.ingest.pipelines.fragmenter;

import lombok.Builder;
import lombok.SneakyThrows;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hadoop.hbase.client.Table;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.fragmenter.common.HbaseStore;
import org.gbif.pipelines.fragmenter.common.RawRecord;

public class RawRecordFilterFn extends DoFn<RawRecord, RawRecord> {

  private final SerializableSupplier<Table> tableSupplier;
  private Table table;

  @Builder(buildMethodName = "create")
  private RawRecordFilterFn(SerializableSupplier<Table> tableSupplier) {
    this.tableSupplier = tableSupplier;
  }

  @Setup
  public void setup() {
    if (table == null && tableSupplier != null) {
      table = tableSupplier.get();
    }
  }

  /** Beam @Teardown closes initialized resources */
  @SneakyThrows
  @Teardown
  public void tearDown() {
    if (table != null) {
      table.close();
    }
  }

  @ProcessElement
  public void processElement(@Element RawRecord rr, OutputReceiver<RawRecord> out) {
    boolean isNewRawRecord = HbaseStore.isNewRawRecord(table, rr);
    if (isNewRawRecord) {
      HbaseStore.populateCreatedDate(table, rr);
      out.output(rr);
    }
  }
}
