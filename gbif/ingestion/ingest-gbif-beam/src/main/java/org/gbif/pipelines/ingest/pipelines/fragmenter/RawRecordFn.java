package org.gbif.pipelines.ingest.pipelines.fragmenter;

import java.util.function.Predicate;
import lombok.Builder;
import lombok.SneakyThrows;
import org.apache.beam.sdk.transforms.DoFn;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.fragmenter.common.RawRecord;
import org.gbif.pipelines.fragmenter.record.DwcaOccurrenceRecord;
import org.gbif.pipelines.fragmenter.record.OccurrenceRecordConverter;
import org.gbif.pipelines.keygen.HBaseLockingKey;

public class RawRecordFn extends DoFn<DwcaOccurrenceRecord, RawRecord> {
  private final boolean useTriplet;
  private final boolean useOccurrenceId;
  private final boolean generateIdIfAbsent;
  private final SerializableSupplier<HBaseLockingKey> keygenServiceSupplier;
  private HBaseLockingKey keygenService;

  @Builder(buildMethodName = "create")
  private RawRecordFn(
      boolean useTriplet,
      boolean useOccurrenceId,
      boolean generateIdIfAbsent,
      SerializableSupplier<HBaseLockingKey> keygenServiceSupplier) {
    this.useTriplet = useTriplet;
    this.useOccurrenceId = useOccurrenceId;
    this.generateIdIfAbsent = generateIdIfAbsent; // for IT tests
    this.keygenServiceSupplier = keygenServiceSupplier;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (keygenService == null && keygenServiceSupplier != null) {
      keygenService = keygenServiceSupplier.get();
    }
  }

  /** Beam @Teardown closes initialized resources */
  @SneakyThrows
  @Teardown
  public void tearDown() {
    if (keygenService != null) {
      keygenService.close();
    }
  }

  @ProcessElement
  public void processElement(@Element DwcaOccurrenceRecord dor, OutputReceiver<RawRecord> out) {
    Predicate<String> emptyValidator = s -> true;
    OccurrenceRecordConverter.convert(
            keygenService, emptyValidator, useTriplet, useOccurrenceId, generateIdIfAbsent, dor)
        .ifPresent(out::output);
  }
}
