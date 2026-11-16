package org.gbif.pipelines.transform;

import java.io.Serializable;
import java.time.Instant;
import java.util.Optional;
import java.util.function.BiConsumer;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.specific.GbifIdInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.keygen.HBaseLockingKey;

@Slf4j
@Builder
public class GbifIdTransform implements Serializable {

  private final String absentName;
  private final boolean isTripletValid;
  private final boolean isOccurrenceIdValid;
  private final boolean useExtendedRecordId;
  private final boolean generateIdIfAbsent;
  private final BiConsumer<ExtendedRecord, IdentifierRecord> gbifIdFn;
  private final SerializableSupplier<HBaseLockingKey> keygenServiceSupplier;

  private HBaseLockingKey keygenService;

  public Optional<IdentifierRecord> convert(ExtendedRecord source) {

    // initialise a new IdentifierRecord with id from ExtendedRecord and current timestamp
    IdentifierRecord ir =
        IdentifierRecord.newBuilder()
            .setId(source.getId())
            .setFirstLoaded(Instant.now().toEpochMilli())
            .build();

    if (keygenService == null && keygenServiceSupplier != null) {
      keygenService = keygenServiceSupplier.get();
    }

    return Interpretation.from(source)
        .to(ir)
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(
            GbifIdInterpreter.interpretGbifId(
                keygenService,
                isTripletValid,
                isOccurrenceIdValid,
                useExtendedRecordId,
                generateIdIfAbsent,
                gbifIdFn))
        .getOfNullable();
  }
}
