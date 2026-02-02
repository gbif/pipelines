package org.gbif.pipelines.transform;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.specific.GbifIdInterpreter;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.keygen.HBaseLockingKey;

@Slf4j
@Builder
public class GbifAbsentIdTransform implements Serializable, Closeable {

  private final String absentName;
  private final boolean isTripletValid;
  private final boolean isOccurrenceIdValid;
  private final boolean useExtendedRecordId;
  private final boolean generateIdIfAbsent;
  private final SerializableSupplier<HBaseLockingKey> keygenServiceSupplier;

  private HBaseLockingKey keygenService;

  public IdentifierRecord persist(IdentifierRecord ir) {

    if (keygenService == null && keygenServiceSupplier != null) {
      keygenService = keygenServiceSupplier.get();
    }

    GbifIdInterpreter.interpretAbsentGbifId(keygenService, isTripletValid, isOccurrenceIdValid)
        .accept(ir);

    return ir;
  }

  @Override
  public void close() throws IOException {
    if (keygenService != null) {
      keygenService.close();
    }
  }
}
