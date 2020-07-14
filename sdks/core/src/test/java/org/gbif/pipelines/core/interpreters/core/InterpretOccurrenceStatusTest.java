package org.gbif.pipelines.core.interpreters.core;

import java.util.HashMap;
import java.util.Map;

import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.junit.Test;

import lombok.NoArgsConstructor;

import static org.junit.Assert.assertEquals;

public class InterpretOccurrenceStatusTest {

  private static final String ID = "777";

  private static final KeyValueStore<String, OccurrenceStatus> OCCURRENCE_STATUS_VOCABULARY_STUB =
      OccurrenceStatusKvStoreStub.create();

  @Test
  public void interpretOccurrenceStatusTest() {
    // State
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    // BasicInterpreter.interpretOccurrenceStatus(OCCURRENCE_STATUS_VOCABULARY_STUB).accept(er, br);

    // Should
    // assertEquals(OccurrenceStatus.ABSENT.name(), br.getOccurrenceStatus());
  }

  @NoArgsConstructor(staticName = "create")
  private static class OccurrenceStatusKvStoreStub
      implements KeyValueStore<String, OccurrenceStatus> {

    private static final Map<String, OccurrenceStatus> MAP = new HashMap<>();

    static {
      MAP.put("ABSENT", OccurrenceStatus.ABSENT);
      MAP.put("PRESENT", OccurrenceStatus.PRESENT);
    }

    @Override
    public OccurrenceStatus get(String s) {
      return MAP.get(s);
    }

    @Override
    public void close() {}
  }
}
